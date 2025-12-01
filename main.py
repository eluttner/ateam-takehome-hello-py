import asyncio
import json
from collections.abc import Callable
from contextlib import redirect_stdout
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Any, TypedDict
import time


from anthropic import AsyncAnthropic
from anthropic.types import MessageParam, ToolUnionParam

from tasks.task_etl.task import EtlTask

MAX_TOKENS = 4000
NUM_RUNS = 10
MAX_STEPS = 15
VERBOSE = False

class PythonExpressionToolResult(TypedDict):
    result: Any
    error: str | None


class SubmitAnswerToolResult(TypedDict):
    answer: Any
    submitted: bool


def python_expression_tool(expression: str) -> PythonExpressionToolResult:
    """
    Tool that evaluates Python expressions using exec.
    Use print(...) to emit output; stdout will be captured and returned.
    """
    try:
        namespace = {}
        stdout = StringIO()
        with redirect_stdout(stdout):
            exec(expression, namespace, namespace)
        return {"result": stdout.getvalue(), "error": None}
    except KeyboardInterrupt:
        raise
    except Exception as e:
        return {"result": None, "error": str(e)}


def submit_answer_tool(answer: Any) -> SubmitAnswerToolResult:
    """
    Tool for submitting the final answer.
    """
    return {"answer": answer, "submitted": True}


async def run_agent_loop(
    prompt: str,
    tools: list[ToolUnionParam],
    tool_handlers: dict[str, Callable[..., Any]],
    max_steps: int = MAX_STEPS,
    model: str = "claude-haiku-4-5",
    verbose: bool = True,
) -> Any | None:
    """
    Runs an agent loop with the given prompt and tools.

    Args:
        prompt: The initial prompt for the agent
        tools: List of tool definitions for Anthropic API
        tool_handlers: Dictionary mapping tool names to their handler functions
        max_steps: Maximum number of steps before stopping (default 5)
        model: The Anthropic model to use
        verbose: Whether to print detailed output (default True)

    Returns:
        The submitted answer if submit_answer was called, otherwise None
    """
    client = AsyncAnthropic()
    messages: list[MessageParam] = [{"role": "user", "content": prompt}]
    total_tokens_in = 0
    total_tokens_out = 0
    for step in range(max_steps):
        print(f"- Step {step + 1}/{max_steps}")

        response = await client.messages.create(
            model=model, max_tokens=MAX_TOKENS, tools=tools, messages=messages
        )
        
        # Log token usage
        if response.usage:
            print(f"\tToken usage: Input: {response.usage.input_tokens}, Output: {response.usage.output_tokens}")
            total_tokens_in = total_tokens_in + response.usage.input_tokens
            total_tokens_out = total_tokens_out + response.usage.output_tokens

        assert response.stop_reason in ["max_tokens", "tool_use", "end_turn"], (
            f"unsupported stop_reason {response.stop_reason}"
        )
        if response.stop_reason == "max_tokens":
            print(
                f"Model reached max_tokens limit {MAX_TOKENS}. Increase "
                "MAX_TOKENS, simplify your task, or update the code to provide "
                "a message back to the model when it exceeds MAX_TOKENS."
            )

        # Track if we need to continue
        has_tool_use = False
        tool_results = []
        submitted_answer = None

        # Process the response
        for content in response.content:
            if content.type == "text":
                if verbose:
                    print(f"Assistant: {content.text}")
            elif content.type == "tool_use":
                has_tool_use = True
                tool_name = content.name

                if tool_name in tool_handlers:
                    if verbose:
                        print(f"Using tool: {tool_name}")

                    # Extract arguments based on tool
                    handler = tool_handlers[tool_name]
                    tool_input = content.input

                    # Call the appropriate tool handler
                    if tool_name == "python_expression":
                        assert (
                            isinstance(tool_input, dict) and "expression" in tool_input
                        )
                        if verbose:
                            print("\nInput:")
                            print("```")
                            for line in tool_input["expression"].split("\n"):
                                print(f"{line}")
                            print("```")
                        result = handler(tool_input["expression"])
                        if verbose:
                            print("\nOutput:")
                            print("```")
                            print(result)
                            print("```")
                    elif tool_name == "submit_answer":
                        assert isinstance(tool_input, dict) and "answer" in tool_input
                        result = handler(tool_input["answer"])
                        submitted_answer = result["answer"]
                    else:
                        # Generic handler call
                        result = (
                            handler(**tool_input)
                            if isinstance(tool_input, dict)
                            else handler(tool_input)
                        )

                    tool_results.append(
                        {
                            "type": "tool_result",
                            "tool_use_id": content.id,
                            "content": json.dumps(result),
                        }
                    )

        # If we have tool uses, add them to the conversation
        if has_tool_use:
            messages.append({"role": "assistant", "content": response.content})

            messages.append({"role": "user", "content": tool_results})
            print(f"\tCummulative tokens in: {total_tokens_in}")
            print(f"\tCummulative tokens out: {total_tokens_out}")

            # If an answer was submitted, return it
            if submitted_answer is not None:
                if verbose:
                    print(f"\nAgent submitted answer: {submitted_answer}")
                return submitted_answer
        else:
            # No tool use, conversation might be complete
            if verbose:
                print("\nNo tool use in response, ending loop.")
            break


    print(f"\n- Reached maximum steps ({max_steps}) without submitting answer.")
    return None


async def run_single_test(
    run_id: int,
    num_runs: int,
    prompt: str,
    tools: list[ToolUnionParam],
    tool_handlers: dict[str, Callable[..., Any]],
    expected_answer: Any,
    sandbox_dir: Path,
    verbose: bool = False,
) -> tuple[int, bool, Any]:

    print(f"\n\n{'=' * 20} RUN {run_id}/{num_runs} {'=' * 20}")

    result = await run_agent_loop(
        prompt=prompt,
        tools=tools,
        tool_handlers=tool_handlers,
        max_steps=MAX_STEPS,
        verbose=verbose,
    )

    current_run = f"run_{run_id:03d}"
    success = expected_answer(current_run, result, verbose)
    run_dir = sandbox_dir / current_run
    rename_dir = run_dir.parent / f"run_{run_id:03d}_success"
    if success[0]:
        print(f"\n✅ Run {run_id}: SUCCESS\n{success[1]}")
    else:
        print(f"\n⛔️ Run {run_id}: FAILURE\n{success[1]}")
        rename_dir = run_dir.parent / f"run_{run_id:03d}_failure"

    run_dir.rename(rename_dir)
    with open(rename_dir / "results.txt", "w") as f:
        f.write(success[1])

    return run_id, success, result


async def main(concurrent: bool = True):
    time_start = time.time()
    print(f"\n\n{'=' * 20} START {'=' * 20}")
    print(f"MAX_TOKENS: {MAX_TOKENS}")
    print(f"NUM_RUNS: {NUM_RUNS}")
    print(f"MAX_STEPS: {MAX_STEPS}")
    print(f"VERBOSE: {VERBOSE}")
    print(f"\n{'=' * 60}")

    tools: list[ToolUnionParam] = [
        {
            "name": "python_expression",
            "description": "Evaluates a Python expression",
            "input_schema": {
                "type": "object",
                "properties": {
                    "expression": {
                        "type": "string",
                        "description": "Will be passed to exec(). Use print() to output something. Returns stdout. ",
                    }
                },
                "required": ["expression"],
            },
        },
        {
            "name": "submit_answer",
            "description": "Submit the final answer",
            "input_schema": {
                "type": "object",
                "properties": {"answer": {"description": "The final answer to submit"}},
                "required": ["answer"],
            },
        },
    ]

    tool_handlers = {
        "python_expression": python_expression_tool,
        "submit_answer": submit_answer_tool,
    }

    # Run the test 10 times and track success rate
    num_runs = NUM_RUNS

    execution_mode = "concurrently" if concurrent else "sequentially"
    print(f"Running {num_runs} test iterations {execution_mode}...")
    print("=" * 60)

    # sandbox_dir
    sandbox_dir = Path("./sandbox/" + datetime.now().strftime("%Y%m%d-%H%M%S_task_etl"))
    sandbox_dir.mkdir(parents=True, exist_ok=True)

    task = EtlTask(sandbox_dir)
    task.setup()

    # Create all test coroutines
    tasks = [
        run_single_test(
            run_id=i + 1,
            num_runs=num_runs,
            prompt=task.prompt(),
            tools=tools,
            tool_handlers=tool_handlers,
            expected_answer=task.grader,
            sandbox_dir=sandbox_dir,
            verbose=VERBOSE,
        )
        for i in range(num_runs)
    ]

    # Run concurrently or sequentially based on the flag
    if concurrent:
        # Process results as they complete
        results = []
        for coro in asyncio.as_completed(tasks):
            result = await coro
            results.append(result)
    else:
        # Run sequentially by awaiting each task in order
        results = []
        for task in tasks:
            result = await task
            results.append(result)

    # Count successes
    successes = sum((s[0] if isinstance(s, tuple) else s) for _, s, _ in results)

    # Calculate and display pass rate
    pass_rate = (successes / num_runs) * 100
    print(f"\n{'=' * 60}")
    print("Test Results:")
    print(f"  Passed: {successes}/{num_runs}")
    print(f"  Failed: {num_runs - successes}/{num_runs}")
    print(f"  Pass Rate: {pass_rate:.1f}%")
    print(f"{'=' * 60}")
    print(f"Total time: {time.time() - time_start:.2f} seconds")
    print(f"{'=' * 60}")

if __name__ == "__main__":
    # Set to True for concurrent execution, False for sequential execution
    """
    ⚠️ Run sequentially to avoid the anthropic api rate limit for 50k input/tokens/minute
    anthropic.RateLimitError: Error code: 429 - {'type': 'error', 'error': {'type': 'rate_limit_error', 'message': 'This request would exceed the rate limit for your organization (612cf0e3-dad7-416e-9de4-82567fcfe1a7) of 50,000 input tokens per minute. For details, refer to: https://docs.claude.com/en/api/rate-limits. You can see the response headers for current usage. Please reduce the prompt length or the maximum tokens requested, or try again later. You may also contact sales at https://www.anthropic.com/contact-sales to discuss your options for a rate limit increase.'}, 'request_id': 'req_011CVfP63Tr9DnBwbY9ZefRn'}
    """
    asyncio.run(main(concurrent=False))
