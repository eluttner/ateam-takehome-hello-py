hello-py
===

Setup instructions:

1. Clone the repository:
   ```
   git clone https://github.com/preferencemodel/hello-py.git
   ```

2. Navigate to the project directory:
   ```
   cd hello-py
   ```

3. Set up `ANTHROPIC_API_KEY` environment variable:
   ```
   export ANTHROPIC_API_KEY=your_api_key_here
   ```

4. Run the agent:
   ```
   uv run main.py
   ```

## Execution Modes

The test suite supports both concurrent and sequential execution. 

To change modes, edit the `concurrent` parameter at the bottom of `main.py`:

```python
asyncio.run(main(concurrent=True))
asyncio.run(main(concurrent=False))
```

When running concurrently, results print as they complete (not in run order) for faster overall execution.

===

# My comments

## Virtual Env

```bash
    uv venv
    source .venv/bin/activate
    uv sync
```

## Main work steps
1 - create a task_interface
2 - move the current implementation to task interface
3 - add sandbox dir
4 - add run dir
5 - replace expected result with a grader function
6 - create global vars in grader
7 - modify main for logging steps and change the verbose
8 - work on the broken code
9 - work on the grader
10 - enhance messages / outputs
11 - to optimize the prompt https://platform.openai.com/chat/edit?models=gpt-5&optimize=true
12 - small dataflow fixes and optimize logging

## Decisions
- Verbose logging was too verbose and missing the information flow
- created a sandbox directory, to have the input/output database, code and messages
- created a task directory, can be expanded to more tasks, for example, selecting a task from command line
- Run in sequence, running in parallel will overflow the Anthropic API rate limiter (50k/input tokens/minute)

# Enhancements
- use a log library to have better control
- create a config file
- expand to more tasks (it is hardcoded for this one task), but easy to expand
- perhaps log intermediate results
 
## Injected bugs
Bug 0: No PRIMARY KEY to prevent duplicates
Bug 1: date watermark
Bug 2: Off-by-one (> instead of >=)
Bug 3: String comparison with DD/MM/YYYY format will fail at month boundary
Bug 4: No ORDER BY
Bug 5: Appending without checking for existing (Idempotency)

## Process flow
- start the task manager
- start the task
- send the prompt and get the modified code
- grader creates a sandbox environment to test the code
    - grader runs as a real case scenario
        - run day by day
        - inject a past record (ETL nightmare)
        - run a job twice in the same day
    - compare the results (demonstrated that it support many conditions)
        - test for number of rows
        - test for duplicate records

# Observation
- the repo has a sandbox commited for demonstration
- costed $2.22 to run 20 times
```
============================================================
Test Results:
  Passed: 4/20
  Failed: 16/20
  Pass Rate: 20.0%
============================================================
Total time: 1496.35 seconds
============================================================
```