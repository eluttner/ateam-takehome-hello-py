from typing import Protocol, Any
from pathlib import Path

class TaskInterface(Protocol):
    def __init__(self, sandbox_dir: Path) -> None:
        ...

    def setup(self) -> None:

        """
        Set up the task environment in the sandbox directory.
        """
        ...

    def prompt(self) -> str:
        """
        Return the prompt for the task, potentially using the sandbox directory.
        """
        ...

    def broken_code(self) -> str:
        """
        Return the broken code that needs to be fixed.
        """
        ...

    def grader(self, answer: Any) -> (bool, str):
        """
        Check if the answer is correct.
        """
        ...
