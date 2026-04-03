import threading
from abc import ABC, abstractmethod
from typing import Any


class CancellationToken:
    """
    Cooperative cancellation token passed to operations that opt in.

    When an operation is invoked via ``POST /operations/<name>``, the framework
    creates a ``CancellationToken`` for the run and passes it to ``call()`` if
    the method signature declares a second parameter.

    Poll :attr:`is_cancelled` or call :meth:`raise_if_cancelled` at convenient
    checkpoints to exit early when cancellation is requested::

        class SlowOp(Operation):
            def call(self, tool_input: Any, cancellation_token=None) -> Any:
                for chunk in big_dataset:
                    if cancellation_token:
                        cancellation_token.raise_if_cancelled()
                    process(chunk)
                return {"done": True}
    """

    def __init__(self) -> None:
        self._event = threading.Event()

    def cancel(self) -> None:
        """Signal that the operation should stop."""
        self._event.set()

    @property
    def is_cancelled(self) -> bool:
        """True once :meth:`cancel` has been called."""
        return self._event.is_set()

    def raise_if_cancelled(self) -> None:
        """Raise :exc:`OperationCancelledError` if cancellation has been requested."""
        if self.is_cancelled:
            raise OperationCancelledError("Operation was cancelled")


class OperationCancelledError(Exception):
    """Raised by an operation that detects its cancellation token has been set."""


class Operation(ABC):
    """
    Base class for operations exposed as MCP tools.

    Subclasses must set TOOL_SPEC to a dict matching the MCP tool definition
    format (name, description, inputSchema), and implement ``call``.

    To support cooperative cancellation when invoked via
    ``POST /operations/<name>``, declare a second parameter on ``call``::

        def call(self, tool_input: Any, cancellation_token=None) -> Any: ...

    The framework detects this at invocation time and passes a
    :class:`CancellationToken`.  Operations that omit the second parameter
    continue to work unchanged — the token is simply not forwarded.

    Example::

        class MyOp(Operation):
            TOOL_SPEC = {
                "name": "my_op",
                "description": "Does something useful",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "value": {"type": "string"}
                    },
                    "required": ["value"],
                },
            }

            def call(self, tool_input: Any) -> Any:
                return {"result": tool_input["value"].upper()}
    """

    TOOL_SPEC: dict = {
        "name": "operation",
        "description": "An abstract operation",
        "inputSchema": {
            "type": "object",
            "properties": {},
        },
    }

    @abstractmethod
    def call(self, tool_input: Any) -> Any:
        """
        Execute the operation.

        Args:
            tool_input: The ``arguments`` dict from the MCP ``tools/call`` request
                        or the JSON body from ``POST /operations/<name>``.

        Returns:
            Any JSON-serialisable value.
        """
        ...
