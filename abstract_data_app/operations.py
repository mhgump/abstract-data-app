from abc import ABC, abstractmethod
from typing import Any


class Operation(ABC):
    """
    Base class for operations exposed as MCP tools.

    Subclasses must set TOOL_SPEC to a dict matching the MCP tool definition
    format (name, description, inputSchema), and implement ``call``.

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
            tool_input: The ``arguments`` dict from the MCP ``tools/call`` request.

        Returns:
            Any JSON-serialisable value.
        """
        ...
