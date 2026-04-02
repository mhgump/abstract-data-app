from dataclasses import dataclass


@dataclass
class Config:
    """Configuration for the abstract data app server."""

    host: str = "0.0.0.0"
    """Host to bind the HTTP server to."""

    port: int = 8000
    """Port to bind the HTTP server to."""

    num_threads: int = 8
    """Size of the request-handling thread pool."""

    debug: bool = False
    """Enable Flask debug mode (do not use in production)."""

    print_errors: bool = True
    """Print full tracebacks for unhandled exceptions to stderr."""

    mcp_path: str = "/mcp"
    """URL path for the MCP JSON-RPC endpoint."""
