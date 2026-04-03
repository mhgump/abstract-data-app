"""
Validation utilities for dataclass-typed data.

``validate_dataclass_dict`` is the main entry point used by the framework.
It validates every field of a Python dataclass against the provided dict,
running per-field checks *in parallel* using a thread pool, then collecting
all errors before returning.
"""

import dataclasses
import typing
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Optional, get_type_hints


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def validate_dataclass_dict(data_type: type, data: dict[str, Any]) -> list[str]:
    """
    Validate *data* (a plain dict) against the dataclass *data_type*.

    Steps:
    1. Check for missing required fields (synchronously – order matters for UX).
    2. Validate each present field's value against its type annotation in
       parallel using a thread pool.

    Returns a (possibly empty) list of human-readable error strings.
    """
    if not dataclasses.is_dataclass(data_type):
        return [f"{getattr(data_type, '__name__', data_type)} is not a dataclass"]

    try:
        hints = get_type_hints(data_type)
    except Exception as exc:
        return [f"Could not resolve type hints for {data_type.__name__}: {exc}"]

    fields = {f.name: f for f in dataclasses.fields(data_type)}
    errors: list[str] = []

    # --- Pass 1: missing required fields (fast, synchronous) ---
    for fname, field in fields.items():
        has_default = (
            field.default is not dataclasses.MISSING
            or field.default_factory is not dataclasses.MISSING  # type: ignore[misc]
        )
        if not has_default and fname not in data:
            errors.append(f"Field '{fname}': missing required field")

    # --- Pass 2: type-check present fields in parallel ---
    to_check = [
        (fname, data[fname], hints.get(fname, Any))
        for fname in fields
        if fname in data
    ]

    if to_check:
        workers = min(len(to_check), 16)
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(_validate_field, fname, value, hint): fname
                for fname, value, hint in to_check
            }
            for future in as_completed(futures):
                error = future.result()
                if error is not None:
                    errors.append(error)

    return errors


def dataclass_to_json_schema(data_type: type) -> dict:
    """
    Generate a JSON Schema ``object`` for a Python dataclass.

    Handles str, int, float, bool, None, Optional[X], List[X], Dict[K, V],
    Union[X, Y, ...], and nested dataclasses recursively.
    """
    if not dataclasses.is_dataclass(data_type):
        return {"type": "object"}

    try:
        hints = get_type_hints(data_type)
    except Exception:
        hints = {}

    properties: dict = {}
    required: list[str] = []

    for field in dataclasses.fields(data_type):
        hint = hints.get(field.name, Any)
        properties[field.name] = _hint_to_schema(hint)
        has_default = (
            field.default is not dataclasses.MISSING
            or field.default_factory is not dataclasses.MISSING  # type: ignore[misc]
        )
        if not has_default:
            required.append(field.name)

    schema: dict = {"type": "object", "properties": properties}
    if required:
        schema["required"] = required
    return schema


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _validate_field(name: str, value: Any, hint: Any) -> Optional[str]:
    """Validate a single field value. Returns an error string or None."""
    try:
        _check_type(value, hint)
        return None
    except (TypeError, ValueError) as exc:
        return f"Field '{name}': {exc}"
    except Exception as exc:
        return f"Field '{name}': unexpected validation error: {exc}"


def _check_type(value: Any, hint: Any) -> None:
    """
    Recursively assert that *value* conforms to *hint*.
    Raises ``TypeError`` or ``ValueError`` on mismatch.
    """
    # Fast-path: no constraint
    if hint is Any or hint is object:
        return

    origin = getattr(hint, "__origin__", None)

    # --- Primitive scalars ---
    if hint is type(None):
        if value is not None:
            raise TypeError(f"expected None, got {type(value).__name__}")
        return

    if hint is bool:
        if not isinstance(value, bool):
            raise TypeError(f"expected bool, got {type(value).__name__}")
        return

    if hint is int:
        # bool is a subclass of int in Python; reject it here.
        if not isinstance(value, int) or isinstance(value, bool):
            raise TypeError(f"expected int, got {type(value).__name__}")
        return

    if hint is float:
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise TypeError(f"expected float, got {type(value).__name__}")
        return

    if hint is str:
        if not isinstance(value, str):
            raise TypeError(f"expected str, got {type(value).__name__}")
        return

    # --- Generic aliases ---
    if origin is typing.Union:
        _check_union(value, hint.__args__)
        return

    if origin is list:
        _check_list(value, hint)
        return

    if origin is dict:
        _check_dict(value, hint)
        return

    # Python 3.10+ ``X | Y`` produces ``types.UnionType``
    try:
        import types as _types
        if isinstance(hint, _types.UnionType):
            _check_union(value, hint.__args__)
            return
    except AttributeError:
        pass

    # --- Nested dataclass ---
    if dataclasses.is_dataclass(hint):
        if not isinstance(value, dict):
            raise TypeError(
                f"expected dict for nested dataclass {hint.__name__}, "
                f"got {type(value).__name__}"
            )
        nested_errors = validate_dataclass_dict(hint, value)
        if nested_errors:
            joined = "; ".join(nested_errors)
            raise TypeError(f"nested {hint.__name__} errors: {joined}")
        return

    # Unknown / un-handled hint – be lenient.


def _check_union(value: Any, args: tuple) -> None:
    none_type = type(None)
    if value is None:
        if none_type in args:
            return
        raise TypeError(f"unexpected None for Union{list(args)}")
    non_none = [a for a in args if a is not none_type]
    last_err: Optional[str] = None
    for t in non_none:
        try:
            _check_type(value, t)
            return  # matched
        except (TypeError, ValueError) as exc:
            last_err = str(exc)
    raise TypeError(
        f"value {value!r} does not match any type in Union: {last_err}"
    )


def _check_list(value: Any, hint: Any) -> None:
    if not isinstance(value, list):
        raise TypeError(f"expected list, got {type(value).__name__}")
    args = getattr(hint, "__args__", None)
    if args:
        item_hint = args[0]
        for i, item in enumerate(value):
            try:
                _check_type(item, item_hint)
            except (TypeError, ValueError) as exc:
                raise TypeError(f"item[{i}]: {exc}") from None


def _check_dict(value: Any, hint: Any) -> None:
    if not isinstance(value, dict):
        raise TypeError(f"expected dict, got {type(value).__name__}")
    args = getattr(hint, "__args__", None)
    if args and len(args) == 2:
        key_hint, val_hint = args
        for k, v in value.items():
            try:
                _check_type(k, key_hint)
            except (TypeError, ValueError) as exc:
                raise TypeError(f"key {k!r}: {exc}") from None
            try:
                _check_type(v, val_hint)
            except (TypeError, ValueError) as exc:
                raise TypeError(f"value for {k!r}: {exc}") from None


def _hint_to_schema(hint: Any) -> dict:
    """Convert a Python type hint into a JSON Schema fragment."""
    if hint is Any or hint is object:
        return {}
    if hint is str:
        return {"type": "string"}
    if hint is int:
        return {"type": "integer"}
    if hint is float:
        return {"type": "number"}
    if hint is bool:
        return {"type": "boolean"}
    if hint is type(None):
        return {"type": "null"}

    origin = getattr(hint, "__origin__", None)

    if origin is typing.Union:
        return _union_schema(hint.__args__)

    if origin is list:
        args = getattr(hint, "__args__", None)
        if args:
            return {"type": "array", "items": _hint_to_schema(args[0])}
        return {"type": "array"}

    if origin is dict:
        args = getattr(hint, "__args__", None)
        if args and len(args) == 2:
            return {
                "type": "object",
                "additionalProperties": _hint_to_schema(args[1]),
            }
        return {"type": "object"}

    try:
        import types as _types
        if isinstance(hint, _types.UnionType):
            return _union_schema(hint.__args__)
    except AttributeError:
        pass

    if dataclasses.is_dataclass(hint):
        return dataclass_to_json_schema(hint)

    return {}


def _union_schema(args: tuple) -> dict:
    none_type = type(None)
    nullable = none_type in args
    non_none = [a for a in args if a is not none_type]

    if len(non_none) == 1:
        schema = _hint_to_schema(non_none[0])
        if nullable:
            if "type" in schema and isinstance(schema["type"], str):
                return {**schema, "type": [schema["type"], "null"]}
        return schema

    schemas = [_hint_to_schema(t) for t in non_none]
    if nullable:
        schemas.append({"type": "null"})
    return {"oneOf": schemas}
