import re


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_COLUMN_EXPRESSION_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\(\*\)$")


def validate_identifier(
    identifier: str,
    *,
    kind: str = "identifier",
    allow_column_expression: bool = False,
) -> str:
    if identifier == "*":
        return identifier
    if allow_column_expression and _COLUMN_EXPRESSION_RE.fullmatch(identifier):
        return identifier
    if not _IDENTIFIER_RE.fullmatch(identifier):
        raise ValueError(f"Unsafe SQL identifier for {kind}: {identifier!r}")
    return identifier
