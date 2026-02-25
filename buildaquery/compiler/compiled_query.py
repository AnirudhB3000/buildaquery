from dataclasses import dataclass, field
from typing import Any

# ==================================================
# Compiled Output
# ==================================================

@dataclass
class CompiledQuery:
    """
    Represents the result of the compilation process.
    """
    sql: str
    params: list[Any] = field(default_factory=list)
