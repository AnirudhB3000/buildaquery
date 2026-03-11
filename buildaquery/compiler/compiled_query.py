from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

# ==================================================
# Compiled Output
# ==================================================

@dataclass
class CompiledQuery:
    """
    Represents the result of the compilation process.
    """
    sql: str
    params: Sequence[Any] | Mapping[str, Any] = field(default_factory=list)

    def to_sql(self) -> str:
        """
        Returns the placeholder-based SQL string for debug and inspection flows.
        """
        return self.sql
