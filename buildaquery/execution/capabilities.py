from dataclasses import asdict, dataclass


# ==================================================
# Executor Capability Model
# ==================================================


@dataclass(frozen=True)
class ExecutorCapabilities:
    """
    Explicit dialect capability contract for safe application branching.
    """

    transactions: bool
    savepoints: bool
    upsert: bool
    insert_returning: bool
    update_returning: bool
    delete_returning: bool
    select_for_update: bool
    select_for_share: bool
    lock_nowait: bool
    lock_skip_locked: bool
    execute_many: bool = True
    execute_raw: bool = True

    def to_dict(self) -> dict[str, bool]:
        """
        Returns a plain dict view for logging or app-level branching.
        """
        return asdict(self)
