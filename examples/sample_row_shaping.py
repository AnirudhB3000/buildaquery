from dataclasses import dataclass

from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.execution.sqlite import SqliteExecutor


@dataclass
class UserRow:
    id: int
    email: str


def main() -> None:
    query = CompiledQuery(sql="SELECT id, email FROM users WHERE active = ?", params=[True])

    tuple_executor = SqliteExecutor(connection_info=":memory:")
    dict_executor = SqliteExecutor(connection_info=":memory:", row_output="dict")
    model_executor = SqliteExecutor(
        connection_info=":memory:",
        row_output="model",
        row_model=UserRow,
    )

    print("Tuple mode:", tuple_executor.to_sql(query))
    print("Dict mode:", dict_executor.to_sql(query))
    print("Model mode:", model_executor.to_sql(query))
    print("Use fetch_all/fetch_one/execute on a row-returning query with the configured executor.")


if __name__ == "__main__":
    main()
