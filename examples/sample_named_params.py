from buildaquery.compiler.compiled_query import CompiledQuery
from buildaquery.execution.oracle import OracleExecutor
from buildaquery.execution.postgres import PostgresExecutor
from buildaquery.execution.sqlite import SqliteExecutor


# Named params are a convenience for externally supplied SQL / CompiledQuery inputs.
# Executors rewrite :name placeholders into the target dialect's native placeholder style.

sqlite_executor = SqliteExecutor(connection_info="static/test-sqlite/db.sqlite")
sqlite_query = CompiledQuery(
    sql="SELECT id, email FROM users WHERE email = :email AND tenant_id = :tenant_id",
    params={"email": "alice@example.com", "tenant_id": 42},
)
print(sqlite_executor.to_sql(sqlite_query).to_sql())
print(sqlite_executor.to_sql(sqlite_query).params)

postgres_executor = PostgresExecutor(connection_info="postgresql://user:password@localhost:5432/app")
postgres_query = CompiledQuery(
    sql="SELECT id FROM users WHERE email = :email OR backup_email = :email",
    params={"email": "alice@example.com"},
)
print(postgres_executor.to_sql(postgres_query).to_sql())
print(postgres_executor.to_sql(postgres_query).params)

oracle_executor = OracleExecutor(connection_info="oracle://user:password@localhost:1521/XEPDB1")
oracle_query = CompiledQuery(
    sql="SELECT :name AS display_name, :account_id AS account_id FROM dual",
    params={"name": "Alice", "account_id": 5},
)
print(oracle_executor.to_sql(oracle_query).to_sql())
print(oracle_executor.to_sql(oracle_query).params)
