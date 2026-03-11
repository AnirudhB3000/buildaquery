from buildaquery.execution.postgres import PostgresExecutor


# Syntax-only example: branch on explicit executor capabilities.
executor = PostgresExecutor(connection=object())
capabilities = executor.capabilities()

if capabilities.select_for_update and capabilities.lock_skip_locked:
    print("This dialect can use FOR UPDATE SKIP LOCKED patterns.")

if capabilities.insert_returning:
    print("This dialect can fetch inserted rows directly.")

print(capabilities.to_dict())
