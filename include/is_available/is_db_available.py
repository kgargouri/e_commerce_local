from airflow.providers.postgres.hooks.postgres import PostgresHook

def is_db_available_callable():
    """Checks if the Postgres connection is established without a request"""
    try:
        hook = PostgresHook(postgres_conn_id="postgres_default")  # Login ID
        conn = hook.get_conn()  # attempts to establish the connection
        conn.close()            # close immediately
        print("✅ Postgres connection established")
        return True
    except Exception as e:
        print(f"❌ Postgres connection failed : {e}")
        return False