from airflow.hooks.base import BaseHook

def is_spark_available_callable():
    try:
        conn = BaseHook.get_connection("spark_default")
        print("DEBUG conn spark:", conn)
        # We check that "master" exists in the extras.
        if "master" not in conn.extra_dejson:
            print("❌ The Spark connection does not have a 'master' field")
            return False

        print("✅ Spark connection found")
        return True
    except Exception as e:
        print(f"❌ Error during get_connection spark_default : {e}")
        return False