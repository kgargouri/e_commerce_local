from airflow.providers.postgres.hooks.postgres import PostgresHook
import csv
import glob

def load_to_postgres_callable(table_name, run_date):
    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cursor = conn.cursor()

    path_dir=f"/tmp/{table_name}/transformed/{table_name}_transform_{run_date}"
    file_path=f"{path_dir}/part-*.csv"

    files = glob.glob(file_path)
    if not files:
        print(f"⚠️ No CSV files were found in {file_path}.")
        return None

    print(f"🚀 Loading the file {file_path} into the ecommerce_{table_name} table...")

    with open(files[0], "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [row for row in reader]

        for row in rows:
            placeholders = ','.join(['%s'] * len(row))
            columns = ','.join(row.keys())
            values = list(row.values())

            cursor.execute(
                f"INSERT INTO ecommerce_{table_name} ({columns}) VALUES ({placeholders});",
                values
            )

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Table ecommerce_{table_name} populated with {len(rows)} rows.")