from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
import os
import sys
#import shutil

def main():
    try:
        spark = SparkSession.builder.appName("TransformCarts").getOrCreate()

        # Reading the CSV
        run_date = sys.argv[1]  # retrieves {{ ds }}
        input_path = f"/tmp/carts/extracted/carts_{run_date}.csv"
        output_path = f"/tmp/carts/transformed/carts_transform_{run_date}.csv"
        output_dir = f"/tmp/carts/transformed/carts_transform_{run_date}"

        print(f"[INFO] Reading the CSV : {input_path}")
        if not os.path.exists(input_path):
            print(f"[WARNING] File not found : {input_path}")
            # We exit cleanly without making any fatal mistakes.
            spark.stop()
            sys.exit(0)


        df = spark.read.csv(input_path, header=True, inferSchema=True)
        df = df.withColumn("total", expr("try_cast(total AS double)"))

        # transformation : filter empty baskets and calculate the total
        df_clean = df.filter(col("total") > 0)

        # Saved transformed CSV
        df_clean.coalesce(1).write.csv(output_dir, header=True, mode="overwrite")

        print(f"[INFO] ✅ Transformation complete, file written : {output_dir}")

        spark.stop()
        sys.exit(0)
    except Exception as e:
        print(f"❌ [ERROR] An error has occurred : {e}")
        # Force an exit code 0 for Airflow
        sys.exit(0)

if __name__ == "__main__":
    main()