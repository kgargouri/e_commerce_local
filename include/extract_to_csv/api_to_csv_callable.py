import requests
import pandas as pd
import os
from datetime import datetime
from airflow.hooks.base import BaseHook

def api_to_csv_callable(api_data, run_date):
    """Call the API and save as CSV"""
    try:
        # Retrieving the API connection
        api = BaseHook.get_connection("dummyjson_api")
        url = f"{api.host}/{api_data}"

        # API call
        response = requests.get(url, headers=api.extra_dejson.get("headers", {}))
        if response.status_code != 200:
            raise Exception(f"API not available, code={response.status_code}")

        data = response.json().get(api_data, [])
        if not data:
            raise Exception(f"⚠️ No {api_data} found in the API response")

        # Save as CSV
        path = f"/tmp/{api_data}/extracted/{api_data}_{run_date}.csv"
        os.makedirs(os.path.dirname(path), exist_ok=True)
        pd.DataFrame(data).to_csv(path, index=False)

        print(f"✅ Saved file : {path}")
        return path

    except Exception as e:
        print(f"❌ Error in {api_data}_to_csv_callable : {e}")
        raise