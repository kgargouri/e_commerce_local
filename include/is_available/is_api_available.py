import requests
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue

def is_api_available_callable():
    api = BaseHook.get_connection('dummyjson_api')
    url=f"{api.host}{api.extra_dejson['endpoint']}"
    try:
        response = requests.get(url, headers=api.extra_dejson['headers'])
        if response.status_code == 200:
            return PokeReturnValue(is_done=True, xcom_value=url)
        return False
    except Exception as e:
        print(f"❌ Error during API call : {e}")
        return False