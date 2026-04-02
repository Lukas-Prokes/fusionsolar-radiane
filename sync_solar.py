import os
import json
import requests
from fusion_solar_py.client import FusionSolarClient

def run_sync():
    user = os.getenv("HUAWEI_USER")
    password = os.getenv("HUAWEI_PASS")
    subdomain = "uni002eu5" 

    try:
        print(f"Connecting to {subdomain}...")
        client = FusionSolarClient(user, password, huawei_subdomain=subdomain)
        
        # Get the stats object
        stats = client.get_power_status()
        
        # Correct attribute names for this library:
        # current_power_kw, total_power_today_kwh, total_power_kwh
        data_to_send = {
            "current_power": getattr(stats, 'current_power_kw', 0),
            "daily_yield": getattr(stats, 'total_power_today_kwh', 0),
            "total_yield": getattr(stats, 'total_power_kwh', 0),
            # Battery fields can vary by plant setup, so we use a safe get
            "battery_soc": getattr(stats, 'battery_soc', "N/A"),
            "battery_power": getattr(stats, 'battery_power', "N/A")
        }

        print(f"Data Extracted: {data_to_send}")

        # Push to Cloudflare
        cf_url = f"https://api.cloudflare.com/client/v4/accounts/{os.getenv('CF_ACCOUNT_ID')}/storage/kv/namespaces/{os.getenv('CF_KV_ID')}/values/SOLAR_LIVE"
        headers = {
            "Authorization": f"Bearer {os.getenv('CF_TOKEN')}",
            "Content-Type": "text/plain"
        }
        
        response = requests.put(cf_url, headers=headers, data=json.dumps(data_to_send))
        
        if response.status_code == 200:
            print("Success! Cloudflare KV updated.")
        else:
            print(f"Cloudflare Error: {response.text}")

    except Exception as e:
        print(f"Failed to sync: {str(e)}")
        # This will help us if there's still a naming mismatch
        if 'stats' in locals():
            print(f"Available data fields are: {dir(stats)}")

if __name__ == "__main__":
    run_sync()
