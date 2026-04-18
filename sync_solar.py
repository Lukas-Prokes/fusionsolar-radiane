import json
import os
import ssl
import requests
import urllib3
from datetime import datetime, timezone
from fusion_solar_py.client import FusionSolarClient

# Disable SSL verification globally to handle FusionSolar's self-signed cert
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
old_init = ssl.SSLContext.__init__
def patched_init(self, *args, **kwargs):
    old_init(self, *args, **kwargs)
    self.check_hostname = False
    self.verify_mode = ssl.CERT_NONE
ssl.SSLContext.__init__ = patched_init

CF_ACCOUNT_ID = os.environ['CF_ACCOUNT_ID']
CF_KV_ID = os.environ['CF_KV_ID']
CF_TOKEN = os.environ['CF_TOKEN']

cf_headers = {'Authorization': f'Bearer {CF_TOKEN}'}


def kv_url(key):
    return (
        f'https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}'
        f'/storage/kv/namespaces/{CF_KV_ID}/values/{key}'
    )


# Load sync jobs from KV
resp = requests.get(kv_url('SYNC_JOBS'), headers=cf_headers)
if resp.status_code == 404 or not resp.text.strip():
    print('No sync jobs registered yet — nothing to do.')
    exit(0)

jobs = json.loads(resp.text)
print(f'Found {len(jobs)} sync job(s)')

for job in jobs:
    username = job['username']
    password = job['password']
    region = job.get('region', 'uni002eu5')
    station_id = job['stationId']
    kv_key = f'SOLAR_LIVE_{station_id}'

    try:
        client = FusionSolarClient(username, password, huawei_subdomain=region)
        kpi = client.get_station_real_kpi(station_code=station_id)

        # storage_charge_discharge_power: positive = charging, negative = discharging
        batt_raw = kpi.get('storage_charge_discharge_power', 0) or 0

        data_to_send = {
            'solar_power': kpi.get('radiation_intensity', 0) or 0,
            'battery_soc': kpi.get('storage_state_of_charge', 0) or 0,
            'battery_charge': max(0.0, batt_raw),
            'battery_discharge': max(0.0, -batt_raw),
            'consumption': kpi.get('inverter_power', kpi.get('use_power', 0)) or 0,
            'grid_export': kpi.get('grid_power', 0) or 0,
            'synced_at': datetime.now(timezone.utc).isoformat(),
            'raw': kpi,
        }

        requests.put(
            kv_url(kv_key),
            data=json.dumps(data_to_send),
            headers={**cf_headers, 'Content-Type': 'application/json'},
        ).raise_for_status()

        print(f'Synced station={station_id} ({job.get("stationName", "")}) → {kv_key}')

    except Exception as e:
        print(f'Error syncing station={station_id}: {e}')
        # Continue with remaining jobs
