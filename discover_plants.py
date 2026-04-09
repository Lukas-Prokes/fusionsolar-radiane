import os
import json
import requests
from fusion_solar_py.client import FusionSolarClient

HUAWEI_USER = os.environ['HUAWEI_USER']
HUAWEI_PASS = os.environ['HUAWEI_PASS']
HUAWEI_REGION = os.environ.get('HUAWEI_REGION', 'uni002eu5')
JOB_ID = os.environ['JOB_ID']

CF_ACCOUNT_ID = os.environ['CF_ACCOUNT_ID']
CF_KV_ID = os.environ['CF_KV_ID']
CF_TOKEN = os.environ['CF_TOKEN']


def kv_url(key):
    return (
        f'https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}'
        f'/storage/kv/namespaces/{CF_KV_ID}/values/{key}'
    )


cf_headers = {'Authorization': f'Bearer {CF_TOKEN}'}

client = FusionSolarClient(HUAWEI_USER, HUAWEI_PASS, huawei_subdomain=HUAWEI_REGION)
stations = client.get_station_list()

# stationCode is required for get_station_real_kpi() calls
plants = [
    {
        'id': s['stationCode'],
        'name': s.get('stationName', 'Unknown Plant'),
        'capacity': s.get('capacity', None),
        'location': s.get('stationAddr', s.get('address', None)),
    }
    for s in stations
    if s.get('stationCode')
]

result = json.dumps({'success': True, 'plants': plants})

requests.put(
    kv_url(f'PLANT_DISCOVERY_{JOB_ID}'),
    data=result,
    headers={**cf_headers, 'Content-Type': 'application/json'},
).raise_for_status()

# Delete temp credentials now that we're done
requests.delete(kv_url(f'DISCOVER_CREDS_{JOB_ID}'), headers=cf_headers)

print(f'Stored {len(plants)} plants for job {JOB_ID}')
