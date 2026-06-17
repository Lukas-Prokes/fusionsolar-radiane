import json
import os
import sys
import time
import requests
import urllib3
from datetime import datetime, timezone
from fusion_solar_py.client import FusionSolarClient

# Disable SSL verification for FusionSolar's self-signed cert
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
requests.packages.urllib3.disable_warnings()
_original_request = requests.Session.request
def _no_verify_request(self, method, url, **kwargs):
    kwargs.setdefault('verify', False)
    return _original_request(self, method, url, **kwargs)
requests.Session.request = _no_verify_request

CF_ACCOUNT_ID = os.environ['CF_ACCOUNT_ID']
CF_KV_ID = os.environ['CF_KV_ID']
CF_TOKEN = os.environ['CF_TOKEN']

cf_headers = {'Authorization': f'Bearer {CF_TOKEN}'}


def kv_url(key):
    return (
        f'https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}'
        f'/storage/kv/namespaces/{CF_KV_ID}/values/{key}'
    )


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def status_key(station_id):
    return f'FUSIONSOLAR_SYNC_STATUS_{station_id}'


def read_status(station_id):
    resp = requests.get(kv_url(status_key(station_id)), headers=cf_headers, verify=True)
    if resp.status_code == 404:
        return {}
    resp.raise_for_status()
    raw = resp.text.strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f'Invalid FusionSolar status JSON for station={station_id}: {e}')
    if not isinstance(data, dict):
        raise ValueError(f'FusionSolar status for station={station_id} is not an object')
    return data


def write_status(station_id, updates):
    current = {}
    try:
        current = read_status(station_id)
    except Exception as e:
        print(f'Could not read existing status for station={station_id}: {e}', file=sys.stderr)
    payload = {
        **current,
        **updates,
        'stationId': station_id,
        'updatedAt': now_iso(),
    }
    if not payload.get('registeredAt'):
        payload['registeredAt'] = current.get('registeredAt') or updates.get('registeredAt') or now_iso()
    resp = requests.put(
        kv_url(status_key(station_id)),
        data=json.dumps(payload),
        headers={**cf_headers, 'Content-Type': 'application/json'},
        verify=True,
    )
    resp.raise_for_status()
    return payload


def mark_status(station_id, updates):
    last_error = None
    for attempt in range(3):
        try:
            return write_status(station_id, updates)
        except Exception as e:
            last_error = e
            print(
                f'Failed to update FusionSolar status for station={station_id} '
                f'(attempt {attempt + 1}/3): {e}',
                file=sys.stderr,
            )
            time.sleep(0.5 * (attempt + 1))
    print(
        f'Giving up on FusionSolar status update for station={station_id}: {last_error}',
        file=sys.stderr,
    )
    return None


def coerce_number(value):
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def get_path_value(payload, path):
    current = payload
    for part in path:
        if isinstance(current, dict):
            if part in current:
                current = current[part]
                continue
            if isinstance(part, str):
                matched = next(
                    (key for key in current.keys() if isinstance(key, str) and key.lower() == part.lower()),
                    None,
                )
                if matched is None:
                    return None
                current = current[matched]
                continue
            return None
        if isinstance(current, list) and isinstance(part, int):
            if part < 0 or part >= len(current):
                return None
            current = current[part]
            continue
        return None
    return current


def first_number_in(value):
    number = coerce_number(value)
    if number is not None:
        return number
    if isinstance(value, dict):
        for key in ('value', 'power', 'currentPower', 'current_power', 'val', 'data'):
            candidate = first_number_in(value.get(key))
            if candidate is not None:
                return candidate
    if isinstance(value, list):
        for item in value:
            candidate = first_number_in(item)
            if candidate is not None:
                return candidate
    return None


def pick_path_number(payload, paths):
    for path in paths:
        value = get_path_value(payload, path)
        if value is None:
            continue
        candidate = first_number_in(value)
        if candidate is not None:
            return candidate
    return None


def is_valid_plant_dn(value):
    return isinstance(value, str) and value.startswith('NE=')


def _station_identifiers(station):
    return {
        'stationCode': station.get('stationCode'),
        'dn': station.get('dn'),
        'dnId': station.get('dnId'),
        'stationDn': station.get('stationDn'),
        'plantDn': station.get('plantDn'),
    }


def resolve_plant_id(client, job, station_id):
    stored = job.get('resolvedPlantId')
    if is_valid_plant_dn(stored):
        return stored
    if stored is not None:
        print(f'Ignoring invalid resolvedPlantId={stored}; resolving from FusionSolar')

    if is_valid_plant_dn(station_id):
        return station_id

    if station_id is not None and not is_valid_plant_dn(station_id):
        print(f'Ignoring invalid resolvedPlantId={station_id}; resolving from FusionSolar')

    plant_ids = client.get_plant_ids()
    stations = client.get_station_list()

    plant_id_candidates = [str(pid) for pid in plant_ids if is_valid_plant_dn(str(pid))]
    station_id_str = str(station_id)

    for station in stations or []:
        if not isinstance(station, dict):
            continue
        if str(station.get('stationCode') or '') == station_id_str:
            resolved = station.get('dn') or station.get('dnId') or station.get('stationDn') or station.get('plantDn')
            if is_valid_plant_dn(resolved):
                return resolved

    if len(plant_id_candidates) == 1:
        return plant_id_candidates[0]

    raise ValueError(
        f'Could not resolve FusionSolar plant DN for stationId={station_id_str}; '
        f'plantIds={plant_id_candidates}; stationList={[ _station_identifiers(s) for s in stations if isinstance(s, dict) ]}'
    )


# Load sync jobs from KV — guard against all HTTP error codes, not just 404.
resp = requests.get(kv_url('SYNC_JOBS'), headers=cf_headers, verify=True)

if resp.status_code == 404:
    print('No sync jobs registered yet — nothing to do.')
    sys.exit(0)

if not resp.ok:
    # Non-404 failure (e.g. 401 bad token, 403 forbidden, 500 server error).
    # Treat as transient and exit cleanly so GitHub does not mark the run red
    # unless it happens consistently.
    print(
        f'KV read for SYNC_JOBS failed ({resp.status_code}): '
        f'{resp.text[:300]}',
        file=sys.stderr,
    )
    sys.exit(1)

raw_body = resp.text.strip()
if not raw_body:
    print('SYNC_JOBS key exists but is empty — nothing to do.')
    sys.exit(0)

try:
    jobs = json.loads(raw_body)
except json.JSONDecodeError as e:
    # Cloudflare returned something that is not valid JSON.  Log the raw body
    # so we can see whether it was an HTML error page or garbage.
    print(
        f'SYNC_JOBS is not valid JSON: {e}\n'
        f'Raw body (first 300 chars): {raw_body[:300]}',
        file=sys.stderr,
    )
    sys.exit(1)

if not isinstance(jobs, list):
    # Cloudflare API error responses are dicts, not lists.
    print(
        f'SYNC_JOBS parsed to {type(jobs).__name__} instead of list — '
        f'possible Cloudflare API error: {raw_body[:300]}',
        file=sys.stderr,
    )
    sys.exit(1)

print(f'Found {len(jobs)} sync job(s)')

for job in jobs:
    username = job['username']
    password = job['password']
    region = job.get('region', 'uni002eu5')
    station_id = job['stationId']
    kv_key = f'SOLAR_LIVE_{station_id}'
    legacy_kv_key = 'SOLAR_LIVE'

    try:
        mark_status(station_id, {
            'lastAttemptAt': now_iso(),
            'lastStage': 'fetching_kpi',
            'lastErrorAt': None,
            'lastErrorMessage': None,
            'jobId': job.get('jobId'),
            'householdId': job.get('householdId'),
            'userId': job.get('userId'),
            'stationName': job.get('stationName'),
            'region': region,
        })
        client = FusionSolarClient(username, password, huawei_subdomain=region)
        resolved_plant_id = resolve_plant_id(client, job, station_id)
        print(f'Calling get_plant_flow with resolvedPlantId={resolved_plant_id}')
        mark_status(station_id, {
            'resolvedPlantId': resolved_plant_id,
            'jobId': job.get('jobId'),
            'householdId': job.get('householdId'),
            'userId': job.get('userId'),
            'stationName': job.get('stationName'),
            'region': region,
        })
        flow_data = client.get_plant_flow(resolved_plant_id)
        kpi = flow_data if isinstance(flow_data, dict) and flow_data else client.get_current_plant_data(resolved_plant_id)
        mark_status(station_id, {
            'lastStage': 'kpi_fetched',
            'jobId': job.get('jobId'),
            'householdId': job.get('householdId'),
            'userId': job.get('userId'),
            'stationName': job.get('stationName'),
            'region': region,
            'resolvedPlantId': resolved_plant_id,
        })

        if not isinstance(kpi, dict) or not kpi:
            raise ValueError('FusionSolar KPI response was empty or malformed')

        # get_plant_flow exposes the most useful live values for Basalt.
        # Positive BATTERY_POWER means charging in the observed flow balance.
        solar_kw = pick_path_number(kpi, [
            ('PV',),
            ('pv',),
            ('PV', 'value'),
            ('PV', 'power'),
            ('pv', 'value'),
            ('pv', 'power'),
        ])
        battery_soc = pick_path_number(kpi, [
            ('deviceTips', 'SOC'),
            ('deviceTips', 'soc'),
            ('battery', 'SOC'),
            ('battery', 'soc'),
        ])
        battery_power = pick_path_number(kpi, [
            ('deviceTips', 'BATTERY_POWER'),
            ('deviceTips', 'batteryPower'),
            ('battery', 'power'),
            ('battery', 'currentPower'),
        ])
        consumption_kw = pick_path_number(kpi, [
            ('electricalLoad',),
            ('electricalLoad', 'value'),
            ('electricalLoad', 'power'),
            ('load', 'power'),
            ('load',),
        ])
        grid_import = pick_path_number(kpi, [
            ('buy', 'power'),
            ('grid', 'buy', 'power'),
        ])
        grid_export = pick_path_number(kpi, [
            ('sell', 'power'),
            ('grid', 'sell', 'power'),
        ])

        if solar_kw is None:
            solar_kw = pick_path_number(kpi, [
                ('currentPower',),
                ('realTimePower',),
                ('inverterPower',),
                ('inverter_power',),
                ('activePower',),
                ('active_power',),
            ])
        if battery_soc is None:
            battery_soc = pick_path_number(kpi, [
                ('storage_state_of_charge',),
                ('batteryStateOfCharge',),
                ('battery_soc',),
                ('ess_soc',),
            ])
        if battery_power is None:
            battery_power = pick_path_number(kpi, [
                ('storage_charge_discharge_power',),
                ('storageChargeDischargePower',),
                ('battery_power',),
                ('batteryPower',),
            ])
        if consumption_kw is None:
            consumption_kw = pick_path_number(kpi, [
                ('use_power',),
                ('load_power',),
                ('loadPower',),
                ('consumption',),
            ])
        if grid_import is None:
            grid_import = pick_path_number(kpi, [
                ('grid_import',),
                ('gridImportKw',),
            ])
        if grid_export is None:
            grid_export = pick_path_number(kpi, [
                ('grid_export',),
                ('grid_power',),
                ('gridPower',),
                ('pgrid',),
            ])

        battery_soc = battery_soc if battery_soc is not None and battery_soc > 0 else None
        battery_power = battery_power if battery_power is not None else None
        battery_charge = max(0.0, battery_power) if battery_power is not None else None
        battery_discharge = max(0.0, -battery_power) if battery_power is not None else None

        data_to_send = {
            'solar_power': solar_kw,
            'realTimePower': solar_kw,
            'battery_soc': battery_soc,
            'battery_power': battery_power,
            'battery_charge': battery_charge,
            'battery_discharge': battery_discharge,
            'consumption': consumption_kw,
            'grid_import': grid_import,
            'grid_export': grid_export,
            'synced_at': datetime.now(timezone.utc).isoformat(),
            'raw_flow': flow_data,
            'raw': kpi,
        }

        put_resp = requests.put(
            kv_url(kv_key),
            data=json.dumps(data_to_send),
            headers={**cf_headers, 'Content-Type': 'application/json'},
            verify=True,
        )
        put_resp.raise_for_status()

        status_result = mark_status(station_id, {
            'lastStage': 'live_data_written',
            'lastSuccessAt': now_iso(),
            'lastErrorAt': None,
            'lastErrorMessage': None,
            'jobId': job.get('jobId'),
            'householdId': job.get('householdId'),
            'userId': job.get('userId'),
            'stationName': job.get('stationName'),
            'region': region,
            'resolvedPlantId': resolved_plant_id,
        })
        if status_result is None:
            raise RuntimeError(f'FusionSolar live data was written but status update failed for station={station_id}')

        try:
            legacy_put_resp = requests.put(
                kv_url(legacy_kv_key),
                data=json.dumps(data_to_send),
                headers={**cf_headers, 'Content-Type': 'application/json'},
                verify=True,
            )
            legacy_put_resp.raise_for_status()
        except Exception as e:
            print(
                f'Legacy SOLAR_LIVE write failed for station={station_id}: {e}',
                file=sys.stderr,
            )

        print(f'Synced station={station_id} ({job.get("stationName", "")}) → {kv_key}')

    except json.JSONDecodeError as e:
        # FusionSolar returned an HTML error page instead of JSON.
        # This usually means the runner IP is rate-limited or temporarily blocked.
        # Print to stderr so GitHub marks the step as failed.
        mark_status(station_id, {
            'lastStage': 'failed',
            'lastAttemptAt': now_iso(),
            'lastErrorAt': now_iso(),
            'lastErrorMessage': f'FusionSolar returned non-JSON: {e}',
            'jobId': job.get('jobId'),
            'householdId': job.get('householdId'),
            'userId': job.get('userId'),
            'stationName': job.get('stationName'),
            'region': region,
        })
        print(
            f'FusionSolar returned non-JSON for station={station_id} '
            f'(likely rate-limited or IP blocked): {e}',
            file=sys.stderr,
        )

    except Exception as e:
        mark_status(station_id, {
            'lastStage': 'failed',
            'lastAttemptAt': now_iso(),
            'lastErrorAt': now_iso(),
            'lastErrorMessage': str(e),
            'jobId': job.get('jobId'),
            'householdId': job.get('householdId'),
            'userId': job.get('userId'),
            'stationName': job.get('stationName'),
            'region': region,
        })
        print(f'Error syncing station={station_id}: {e}', file=sys.stderr)
