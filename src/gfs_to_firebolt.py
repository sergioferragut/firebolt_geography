"""
Download GFS GRIB2 files for a date range, extract 10m winds (u10, v10),
stage CSVs to S3, and load into a Firebolt table using GEOGRAPHY.

Assumptions & notes:
- Firebolt Python client is available (package `firebolt`).
- cfgrib/xarray are used to read GRIB2 (system lib ecCodes required).
- Firebolt connection parameters are read from environment variables:
  FIREBOLT_ACCOUNT, FIREBOLT_CLIENT_ID, FIREBOLT_CLIENT_SECRET, FIREBOLT_DATABASE, FIREBOLT_ENGINE
"""
import os
import sys
import tempfile
import glob
import requests
import csv
import boto3
from typing import Iterable, Tuple

try:
    import xarray as xr
except Exception as e:
    print("xarray/cfgrib required to read GRIB2 files. Install cfgrib and xarray.\n", e)
    raise

try:
    from firebolt.client import Client
except Exception:
    Client = None

def download_file(url: str, dest: str) -> None:
    resp = requests.get(url, stream=True)
    resp.raise_for_status()
    with open(dest, "wb") as f:
        for chunk in resp.iter_content(8192):
            f.write(chunk)

def gfs_nomads_url_for(datetime_ymd: str, cycle: str = '00', resolution: str = '0p25', forecast_hour: int = 0) -> str:
    """Build a simple GFS NOMADS URL for the given date (YYYYMMDD), cycle (00/06/12/18), resolution, and forecast hour.

    Example return:
    https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.YYYYMMDD/00/atmos/gfs.t00z.pgrb2.0p25.f000
    """
    return f"https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.{datetime_ymd}/{cycle}/atmos/gfs.t{cycle}z.pgrb2.{resolution}.f{forecast_hour:03d}"

def find_latest_available_cycle(resolution: str = '0p25', http_timeout: float = 5.0):
    """Return a tuple (date_yyyymmdd, cycle_str, test_url) for the most recent available GFS cycle.

    Logic:
    - Use current UTC minus a 2-hour lag to select a candidate cycle among 00/06/12/18.
    - Verify availability by attempting a lightweight HTTP request for f000; if unavailable, back off
      to the previous cycle, up to a few attempts.
    """
    from datetime import datetime, timedelta, timezone

    def previous_cycle(dt_utc, cycle_int):
        order = [0, 6, 12, 18]
        idx = order.index(cycle_int)
        if idx == 0:
            # go to previous day 18z
            prev_dt = dt_utc - timedelta(days=1)
            return prev_dt, 18
        else:
            return dt_utc, order[idx - 1]

    now_utc = datetime.now(timezone.utc)
    base = now_utc - timedelta(hours=2)
    hour = base.hour
    candidate_cycle = 18 if hour >= 18 else 12 if hour >= 12 else 6 if hour >= 6 else 0
    dt = base

    attempts = 6
    for _ in range(attempts):
        ymd = dt.strftime('%Y%m%d')
        cycle_str = f"{candidate_cycle:02d}"
        url = gfs_nomads_url_for(ymd, cycle=cycle_str, resolution=resolution, forecast_hour=0)
        try:
            # HEAD may not be allowed; use GET with stream to avoid full download
            resp = requests.get(url, stream=True, timeout=http_timeout)
            if resp.status_code == 200:
                try:
                    resp.close()
                except Exception:
                    pass
                return ymd, cycle_str, url
        except Exception:
            pass
        # back off to previous cycle
        dt, candidate_cycle = previous_cycle(dt, candidate_cycle)

    # If all checks failed, return the last attempted (without guarantee)
    return dt.strftime('%Y%m%d'), f"{candidate_cycle:02d}", gfs_nomads_url_for(dt.strftime('%Y%m%d'), cycle=f"{candidate_cycle:02d}", resolution=resolution, forecast_hour=0)



def _find_var_by_patterns(ds, patterns):
    names = list(ds.data_vars)
    lowered = [n.lower() for n in names]
    for p in patterns:
        p_low = p.lower()
        for name, lname in zip(names, lowered):
            if p_low in lname:
                return name
    return None


def extract_wind_data_to_parquet(grib_path, parquet_path, cycle, forecast_hour):
    import cfgrib
    import xarray as xr
    import pandas as pd
    ds = xr.open_dataset(grib_path, engine="cfgrib", 
                        filter_by_keys={'typeOfLevel': 'heightAboveGround', 'level':10},
                        decode_timedelta=True
    )
    df_u = ds['u10'].to_dataframe().reset_index()[['latitude', 'longitude', 'time', 'u10']]
    df_v = ds['v10'].to_dataframe().reset_index()[['latitude', 'longitude', 'time', 'v10']]
    df = pd.merge_ordered(df_u, df_v, on=['latitude', 'longitude', 'time'])
    df['obs_date'] = df['time'].dt.date.astype(str) 
    df['obs_hour'] = int(cycle)
    df['forecast_hour'] = forecast_hour
    df = df.rename(columns={'u10': 'wind_u', 'v10': 'wind_v'})
    # Persist in parquet for efficient external table loading
    df[['latitude','longitude','obs_date','obs_hour','forecast_hour','wind_u','wind_v']].to_parquet(parquet_path, index=False)


def create_firebolt_table(conn, table_name: str) -> None:
    """Create a table with forecast_ts, wind components and location GEOGRAPHY."""
    create_stmt = (
        f"""CREATE TABLE IF NOT EXISTS {table_name} (
           forecast_ts TIMESTAMP,
           wind_u DOUBLE,
           wind_v DOUBLE,
           wind_speed DOUBLE,
           wind_heading_rad DOUBLE,
           location GEOGRAPHY
         )
        PARTITION BY TO_YYYYMMDD(forecast_ts)
        """
    )
    cur = conn.cursor()
    try:
        cur.execute(create_stmt)
    finally:
        cur.close()

def get_firebolt_connection_from_env(db: str, engine: str):
    if Client is None:
        raise RuntimeError("firebolt client package not installed")
    account = os.getenv('FIREBOLT_ACCOUNT')
    client_id = os.getenv('FIREBOLT_CLIENT_ID')
    client_secret = os.getenv('FIREBOLT_CLIENT_SECRET')
    # Prefer client credentials (service account) flow. Provide clearer messages
    if not account:
        raise RuntimeError('FIREBOLT_ACCOUNT environment variable not set; please set your Firebolt account slug')

    if client_id and client_secret:
        try:
            # Use the documented connect() helper from the SDK and pass the account name
            from firebolt.db import connect
            from firebolt.client.auth import ClientCredentials
        except Exception as e:
            raise RuntimeError('firebolt SDK missing connect() or ClientCredentials: ' + str(e))
        try:
            # The SDK expects an account_name kwarg; pass it to avoid internal assertion failures
            conn = connect(account_name=account, engine_name=engine, database=db, auth=ClientCredentials(client_id, client_secret))
            return conn
        except AssertionError:
            # This often means the SDK didn't receive an account name; re-raise with context
            raise RuntimeError('Failed to connect to Firebolt: account_name was not accepted by SDK. Verify FIREBOLT_ACCOUNT value')
        except Exception as e:
            raise RuntimeError('Failed to connect to Firebolt using ClientCredentials: ' + str(e))
    else:
        raise RuntimeError('FIREBOLT_CLIENT_ID and FIREBOLT_CLIENT_SECRET environment variables not set; please set them for service account authentication')

def upload_file_to_s3(local_path: str, bucket: str, key: str, aws_region: str = None) -> str:
    s3 = boto3.client('s3', region_name=aws_region)
    s3.upload_file(local_path, bucket, key)
    return f's3://{bucket}/{key}'


def create_firebolt_external_table(conn, ext_table_name: str, s3_url: str) -> bool:
    """Create an external table pointing at objects in the given S3 prefix.

    - s3_location should be a folder-like URL ending with a slash: s3://bucket/prefix/
    Returns True on success, False on failure.
    """

    ak = os.getenv('AWS_EXTERNAL_ACCESS_KEY_ID') or os.getenv('AWS_ACCESS_KEY_ID')
    sk = os.getenv('AWS_EXTERNAL_SECRET_ACCESS_KEY') or os.getenv('AWS_SECRET_ACCESS_KEY')
    token = os.getenv('AWS_EXTERNAL_SESSION_TOKEN') or os.getenv('AWS_SESSION_TOKEN')
    if ak and sk:
        cred_parts = [f"AWS_ACCESS_KEY_ID='{ak}'", f"AWS_SECRET_ACCESS_KEY='{sk}'"]
        if token:
            cred_parts.append(f"AWS_SESSION_TOKEN='{token}'")
        cred_clause = ", ".join(cred_parts)
        ddl =  "\n".join([
            f"CREATE EXTERNAL TABLE IF NOT EXISTS {ext_table_name} (",
            "  latitude DOUBLE,",
            "  longitude DOUBLE,",
            "  obs_date TIMESTAMP,",
            "  obs_hour INT,",
            "  forecast_hour INT,",
            "  wind_u DOUBLE,",
            "  wind_v DOUBLE",
            ")",
            f"URL = '{s3_url}'",
            f"CREDENTIALS = ({cred_clause})",
            "TYPE = PARQUET",
            f"OBJECT_PATTERN = '*.parquet'"
        ])
    else:
        # Fail fast with a clear message since staging requires credentials
        raise RuntimeError('Missing AWS credentials for external table. Set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY or AWS_EXTERNAL_* env vars.')
    print(f"DDL:{ddl}")
    # Try variants in order until one succeeds
    # First, drop any existing external table with the same name to avoid stale/wrong definitions
    try:
        drop_cur = conn.cursor()
        try:
            drop_cur.execute(f"DROP EXTERNAL TABLE IF EXISTS {ext_table_name}")
            print(f'Dropped existing external table {ext_table_name} (if it existed)')
        except Exception:
            pass
        finally:
            drop_cur.close()
    except Exception:
        pass

    cur = conn.cursor()
    try:
        cur.execute(ddl)
        print('Created external table using DDL variant')
        result = True
    except Exception as e:
        print('External table creation failed:', e)
        result = False
    finally:
        cur.close()
    return result


def insert_from_external_table(conn, table_name: str, ext_table_name: str) -> None:
        """Insert from the external table into the final table computing speed, heading, and forecast_ts in SQL."""

        insert_stmt = f"""
        INSERT INTO {table_name} (forecast_ts, wind_u, wind_v, wind_speed, wind_heading_rad, location)
        SELECT
            DATE_ADD('hour', COALESCE(obs_hour, 0) + COALESCE(forecast_hour, 0), TRY_CAST(obs_date AS TIMESTAMP)) AS forecast_ts,
            wind_u,
            wind_v,
            sqrt((wind_u)*(wind_u) + (wind_v)*(wind_v)) AS wind_speed,
            atan2(wind_v, wind_u) AS wind_heading_rad,
            CAST(CONCAT('SRID=4326;POINT(', CAST(longitude AS STRING), ' ', CAST(latitude AS STRING), ')') AS GEOGRAPHY) as location
        FROM {ext_table_name}
        """
        cur = conn.cursor()
        try:
            cur.execute(insert_stmt)
        finally:
            cur.close()

def cleanup_local_grib_files():
    """Remove any local GRIB and index files created during download."""
    patterns = ['./grib_*.grib2', './grib_*.idx']
    for pattern in patterns:
        for path in glob.glob(pattern):
            try:
                os.remove(path)
            except Exception:
                pass

def main():
    import argparse
    p = argparse.ArgumentParser(description='Download GFS GRIB2 (u10, v10) and load to Firebolt via S3 external table')
    p.add_argument('--table', default='gfs_points', help='Firebolt destination table name')
    p.add_argument('--db', default=os.getenv('FIREBOLT_DATABASE'), help='Firebolt database name (or env FIREBOLT_DATABASE)')
    p.add_argument('--engine', default=os.getenv('FIREBOLT_ENGINE'), help='Firebolt engine name (or env FIREBOLT_ENGINE)')
    p.add_argument('--s3-bucket', required=True, help='S3 bucket to upload staged CSVs')
    p.add_argument('--s3-key', required=True, help='S3 key/prefix for staged CSVs (e.g. data/gfs/out.csv)')
    p.add_argument('--aws-region', default=None, help='AWS region for S3 upload')
    p.add_argument('--ext-table-name', default=None, help='name for temporary external table in Firebolt')
    p.add_argument('--start-date', required=False, help='start date YYYY-MM-DD (inclusive)')
    p.add_argument('--end-date', required=False, help='end date YYYY-MM-DD (inclusive)')
    p.add_argument('--resolution', default='0p25', help='GFS resolution (e.g. 0p25)')
    p.add_argument('--latest-only', action='store_true', help='Only compute and print the most recent available GFS date/cycle and exit')
    args = p.parse_args()

    if args.latest_only:
        ymd, cycle, url = find_latest_available_cycle(resolution=args.resolution)
        print(f"latest_date={ymd} cycle={cycle} url={url}")
        return

    if not args.db or not args.engine:
        print('Provide --db and --engine or set FIREBOLT_DATABASE and FIREBOLT_ENGINE env vars')
        sys.exit(1)

    # Download and extract for the date range
    csv_paths = []
    from datetime import datetime, timedelta
    if not args.start_date or not args.end_date:
        from datetime import datetime, timezone
        # Default to the most recent available cycle date only
        ymd, cycle, _ = find_latest_available_cycle(resolution=args.resolution)
        try:
            start_dt = datetime.fromisoformat(f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:8]}")
            end_dt = start_dt
            # Only process the detected latest cycle
            start_cycle = int(cycle)
            end_cycle = start_cycle + 6  # exclusive end for range()
        except Exception:
            print('Failed to compute latest available date range')
            sys.exit(1)
    else:
        try:
            start_dt = datetime.fromisoformat(args.start_date)
            end_dt = datetime.fromisoformat(args.end_date)
            # Process all daily GFS cycles: 00,06,12,18
            start_cycle = 0
            end_cycle = 24  # exclusive end for range()

        except Exception:
            print('Start/end dates must be in YYYY-MM-DD format')
            sys.exit(1)
    if end_dt < start_dt:
        print('end-date must be >= start-date')
        sys.exit(1)

    cur_dt = start_dt
    while cur_dt <= end_dt:
        dt_str = cur_dt.strftime('%Y%m%d')
        for cycle_int in range(int(start_cycle), int(end_cycle), 6):
            cycle = f"{cycle_int:02d}"
            for forecast_hour in range(6):
                url = gfs_nomads_url_for(dt_str, cycle=cycle, resolution=args.resolution, forecast_hour=forecast_hour)
                print('Downloading', url)
                tmp_name = f"./grib_{dt_str}_{cycle}_{args.resolution}_{forecast_hour}.grib2"
                download_file(url, tmp_name)
                print('Downloaded to', tmp_name)
                print('Extracting points...')

                date_part = dt_str
                hour_part = '00' if cycle is None else f"{int(cycle):02d}"
                pq_filename = f"{date_part}_{hour_part}_{int(forecast_hour):03d}.parquet"
                pq_path = os.path.join(os.getcwd(), pq_filename)
                extract_wind_data_to_parquet(tmp_name, pq_path, cycle, forecast_hour)

                csv_paths.append(pq_path)
                try:
                    os.remove(tmp_name)
                except Exception:
                    pass
        cur_dt += timedelta(days=1)

    # Upload staged CSVs to S3
    print('Uploading staged CSVs to S3 ...')
    key = args.s3_key
    base = args.s3_key.rstrip('/')
    s3_url = f's3://{args.s3_bucket}/{base}/'  # base prefix for external table
    for parquet_path in csv_paths:
        upload_key = f"{base}/{os.path.basename(parquet_path)}"
        print(f'Uploading {parquet_path} to s3://{args.s3_bucket}/{upload_key} ...')
        s3_url_file = upload_file_to_s3(parquet_path, args.s3_bucket, upload_key, args.aws_region)
        print('Uploaded to', s3_url_file)
        try:
            os.remove(parquet_path)
            print('Deleted local Parquet', parquet_path)
        except Exception:
            pass
    print('Upload complete.')

    print('Connecting to Firebolt...')
    conn = get_firebolt_connection_from_env(args.db, args.engine)

    print('Creating table (if not exists)')
    create_firebolt_table(conn, args.table)

    # Create an external table over the staged CSV and INSERT from it into the final table.
    ext_table = args.ext_table_name or f"ext_{args.table}_staged"
    print(f'Creating external table {ext_table} pointing at {s3_url}...')
    if create_firebolt_external_table(conn, ext_table, s3_url):
        print(f'Inserting into {args.table} from external table {ext_table}...')
        insert_from_external_table(conn, args.table, ext_table)
        print('Insert from external table complete')
    else:
        print('Failed to create external table; aborting insert step')
        # Cleanup local GRIB remnants before exiting
        cleanup_local_grib_files()
        return
    # Optionally drop the external table
    try:
        cur = conn.cursor()
        try:
            cur.execute(f"DROP EXTERNAL TABLE IF EXISTS {ext_table}")
            print(f'Dropped external table {ext_table}')
        finally:
            cur.close()
    except Exception:
        pass

    # Final cleanup of local GRIB remnants
    cleanup_local_grib_files()

if __name__ == '__main__':
    main()
