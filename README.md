# firebolt_geography
Testing firebolt geography functionality in a real-time analytics use case.

This project contains a small script to download NOAA GFS GRIB2 files and load wind point data into a Firebolt table using the GEOGRAPHY type. It extracts 10m wind components (u10, v10) and computes wind speed and heading on insert.

Files:
- `src/gfs_to_firebolt.py` - Main script. Downloads GFS GRIB2 by date range, extracts (lon, lat, u10, v10) and stages CSV with WKT `POINT(lon lat)` (SRID=4326). Loads into Firebolt via external table and `INSERT ... SELECT` computing speed/heading.
- `requirements.txt` - Python dependencies. Note: `cfgrib` requires the system `ecCodes` library to be installed.
- `src/visualize_wind_map.py` - Helper to render a Folium map of wind vectors near a given lat/lon using the staged CSVs.

Quick start:

1. Install system dependency ecCodes (macOS, Homebrew):

	brew install eccodes

2. Create a virtualenv and install Python deps:

	python -m venv .venv
	source .venv/bin/activate
	pip install -r requirements.txt

3. Set Firebolt credentials in the environment (service account / client-credentials flow):

	export FIREBOLT_ACCOUNT=your_account_slug
	export FIREBOLT_CLIENT_ID=your_client_id
	export FIREBOLT_CLIENT_SECRET=your_client_secret
	export FIREBOLT_DATABASE=your_database
	export FIREBOLT_ENGINE=your_engine

Alternatively, you can source the helper script to export all required variables in one go (edit the file first to your values):

	source SET_ENV_VARS.sh


4. Run the script for a date range (downloads NOMADS GFS GRIB2, extracts, stages to S3, creates external table, and inserts into the target table):

	python src/gfs_to_firebolt.py \
	 --start-date 2024-12-01 \
	 --end-date 2024-12-02 \
	 --resolution 0p25 \
	 --table gfs_points \
	 --db "$FIREBOLT_DATABASE" \
	 --engine "$FIREBOLT_ENGINE" \
	 --s3-bucket my-bucket \
	 --s3-key data/gfs/gfs_sample.csv

5. Visualize wind vectors around a location from the created CSVs (local files):

	python src/visualize_wind_map.py \
	  --lat 37.7749 --lon -122.4194 \
	  --radius-km 150 \
	  --csv-files "2025*.csv" \
	  --out wind_map.html

Staged load
-------------------------------------------
The script writes extracted rows to local CSVs, uploads them to S3 when `--s3-bucket` and `--s3-key` are provided, creates a Firebolt external table over the S3 prefix, and inserts into the destination table computing:

- wind_speed = sqrt(u^2 + v^2)
- wind_heading_rad = atan2(v, u)

Useful flags:

	--start-date YYYY-MM-DD    # required (inclusive)
	--end-date YYYY-MM-DD      # required (inclusive)
	--resolution 0p25          # GFS grid resolution (default 0p25)
	--table gfs_points         # destination table name
	--db / --engine            # Firebolt database/engine (or env vars)
	--s3-bucket / --s3-key     # stage CSVs to s3://bucket/prefix/file.csv
	--ext-table-name           # optional external table name (auto if omitted)

AWS credentials for S3/external table can be provided via the standard environment variables:

	AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY [/ AWS_SESSION_TOKEN]

or their `AWS_EXTERNAL_*` variants if you prefer to scope credentials specifically for external table creation.

Notes:
- GRIB2 files are large; this script currently iterates all grid points for u10/v10.
- The Firebolt table schema created includes: `obs_date DATE`, `obs_hour INT`, `forecast_hour INT`, `wind_u DOUBLE`, `wind_v DOUBLE`, `wind_speed DOUBLE`, `wind_heading_rad DOUBLE`, `location GEOGRAPHY`.
- The script downloads GFS for cycles 00/06/12/18 and currently `forecast_hour=0` for each day in the specified range.

