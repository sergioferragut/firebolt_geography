"""
Sailing wind analysis visualization using Firebolt queries.
Renders wind vectors colored by heading difference from desired sailing direction.

Usage example:

  python src/sailing_wind_map.py \
    --lat 36.469271 --lon -81.169424 \
    --desired-heading 45 \
    --table gfs_points \
    --db "$FIREBOLT_DATABASE" \
    --engine "$FIREBOLT_ENGINE" \
    --out sailing_map.html

Notes:
- Uses sailing-specific query with heading optimization and distance analysis.
- Colors wind vectors: green (≤30° diff), orange (≤60° diff), red (>60° diff).
- Shows wind speed in mph and distance from current location.
"""
import argparse
import math
import os
from typing import List

import folium
import pandas as pd

try:
    from firebolt.db import connect
    from firebolt.client.auth import ClientCredentials
except Exception:
    connect = None
    ClientCredentials = None


def meters_per_degree_lon_at_lat(latitude_deg: float) -> float:
    return 111320.0 * math.cos(math.radians(latitude_deg))


def get_firebolt_connection(db: str, engine: str):
    account = os.getenv("FIREBOLT_ACCOUNT")
    client_id = os.getenv("FIREBOLT_CLIENT_ID")
    client_secret = os.getenv("FIREBOLT_CLIENT_SECRET")
    if not (connect and ClientCredentials):
        raise RuntimeError("firebolt SDK not available; install 'firebolt' package")
    if not account:
        raise RuntimeError("FIREBOLT_ACCOUNT not set in environment")
    if not (client_id and client_secret):
        raise RuntimeError("FIREBOLT_CLIENT_ID/SECRET not set in environment")
    return connect(account_name=account, engine_name=engine, database=db, auth=ClientCredentials(client_id, client_secret))


def fetch_sailing_wind_data(db: str, engine: str, table: str, center_lat: float, center_lon: float, desired_heading: int, radius_km: float, limit: int, rect_search: bool) -> pd.DataFrame:
    """Fetch wind data optimized for sailing analysis."""
    from datetime import datetime
    conn = get_firebolt_connection(db, engine)
    radius_m = radius_km * 1000.0
    point_wkt = f"ST_GEOGPOINT({center_lat}, {center_lon})"
   
    # Sailing-specific query with heading analysis (removed time filter for now)
    sql = f"""
    SELECT forecast_ts,  
      MOD( CAST(ROUND(wind_heading_rad * 180 / pi()) AS INTEGER) - 90 + 360 , 360) wind_heading, 
      ABS( MOD( CAST(ROUND(wind_heading_rad * 180 / pi()) AS INTEGER) - 90 + 360 , 360) - {desired_heading}) heading_diff, 
      ROUND(wind_speed * 1000 / 1609,2) wind_mph, 
      ST_X(location) latitude, ST_Y(location) longitude,
      ST_DISTANCE({point_wkt}, location)/1609 distance_from_here_miles
    FROM {table}
    """
    if rect_search:
        # radius to lat degrees
        lat_change = radius_m / 111320 
        # radius to lon degrees
        lat_rad = math.radians(center_lat)
        lon_change = radius_m / (111320 * math.cos(lat_rad))

        rect_wkt = f"'POLYGON(({center_lat - lat_change} {center_lon - lon_change}, {center_lat + lat_change} {center_lon - lon_change}, {center_lat + lat_change} {center_lon + lon_change}, {center_lat - lat_change} {center_lon + lon_change}, {center_lat - lat_change} {center_lon - lon_change}))'"
        sql += f"WHERE ST_COVERS(ST_GEOGFROMTEXT({rect_wkt}), location)"
    else:
        sql += f"WHERE ST_DISTANCE({point_wkt}, location) < {radius_m}"

    sql += f"""
        AND forecast_ts > AGO('6h') AND forecast_ts < CURRENT_TIMESTAMP
        ORDER BY forecast_ts DESC, heading_diff ASC, wind_mph DESC
        LIMIT {limit}
        """
    start_time = datetime.now()
    cur = conn.cursor()
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [c[0] for c in cur.description]
    finally:
        cur.close()
    print(f"Query Time (includes retrieval): ({(datetime.now()-start_time).microseconds / 1000} ms)")
    return pd.DataFrame(rows, columns=cols)


def calculate_vector_endpoints(lat: float, lon: float, heading_deg: float, speed_mph: float) -> tuple[float, float]:
        # Convert mph to m/s for vector scaling (1 mph = 0.44704 m/s)
        speed_ms = speed_mph * 0.44704
        vector_length_km = 3 * speed_ms 

        # Convert heading (0° = North, clockwise) to math angle (0° = East, counter-clockwise)
        math_angle_deg = (90.0 - heading_deg) % 360.0
        meters_per_deg_lat = 111320.0
        meters_per_deg_lon = max(meters_per_degree_lon_at_lat(lat), 1e-6)

        # Convert heading to unit vector components
        heading_rad = math.radians(math_angle_deg)
        unit_u = math.cos(heading_rad)
        unit_v = math.sin(heading_rad)

        dx_m = unit_u * (vector_length_km * 1000.0)
        dy_m = unit_v * (vector_length_km * 1000.0)

        end_lon = lon + (dx_m / meters_per_deg_lon)
        end_lat = lat + (dy_m / meters_per_deg_lat)
        return end_lat, end_lon

def add_directional_marker(fmap: folium.Map, lat: float, lon: float, end_lat: float, end_lon: float, color: str, popup_text: str) -> None:
    folium.PolyLine(
            locations=[(lat, lon), (end_lat, end_lon)],
            color=color,
            weight=1,
            opacity=1,
            popup=popup_text,
        ).add_to(fmap)
        # Add marker with wind info
    folium.Circle(
            location=(lat, lon), 
            radius=2000, 
            weight=1,
            color=color, 
            fill=False, 
            fill_opacity=1,
            popup=popup_text,
        ).add_to(fmap)

def add_sailing_wind_vectors(
    fmap: folium.Map,
    df_points: pd.DataFrame,
    arrow_km_per_ms: float,
) -> None:
    """Add wind vectors colored by sailing heading difference."""
    print(f"Adding {len(df_points)} wind vectors...")
    vector_count = 0
    
    for i, row in df_points.iterrows():
        lat = float(row["latitude"])
        lon = float(row["longitude"])
        heading_deg = float(row["wind_heading"])
        speed_mph = float(row["wind_mph"])
        heading_diff = float(row["heading_diff"])
        
        # Color by heading difference
        if heading_diff <= 30:
            color = "green"  # Good heading
        elif heading_diff <= 60:
            color = "orange"  # Moderate heading
        else:
            color = "red"  # Poor heading
        
        end_lat, end_lon = calculate_vector_endpoints(lat, lon, heading_deg, speed_mph)
        # Create popup with sailing info
        popup_text = f"""
        <b>Sailing Wind Analysis</b><br>
        Wind: {speed_mph:.1f} mph @ {heading_deg:.0f}°<br>
        Heading Diff: {heading_diff:.0f}°<br>
        Distance: {row.get('distance_from_here_miles', 0):.1f} miles<br>
        Forecast: {row['forecast_ts']}
        """
        add_directional_marker(fmap, lat, lon, end_lat, end_lon, color, popup_text)
        
        vector_count += 1
    
    print(f"Successfully added {vector_count} wind vectors to map")


def main() -> None:
    parser = argparse.ArgumentParser(description="Sailing wind analysis visualization")
    parser.add_argument("--lat", type=float, required=True, help="Center latitude")
    parser.add_argument("--lon", type=float, required=True, help="Center longitude")
    parser.add_argument("--desired-heading", type=int, required=True, help="Desired sailing heading in degrees (0-360)")
    parser.add_argument("--radius-km", type=float, default=50.0, help="Radius around center to include points (km)")
    parser.add_argument("--table", type=str, default="gfs_points", help="Firebolt table with wind data")
    parser.add_argument("--db", type=str, default=os.getenv("FIREBOLT_DATABASE"), help="Firebolt database name or env FIREBOLT_DATABASE")
    parser.add_argument("--engine", type=str, default=os.getenv("FIREBOLT_ENGINE"), help="Firebolt engine name or env FIREBOLT_ENGINE")
    parser.add_argument("--limit", type=int, default=1000, help="Max points to render")
    parser.add_argument("--km-per-ms", type=float, default=3.0, help="Arrow length in km per 1 m/s of wind speed")
    parser.add_argument("--out", type=str, default="sailing_map.html", help="Output HTML file path")
    parser.add_argument("--rect-search", action="store_true", help="Use rectangular search instead of circular search")
    args = parser.parse_args()
    
    if not args.db or not args.engine:
        raise SystemExit("Provide --db and --engine or set FIREBOLT_DATABASE and FIREBOLT_ENGINE")

    print(f"Fetching sailing wind data for heading {args.desired_heading}°...")
    df_near = fetch_sailing_wind_data(
        db=args.db,
        engine=args.engine,
        table=args.table,
        center_lat=args.lat,
        center_lon=args.lon,
        desired_heading=args.desired_heading,
        radius_km=args.radius_km,
        limit=args.limit,
        rect_search=args.rect_search,
    )

    if df_near.empty:
        print("No wind data found in the specified area and time range.")
        return

    print(f"Found {len(df_near)} wind points")

    # Build map
    fmap = folium.Map(location=[args.lat, args.lon], zoom_start=10, tiles="cartodbpositron")
    
    end_lat, end_lon = calculate_vector_endpoints(args.lat, args.lon, args.desired_heading, 10)
    add_directional_marker(fmap, args.lat, args.lon, end_lat, end_lon, "blue", "Desired Heading")
  

    # Add wind vectors
    add_sailing_wind_vectors(fmap, df_near, arrow_km_per_ms=args.km_per_ms)

    # Add legend
    legend_html = '''
    <div style="position: fixed; 
                bottom: 50px; left: 50px; width: 200px; height: 90px; 
                background-color: white; border:2px solid grey; z-index:9999; 
                font-size:14px; padding: 10px">
    <p><b>Sailing Wind Legend</b></p>
    <p><i class="fa fa-circle" style="color:green"></i> Good (≤30°)</p>
    <p><i class="fa fa-circle" style="color:orange"></i> Moderate (≤60°)</p>
    <p><i class="fa fa-circle" style="color:red"></i> Poor (>60°)</p>
    </div>
    '''
    fmap.get_root().html.add_child(folium.Element(legend_html))

    fmap.save(args.out)
    print(f"Saved sailing wind map to {args.out}. Open it in a browser.")
    print(f"Green vectors: wind within 30° of desired heading {args.desired_heading}°")
    print(f"Orange vectors: wind within 60° of desired heading")
    print(f"Red vectors: wind more than 60° from desired heading")


if __name__ == "__main__":
    main()
