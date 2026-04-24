#!/usr/bin/env python3
"""
One-time script to build pre-aggregated parquet files from BTS On-Time Performance data.

Downloads monthly zip files from BTS PREZIP (Jan 2018 – Jan 2025), extracts CSVs,
computes aggregations at multiple granularities, then discards the raw data.

Output layout (all committed to git — total ~5–15 MB):
  data/agg/
    daily_overall.parquet         overall KPIs by day (~2.6K rows)
    daily_carrier.parquet         KPIs by day × carrier (~52K rows)
    daily_origin.parquet          KPIs by day × origin airport
    monthly_cause.parquet         delay cause minutes by month × cause (~425 rows)
    monthly_carrier_cause.parquet delay cause minutes by month × carrier × cause (~8.5K rows)
    monthly_origin.parquet        KPIs by month × origin airport (~30K rows)
    monthly_dest.parquet          KPIs by month × dest airport (~30K rows)
  data/lookup/
    airports.parquet              airport code → name, city, state, lat, lon, country
    states.parquet                state code → state name, census region
    carriers.parquet              IATA code → full airline name

Usage:
  uv run scripts/build_data.py
  uv run scripts/build_data.py --start 2018-01 --end 2025-01   # default range
  uv run scripts/build_data.py --resume                         # skip months already processed
"""

import argparse
import io
import time
import zipfile
from pathlib import Path

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

ROOT = Path(__file__).parent.parent
AGG_DIR = ROOT / "data" / "agg"
LOOKUP_DIR = ROOT / "data" / "lookup"
RAW_DIR = ROOT / "raw_downloads"

AGG_DIR.mkdir(parents=True, exist_ok=True)
LOOKUP_DIR.mkdir(parents=True, exist_ok=True)
RAW_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# External data sources
# ---------------------------------------------------------------------------

PREZIP_BASE = (
    "https://transtats.bts.gov/PREZIP/"
    "On_Time_Marketing_Carrier_On_Time_Performance_Beginning_January_2018"
)
OURAIRPORTS_URL = "https://davidmegginson.github.io/ourairports-data/airports.csv"
BTS_CARRIERS_URL = "https://transtats.bts.gov/Download_Lookup.asp?Y11x72=Y_haVdhR_PNeeVRef"
BTS_AIRPORTS_URL = "https://transtats.bts.gov/Download_Lookup.asp?Y11x72=Y_NVecbeg"

# ---------------------------------------------------------------------------
# Column names (BTS prezipped CSVs use display names, not API names)
# ---------------------------------------------------------------------------

COL_DATE = "FlightDate"
COL_YEAR = "Year"
COL_MONTH = "Month"
COL_CARRIER = "Operating_Airline"
COL_ORIGIN = "Origin"
COL_DEST = "Dest"
COL_ORIGIN_CITY = "OriginCityName"
COL_ORIGIN_STATE = "OriginState"
COL_ORIGIN_STATE_NAME = "OriginStateName"
COL_DEST_CITY = "DestCityName"
COL_DEST_STATE = "DestState"
COL_DEST_STATE_NAME = "DestStateName"
COL_DEP_DEL15 = "DepDel15"
COL_ARR_DEL15 = "ArrDel15"
COL_CANCELLED = "Cancelled"

CAUSE_COLS = [
    "CarrierDelay",
    "WeatherDelay",
    "NASDelay",
    "SecurityDelay",
    "LateAircraftDelay",
]

# Friendly cause labels used throughout the app
CAUSE_LABELS = {
    "CarrierDelay": "Carrier",
    "WeatherDelay": "Weather",
    "NASDelay": "NAS / Air Traffic Control",
    "SecurityDelay": "Security",
    "LateAircraftDelay": "Late Aircraft",
}

# ---------------------------------------------------------------------------
# Census regions (https://www2.census.gov/geo/pdfs/maps-data/maps/reference/us_regdiv.pdf)
# ---------------------------------------------------------------------------

STATE_REGIONS = {
    "CT": ("Connecticut", "Northeast"),
    "ME": ("Maine", "Northeast"),
    "MA": ("Massachusetts", "Northeast"),
    "NH": ("New Hampshire", "Northeast"),
    "RI": ("Rhode Island", "Northeast"),
    "VT": ("Vermont", "Northeast"),
    "NJ": ("New Jersey", "Northeast"),
    "NY": ("New York", "Northeast"),
    "PA": ("Pennsylvania", "Northeast"),
    "IL": ("Illinois", "Midwest"),
    "IN": ("Indiana", "Midwest"),
    "MI": ("Michigan", "Midwest"),
    "OH": ("Ohio", "Midwest"),
    "WI": ("Wisconsin", "Midwest"),
    "IA": ("Iowa", "Midwest"),
    "KS": ("Kansas", "Midwest"),
    "MN": ("Minnesota", "Midwest"),
    "MO": ("Missouri", "Midwest"),
    "NE": ("Nebraska", "Midwest"),
    "ND": ("North Dakota", "Midwest"),
    "SD": ("South Dakota", "Midwest"),
    "DE": ("Delaware", "South"),
    "FL": ("Florida", "South"),
    "GA": ("Georgia", "South"),
    "MD": ("Maryland", "South"),
    "NC": ("North Carolina", "South"),
    "SC": ("South Carolina", "South"),
    "VA": ("Virginia", "South"),
    "DC": ("District of Columbia", "South"),
    "WV": ("West Virginia", "South"),
    "AL": ("Alabama", "South"),
    "KY": ("Kentucky", "South"),
    "MS": ("Mississippi", "South"),
    "TN": ("Tennessee", "South"),
    "AR": ("Arkansas", "South"),
    "LA": ("Louisiana", "South"),
    "OK": ("Oklahoma", "South"),
    "TX": ("Texas", "South"),
    "AZ": ("Arizona", "West"),
    "CO": ("Colorado", "West"),
    "ID": ("Idaho", "West"),
    "MT": ("Montana", "West"),
    "NV": ("Nevada", "West"),
    "NM": ("New Mexico", "West"),
    "UT": ("Utah", "West"),
    "WY": ("Wyoming", "West"),
    "AK": ("Alaska", "West"),
    "CA": ("California", "West"),
    "HI": ("Hawaii", "West"),
    "OR": ("Oregon", "West"),
    "WA": ("Washington", "West"),
    "PR": ("Puerto Rico", "Territory"),
    "VI": ("U.S. Virgin Islands", "Territory"),
    "GU": ("Guam", "Territory"),
    "MP": ("Northern Mariana Islands", "Territory"),
    "AS": ("American Samoa", "Territory"),
    "TT": ("Trust Territory", "Territory"),
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def months_in_range(start: str, end: str) -> list[tuple[int, int]]:
    s_y, s_m = map(int, start.split("-"))
    e_y, e_m = map(int, end.split("-"))
    return [
        (y, m)
        for y in range(s_y, e_y + 1)
        for m in range(1, 13)
        if (s_y, s_m) <= (y, m) <= (e_y, e_m)
    ]


def _read_csv_bytes(raw: bytes) -> pd.DataFrame | None:
    for encoding in ("utf-8", "latin-1", "cp1252"):
        try:
            return pd.read_csv(io.BytesIO(raw), encoding=encoding, low_memory=False)
        except UnicodeDecodeError:
            continue
    # Last resort: replace undecodable bytes and skip bad lines
    try:
        text = raw.decode("utf-8", errors="replace")
        print("  (encoding fallback: replaced undecodable bytes)", end=" ", flush=True)
        return pd.read_csv(io.StringIO(text), on_bad_lines="skip", low_memory=False)
    except Exception as e:
        print(f"  CSV parse failed: {e}")
        return None


def download_zip(year: int, month: int) -> pd.DataFrame | None:
    csv_path = RAW_DIR / f"{year}_{month:02d}.csv"

    if csv_path.exists():
        print(f"  {year}-{month:02d}  loading from cache...", end=" ", flush=True)
        df = pd.read_csv(csv_path, low_memory=False)
        print(f"{len(df):,} rows")
        return df

    url = f"{PREZIP_BASE}_{year}_{month}.zip"
    print(f"  {year}-{month:02d}  downloading...", end=" ", flush=True)
    t0 = time.time()
    try:
        resp = requests.get(url, timeout=180)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"FAILED ({e})")
        return None

    try:
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            csv_files = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not csv_files:
                print("no CSV in zip")
                return None
            with zf.open(csv_files[0]) as f:
                raw = f.read()
            df = _read_csv_bytes(raw)
            if df is None:
                return None
            df.columns = df.columns.str.strip()
    except zipfile.BadZipFile as e:
        print(f"bad zip ({e})")
        return None

    df.to_csv(csv_path, index=False)

    elapsed = time.time() - t0
    mb = len(resp.content) / 1_048_576
    print(f"{len(df):,} rows  {mb:.0f} MB  {elapsed:.1f}s")
    return df


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    if COL_DATE in df.columns:
        df[COL_DATE] = pd.to_datetime(df[COL_DATE], errors="coerce")

    numeric = [COL_DEP_DEL15, COL_ARR_DEL15, COL_CANCELLED] + CAUSE_COLS
    for col in numeric:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    return df


def make_agg_spec(df: pd.DataFrame) -> dict:
    """Named aggregation spec for core KPI metrics."""
    spec = {
        "total_flights": (COL_DEP_DEL15, "count"),
        "dep_delayed": (COL_DEP_DEL15, "sum"),
        "arr_delayed": (COL_ARR_DEL15, "sum"),
        "cancelled": (COL_CANCELLED, "sum"),
    }
    for col in CAUSE_COLS:
        if col in df.columns:
            spec[f"{col.lower()}_mins"] = (col, "sum")
    return spec


def extract_airport_meta(df: pd.DataFrame, meta: dict) -> None:
    """Accumulate airport → city/state metadata from a month's dataframe."""
    for code_col, city_col, state_col, state_name_col in [
        (COL_ORIGIN, COL_ORIGIN_CITY, COL_ORIGIN_STATE, COL_ORIGIN_STATE_NAME),
        (COL_DEST, COL_DEST_CITY, COL_DEST_STATE, COL_DEST_STATE_NAME),
    ]:
        if not all(c in df.columns for c in [code_col, city_col, state_col]):
            continue
        unique = df[[code_col, city_col, state_col, state_name_col]].drop_duplicates(
            subset=[code_col]
        )
        for _, row in unique.iterrows():
            code = row[code_col]
            if pd.isna(code) or code in meta:
                continue
            meta[code] = {
                "iata_code": code,
                "city": row.get(city_col, ""),
                "state": row.get(state_col, ""),
                "state_name": row.get(state_name_col, ""),
            }


# ---------------------------------------------------------------------------
# Main build
# ---------------------------------------------------------------------------


def build(start: str, end: str, resume: bool) -> None:
    months = months_in_range(start, end)
    print(f"Building data for {len(months)} months: {start} → {end}\n")

    # Load existing data when resuming
    def load_existing(path: Path) -> pd.DataFrame | None:
        if resume and path.exists():
            return pd.read_parquet(path)
        return None

    # Accumulators keyed by output file
    acc: dict[str, list[pd.DataFrame]] = {
        "daily_overall": [],
        "daily_carrier": [],
        "daily_origin": [],
        "monthly_cause": [],
        "monthly_carrier_cause": [],
        "monthly_origin": [],
        "monthly_dest": [],
        "monthly_od": [],
    }
    airport_meta: dict[str, dict] = {}

    # Figure out which months to skip when resuming
    processed_months: set[tuple[int, int]] = set()
    if resume:
        existing = load_existing(AGG_DIR / "daily_overall.parquet")
        if existing is not None and COL_DATE in existing.columns:
            existing[COL_DATE] = pd.to_datetime(existing[COL_DATE])
            for _, row in existing.iterrows():
                processed_months.add((row[COL_DATE].year, row[COL_DATE].month))
            print(f"Resuming — {len(processed_months)} months already done\n")

    for year, month in months:
        if (year, month) in processed_months:
            print(f"  {year}-{month:02d}  (cached, skipping)")
            continue

        df = download_zip(year, month)
        if df is None:
            continue
        df = clean_df(df)

        agg_spec = make_agg_spec(df)

        # daily_overall
        if COL_DATE in df.columns:
            g = df.groupby(COL_DATE).agg(**agg_spec).reset_index()
            acc["daily_overall"].append(g)

        # daily_carrier
        if COL_DATE in df.columns and COL_CARRIER in df.columns:
            g = df.groupby([COL_DATE, COL_CARRIER]).agg(**agg_spec).reset_index()
            acc["daily_carrier"].append(g)

        # daily_origin — enables per-airport day-of-week stats in the flight planner
        if COL_DATE in df.columns and COL_ORIGIN in df.columns:
            g = df.groupby([COL_DATE, COL_ORIGIN]).agg(**agg_spec).reset_index()
            acc["daily_origin"].append(g)

        # monthly_cause — melt cause columns to long format
        cause_avail = [c for c in CAUSE_COLS if c in df.columns]
        if cause_avail:
            g = df.groupby([COL_YEAR, COL_MONTH])[cause_avail].sum().reset_index()
            g = g.melt(
                id_vars=[COL_YEAR, COL_MONTH],
                value_vars=cause_avail,
                var_name="cause_raw",
                value_name="delay_mins",
            )
            g["cause"] = g["cause_raw"].map(CAUSE_LABELS)
            g = g.drop(columns="cause_raw")
            acc["monthly_cause"].append(g)

        # monthly_carrier_cause
        if cause_avail and COL_CARRIER in df.columns:
            g = df.groupby([COL_YEAR, COL_MONTH, COL_CARRIER])[cause_avail].sum().reset_index()
            g = g.melt(
                id_vars=[COL_YEAR, COL_MONTH, COL_CARRIER],
                value_vars=cause_avail,
                var_name="cause_raw",
                value_name="delay_mins",
            )
            g["cause"] = g["cause_raw"].map(CAUSE_LABELS)
            g = g.drop(columns="cause_raw")
            acc["monthly_carrier_cause"].append(g)

        # monthly_origin
        if all(c in df.columns for c in [COL_YEAR, COL_MONTH, COL_ORIGIN]):
            g = df.groupby([COL_YEAR, COL_MONTH, COL_ORIGIN]).agg(**agg_spec).reset_index()
            acc["monthly_origin"].append(g)

        # monthly_dest
        if all(c in df.columns for c in [COL_YEAR, COL_MONTH, COL_DEST]):
            g = df.groupby([COL_YEAR, COL_MONTH, COL_DEST]).agg(**agg_spec).reset_index()
            acc["monthly_dest"].append(g)

        # monthly_od — origin × destination route stats (flight planner)
        if all(c in df.columns for c in [COL_YEAR, COL_MONTH, COL_ORIGIN, COL_DEST]):
            g = (
                df.groupby([COL_YEAR, COL_MONTH, COL_ORIGIN, COL_DEST])
                .agg(
                    total_flights=(COL_DEP_DEL15, "count"),
                    dep_delayed=(COL_DEP_DEL15, "sum"),
                    arr_delayed=(COL_ARR_DEL15, "sum"),
                    cancelled=(COL_CANCELLED, "sum"),
                )
                .reset_index()
            )
            acc["monthly_od"].append(g)

        # airport metadata
        extract_airport_meta(df, airport_meta)

    # -----------------------------------------------------------------------
    # Write aggregation parquets
    # -----------------------------------------------------------------------
    print("\nWriting aggregation parquets...")
    for name, chunks in acc.items():
        if not chunks:
            print(f"  {name}: no data, skipping")
            continue
        out_path = AGG_DIR / f"{name}.parquet"

        # Merge with existing if resuming
        if resume and out_path.exists():
            existing = pd.read_parquet(out_path)
            chunks = [existing] + chunks

        combined = pd.concat(chunks, ignore_index=True)
        combined.to_parquet(out_path, index=False)
        size_kb = out_path.stat().st_size / 1024
        print(f"  {name}: {len(combined):,} rows  {size_kb:.0f} KB")

    # -----------------------------------------------------------------------
    # Lookup tables — skip if no months were processed (resume with nothing new)
    # -----------------------------------------------------------------------
    if not airport_meta and (LOOKUP_DIR / "airports.parquet").exists():
        print("\nNo new months processed — skipping lookup rebuild (existing files kept).")
        _print_summary()
        return

    # -----------------------------------------------------------------------
    # Lookup: airports
    # BTS lookup → airport name + city/state parsed from description
    # OurAirports → lat/lon enrichment
    # -----------------------------------------------------------------------
    print("\nBuilding airport lookup...")

    # BTS airport lookup: "ATL","Atlanta, GA: Hartsfield-Jackson Atlanta International"
    try:
        print(f"  Fetching BTS airport lookup...")
        bts_ap = pd.read_csv(BTS_AIRPORTS_URL, header=0, quotechar='"', encoding="latin-1")
        bts_ap.columns = ["iata_code", "description"]
        bts_ap["iata_code"] = bts_ap["iata_code"].str.strip()
        # Parse "City, ST: Airport Name" → city_state + airport_name
        split = bts_ap["description"].str.split(": ", n=1, expand=True)
        bts_ap["city_state"] = split[0].str.strip()
        bts_ap["airport_name"] = split[1].str.strip()
        # Parse state from "City, ST"
        bts_ap["state"] = bts_ap["city_state"].str.extract(r",\s*([A-Z]{2})$")
        bts_ap["city"] = (
            bts_ap["city_state"].str.replace(r",\s*[A-Z]{2}$", "", regex=True).str.strip()
        )
        bts_ap = bts_ap[["iata_code", "airport_name", "city", "state"]].drop_duplicates(
            "iata_code"
        )
        print(f"  BTS airports: {len(bts_ap)} entries")
    except Exception as e:
        print(f"  BTS airport fetch failed: {e}")
        bts_ap = pd.DataFrame(columns=["iata_code", "airport_name", "city", "state"])

    # Start from airports seen in the flight data, merge BTS names
    airports_df = pd.DataFrame(list(airport_meta.values()))
    # BTS lookup is more complete/authoritative for name+city — prefer it, fall back to flight data values
    airports_df = airports_df.merge(
        bts_ap[["iata_code", "airport_name"]], on="iata_code", how="left"
    )
    # Fill city/state from BTS if we got them, else keep from flight data
    airports_df = airports_df.merge(
        bts_ap[["iata_code", "city", "state"]].rename(
            columns={"city": "bts_city", "state": "bts_state"}
        ),
        on="iata_code",
        how="left",
    )
    airports_df["city"] = airports_df["bts_city"].fillna(airports_df["city"])
    airports_df["state"] = airports_df["bts_state"].fillna(airports_df["state"])
    airports_df = airports_df.drop(columns=["bts_city", "bts_state"], errors="ignore")

    # OurAirports for lat/lon and country
    try:
        print(f"  Fetching OurAirports for lat/lon...")
        oap = pd.read_csv(OURAIRPORTS_URL, low_memory=False)
        oap = oap[oap["iata_code"].notna() & (oap["iata_code"] != "")][
            ["iata_code", "latitude_deg", "longitude_deg", "iso_country"]
        ].rename(
            columns={
                "latitude_deg": "lat",
                "longitude_deg": "lon",
                "iso_country": "country",
            }
        )
        airports_df = airports_df.merge(oap, on="iata_code", how="left")
        print(
            f"  Lat/lon matched for {airports_df['lat'].notna().sum()} of {len(airports_df)} airports"
        )
    except Exception as e:
        print(f"  OurAirports fetch failed: {e} — continuing without lat/lon")

    out_path = LOOKUP_DIR / "airports.parquet"
    airports_df.to_parquet(out_path, index=False)
    size_kb = out_path.stat().st_size / 1024
    print(f"  airports: {len(airports_df)} rows  {size_kb:.0f} KB")

    # -----------------------------------------------------------------------
    # Lookup: states (state code → name, census region)
    # -----------------------------------------------------------------------
    states_df = pd.DataFrame(
        [
            {"state": code, "state_name": name, "region": region}
            for code, (name, region) in STATE_REGIONS.items()
        ]
    )
    out_path = LOOKUP_DIR / "states.parquet"
    states_df.to_parquet(out_path, index=False)
    print(f"  states: {len(states_df)} rows  {out_path.stat().st_size / 1024:.0f} KB")

    # -----------------------------------------------------------------------
    # Lookup: carriers (IATA code → full name from BTS carrier history)
    # Description format: "American Airlines (2013 - )" — strip the date range.
    # -----------------------------------------------------------------------
    print("\nBuilding carrier lookup...")
    active_carriers: set[str] = set()
    for chunk in acc.get("daily_carrier", []):
        if COL_CARRIER in chunk.columns:
            active_carriers.update(chunk[COL_CARRIER].unique())

    try:
        print(f"  Fetching BTS carrier lookup...")
        carr_raw = pd.read_csv(BTS_CARRIERS_URL, header=0, quotechar='"', encoding="latin-1")
        carr_raw.columns = ["iata_code", "carrier_name"]
        carr_raw["iata_code"] = carr_raw["iata_code"].str.strip()
        carr_raw["carrier_name"] = carr_raw["carrier_name"].str.strip()
        # Most recent entry per code (BTS lists historical entries oldest-first)
        bts_lookup = carr_raw.drop_duplicates("iata_code", keep="last").set_index("iata_code")[
            "carrier_name"
        ]
    except Exception as e:
        print(f"  BTS carrier fetch failed: {e} — using code as name for all carriers")
        bts_lookup = pd.Series(dtype=str)

    # Build a row for every carrier seen in the data; fall back to the code itself if not in BTS
    carriers_df = pd.DataFrame(
        [
            {"iata_code": code, "carrier_name": bts_lookup.get(code, code)}
            for code in sorted(active_carriers)
        ]
    )
    matched = carriers_df["carrier_name"].ne(carriers_df["iata_code"]).sum()
    print(f"  {matched} of {len(carriers_df)} carriers matched in BTS lookup")

    out_path = LOOKUP_DIR / "carriers.parquet"
    carriers_df.to_parquet(out_path, index=False)
    print(f"  carriers: {len(carriers_df)} rows  {out_path.stat().st_size / 1024:.0f} KB")

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------
    _print_summary()


def _print_summary() -> None:
    print("\n--- Final file sizes ---")
    for f in sorted(AGG_DIR.glob("*.parquet")) + sorted(LOOKUP_DIR.glob("*.parquet")):
        size_kb = f.stat().st_size / 1024
        rel = f.relative_to(ROOT / "data")
        print(f"  {str(rel):<45} {size_kb:>6.0f} KB")

    total_kb = (
        sum(
            f.stat().st_size
            for f in list(AGG_DIR.glob("*.parquet")) + list(LOOKUP_DIR.glob("*.parquet"))
        )
        / 1024
    )
    print(f"\n  Total: {total_kb:.0f} KB  ({total_kb / 1024:.1f} MB)")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build BTS airline delay parquet files.")
    parser.add_argument(
        "--start", default="2018-01", help="Start month YYYY-MM (default: 2018-01)"
    )
    parser.add_argument("--end", default="2025-01", help="End month YYYY-MM (default: 2025-01)")
    parser.add_argument(
        "--resume", default=False, action="store_true", help="Skip months already present in output"
    )
    args = parser.parse_args()

    build(args.start, args.end, args.resume)
