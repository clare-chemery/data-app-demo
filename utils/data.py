from pathlib import Path
import pandas as pd
import streamlit as st

DATA_DIR = Path(__file__).parent.parent / "data"
AGG_DIR  = DATA_DIR / "agg"
LKP_DIR  = DATA_DIR / "lookup"

FREQ_OPTIONS = ["Daily", "Weekly", "Monthly", "Quarterly", "Yearly"]
FREQ_MAP = {"Daily": "D", "Weekly": "W", "Monthly": "MS", "Quarterly": "QS", "Yearly": "YS"}

CAUSE_COLORS = {
    "Carrier":                   "#ef553b",
    "Weather":                   "#636efa",
    "NAS / Air Traffic Control": "#00cc96",
    "Security":                  "#ab63fa",
    "Late Aircraft":             "#ffa15a",
}

DOW_ORDER = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


@st.cache_data(show_spinner="Loading flight data...")
def load_all() -> dict:
    overall       = pd.read_parquet(AGG_DIR / "daily_overall.parquet")
    carrier_daily = pd.read_parquet(AGG_DIR / "daily_carrier.parquet")
    origin_daily_path = AGG_DIR / "daily_origin.parquet"
    origin_daily  = pd.read_parquet(origin_daily_path) if origin_daily_path.exists() else pd.DataFrame()
    cause         = pd.read_parquet(AGG_DIR / "monthly_cause.parquet")
    carrier_cause = pd.read_parquet(AGG_DIR / "monthly_carrier_cause.parquet")
    origin        = pd.read_parquet(AGG_DIR / "monthly_origin.parquet")
    dest          = pd.read_parquet(AGG_DIR / "monthly_dest.parquet")
    airports      = pd.read_parquet(LKP_DIR / "airports.parquet")
    states        = pd.read_parquet(LKP_DIR / "states.parquet")
    carriers      = pd.read_parquet(LKP_DIR / "carriers.parquet")

    od_path = AGG_DIR / "monthly_od.parquet"
    od = pd.read_parquet(od_path) if od_path.exists() else pd.DataFrame()

    overall["FlightDate"]       = pd.to_datetime(overall["FlightDate"])
    carrier_daily["FlightDate"] = pd.to_datetime(carrier_daily["FlightDate"])
    if not origin_daily.empty:
        origin_daily["FlightDate"] = pd.to_datetime(origin_daily["FlightDate"])

    carrier_daily = carrier_daily.merge(
        carriers.rename(columns={"iata_code": "Operating_Airline"}), how="left"
    )

    airports = airports.drop(columns=["state_name"], errors="ignore")
    airports = airports.merge(states[["state", "state_name", "region"]], on="state", how="left")

    return {
        "overall":       overall,
        "carrier_daily": carrier_daily,
        "origin_daily":  origin_daily,
        "cause":         cause,
        "carrier_cause": carrier_cause,
        "origin":        origin,
        "dest":          dest,
        "od":            od,
        "airports":      airports,
        "states":        states,
        "carriers":      carriers,
    }


def _sum_cols(df: pd.DataFrame, exclude: list[str]) -> list[str]:
    return [c for c in df.columns if c not in exclude and pd.api.types.is_numeric_dtype(df[c])]


def resample_overall(df: pd.DataFrame, freq: str) -> pd.DataFrame:
    cols = _sum_cols(df, ["FlightDate"])
    r = df.set_index("FlightDate").resample(freq)[cols].sum().reset_index()
    r["dep_delay_rate"] = r["dep_delayed"] / r["total_flights"]
    r["arr_delay_rate"] = r["arr_delayed"] / r["total_flights"]
    r["cancel_rate"]    = r["cancelled"]   / r["total_flights"]
    return r


def resample_carrier(df: pd.DataFrame, freq: str) -> pd.DataFrame:
    group = ["Operating_Airline"]
    if "carrier_name" in df.columns:
        group.append("carrier_name")
    num_cols = _sum_cols(df, ["FlightDate"] + group)
    r = (
        df.set_index("FlightDate")
        .groupby([pd.Grouper(freq=freq)] + group)[num_cols]
        .sum()
        .reset_index()
        .rename(columns={"FlightDate": "period"})
    )
    r["dep_delay_rate"] = r["dep_delayed"] / r["total_flights"]
    r["cancel_rate"]    = r["cancelled"]   / r["total_flights"]
    return r


def dow_stats(df: pd.DataFrame) -> pd.DataFrame:
    r = (
        df.assign(dow=df["FlightDate"].dt.day_name())
        .groupby("dow")[["total_flights", "dep_delayed", "cancelled"]]
        .sum()
        .assign(
            dep_delay_rate=lambda d: d.dep_delayed / d.total_flights,
            cancel_rate   =lambda d: d.cancelled   / d.total_flights,
        )
        .reindex(DOW_ORDER)
        .reset_index()
    )
    return r


def dow_stats_airport(origin_daily: pd.DataFrame, airport: str) -> pd.DataFrame:
    ap = origin_daily[origin_daily["Origin"] == airport]
    return dow_stats(ap) if not ap.empty else pd.DataFrame()


def filter_daily(df: pd.DataFrame, start, end) -> pd.DataFrame:
    s, e = pd.Timestamp(start), pd.Timestamp(end)
    return df[(df["FlightDate"] >= s) & (df["FlightDate"] <= e)]


def filter_monthly(df: pd.DataFrame, start, end) -> pd.DataFrame:
    s, e = pd.Timestamp(start), pd.Timestamp(end)
    date_col = df["Year"].astype(str) + "-" + df["Month"].astype(str).str.zfill(2)
    dates = pd.to_datetime(date_col, format="%Y-%m")
    return df[(dates >= s) & (dates <= e)]
