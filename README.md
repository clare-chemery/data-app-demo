# US Flight Delays

A Streamlit data app for exploring US domestic flight delay and cancellation trends using BTS Marketing Carrier On-Time Performance data (Jan 2018 – Jan 2025).

## Pages

- **Overview** — high-level summary stats: total flights, delay rate, cancellation rate
- **Airline Researcher** — historical delay analysis by carrier, cause, origin, and destination
- **Flight Planner** — personalised recommendations based on your location and historical airport/carrier performance

## Data

Raw monthly CSVs are downloaded from the BTS API and processed into aggregated Parquet files under `data/agg/`. Lookup tables (airports, carriers, states) live in `data/lookup/`. Run `scripts/build_data.py` to regenerate the aggregates.

## Running locally

```bash
uv sync
streamlit run streamlit_app.py
```

Requires Python 3.12+.
