# US Flight Delays

A Streamlit app for exploring **US domestic flight delays and cancellations** using **BTS Marketing Carrier On‑Time Performance** data (**Jan 2018 – Jan 2025**).

## What’s inside

- **Overview**: headline metrics (total flights, delay rate, cancellation rate)
- **Airline Researcher**: historical delay analysis by carrier, cause, origin, and destination
- **Flight Planner**: personalised recommendations based on your location and historical airport/carrier performance

## Data pipeline

- **Source**: BTS “PREZIP” downloads that back the public Download page  
  `https://transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FGK&QO_fu146_anzr=b0-gvzr`  
  (There’s no API for this dataset.)
- **Raw inputs**: monthly CSVs (not committed)
- **Processed outputs**:
  - Aggregates: `data/agg/` (Parquet)
  - Lookups (airports, carriers, states): `data/lookup/`
- **Rebuild**: run `scripts/build_data.py` to regenerate aggregates from raw CSVs

## Run locally

```bash
uv sync
streamlit run streamlit_app.py
```
Requires: Python 3.12+