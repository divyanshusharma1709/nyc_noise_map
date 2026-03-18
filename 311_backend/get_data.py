import os
from pathlib import Path

import pandas as pd
from sodapy import Socrata


def env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name, default)
    return value.strip() if isinstance(value, str) else value


def main():
    client = Socrata(
        env("SOCRATA_DOMAIN", "data.cityofnewyork.us"),
        env("SOCRATA_APP_TOKEN"),
        timeout=30,
    )
    limit = int(env("SOCRATA_LIMIT", "10000") or "10000")
    output_path = Path(env("SOCRATA_OUTPUT_PATH", "noise_data.csv"))
    where = env(
        "SOCRATA_WHERE",
        "complaint_type like 'Noise%' AND latitude IS NOT NULL AND longitude IS NOT NULL",
    )

    results = client.get(
        env("SOCRATA_DATASET", "erm2-nwe9"),
        limit=limit,
        order="created_date DESC",
        where=where,
    )

    results_df = pd.DataFrame.from_records(results)
    results_df.to_csv(output_path, index=False)
    print(f"Saved {len(results_df)} rows to {output_path}")


if __name__ == "__main__":
    main()
