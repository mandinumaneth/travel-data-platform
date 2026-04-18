from __future__ import annotations

import random
from datetime import date
from pathlib import Path

import pandas as pd
from faker import Faker

fake = Faker()
random.seed(42)
Faker.seed(42)

OUTPUT_DIR = Path(__file__).resolve().parents[2] / "data" / "csv_samples"
COUNTRIES = [
    "United Kingdom",
    "United States",
    "France",
    "Spain",
    "Italy",
    "Australia",
    "Japan",
    "UAE",
    "India",
    "Singapore",
]


def build_broker_rows(total_brokers: int = 50) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    report_date = date.today().isoformat()

    for broker_id in range(1, total_brokers + 1):
        policies_sold_today = random.randint(1, 40)
        total_premium_value = round(random.uniform(2500, 80000), 2)
        commission_rate = round(random.uniform(0.05, 0.20), 4)
        commission_earned = round(total_premium_value * commission_rate, 2)

        rows.append(
            {
                "broker_id": broker_id,
                "broker_name": fake.name(),
                "broker_company": fake.company(),
                "country": random.choice(COUNTRIES),
                "policies_sold_today": policies_sold_today,
                "total_premium_value": total_premium_value,
                "commission_rate": commission_rate,
                "commission_earned": commission_earned,
                "report_date": report_date,
            }
        )

    return rows


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    rows = build_broker_rows(50)
    dataframe = pd.DataFrame(rows)

    file_name = f"broker_commissions_{date.today().strftime('%Y_%m_%d')}.csv"
    output_path = OUTPUT_DIR / file_name

    dataframe.to_csv(output_path, index=False)
    print(f"Generated broker commission file: {output_path}")
    print(f"Rows generated: {len(dataframe)}")


if __name__ == "__main__":
    main()
