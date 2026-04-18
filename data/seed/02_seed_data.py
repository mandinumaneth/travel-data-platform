from __future__ import annotations

import os
import random
from datetime import date, datetime, timedelta

import psycopg2
from dotenv import load_dotenv
from faker import Faker

load_dotenv()

fake = Faker()
random.seed(42)
Faker.seed(42)

COUNTRIES = [
    "United Kingdom",
    "United States",
    "France",
    "Germany",
    "Spain",
    "Italy",
    "Australia",
    "Canada",
    "India",
    "Japan",
    "Brazil",
    "Netherlands",
    "Sweden",
    "Norway",
    "UAE",
    "Singapore",
    "South Africa",
    "Thailand",
    "Mexico",
    "Ireland",
]

DESTINATIONS = [
    "Thailand",
    "Spain",
    "USA",
    "Japan",
    "Australia",
    "UAE",
    "France",
    "Italy",
    "Maldives",
    "UK",
]

COVERAGE_TYPES = ["medical", "cancellation", "baggage", "comprehensive"]
POLICY_STATUS = ["active", "expired", "cancelled"]
CLAIM_TYPES = ["medical", "cancellation", "baggage", "delay"]


def get_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "travel_db"),
        user=os.getenv("POSTGRES_USER", "travel_user"),
        password=os.getenv("POSTGRES_PASSWORD", "travel_password"),
    )


def reset_existing_data(cur: psycopg2.extensions.cursor) -> None:
    cur.execute("TRUNCATE TABLE claims, policies, customers RESTART IDENTITY CASCADE")
    print("Existing seed data cleared (claims, policies, customers).")


def random_dob() -> date:
    start = date(1955, 1, 1)
    end = date(2005, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))


def seed_customers(cur: psycopg2.extensions.cursor, total: int = 500) -> list[int]:
    print(f"Inserting {total} customers...")
    customer_ids: list[int] = []

    for idx in range(total):
        cur.execute(
            """
            INSERT INTO customers (
                first_name,
                last_name,
                email,
                date_of_birth,
                country_of_residence,
                phone,
                created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING customer_id
            """,
            (
                fake.first_name(),
                fake.last_name(),
                fake.unique.email(),
                random_dob(),
                random.choice(COUNTRIES),
                fake.phone_number()[:50],
                fake.date_time_between(start_date="-2y", end_date="now"),
            ),
        )
        customer_ids.append(cur.fetchone()[0])

        if (idx + 1) % 100 == 0:
            print(f"  Inserted {idx + 1}/{total} customers")

    return customer_ids


def seed_policies(
    cur: psycopg2.extensions.cursor,
    customer_ids: list[int],
    total: int = 1500,
) -> list[dict[str, object]]:
    print(f"Inserting {total} policies...")
    inserted_policies: list[dict[str, object]] = []

    for idx in range(total):
        customer_id = random.choice(customer_ids)
        trip_start = fake.date_between(start_date="-12m", end_date="+12m")
        trip_end = trip_start + timedelta(days=random.randint(2, 30))

        policy_number = f"POL-{datetime.utcnow().strftime('%Y%m')}-{idx + 1:06d}"
        premium_amount = round(random.uniform(50, 800), 2)
        status = random.choices(POLICY_STATUS, weights=[0.55, 0.35, 0.10], k=1)[0]

        created_at = fake.date_time_between(start_date="-18m", end_date="now")

        cur.execute(
            """
            INSERT INTO policies (
                customer_id,
                policy_number,
                destination_country,
                trip_start_date,
                trip_end_date,
                coverage_type,
                premium_amount,
                status,
                broker_id,
                created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING policy_id
            """,
            (
                customer_id,
                policy_number,
                random.choice(DESTINATIONS),
                trip_start,
                trip_end,
                random.choice(COVERAGE_TYPES),
                premium_amount,
                status,
                random.randint(1, 50),
                created_at,
            ),
        )

        inserted_policies.append(
            {
                "policy_id": cur.fetchone()[0],
                "created_at": created_at,
                "trip_start_date": trip_start,
            }
        )

        if (idx + 1) % 250 == 0:
            print(f"  Inserted {idx + 1}/{total} policies")

    return inserted_policies


def seed_claims(
    cur: psycopg2.extensions.cursor,
    policies: list[dict[str, object]],
    total: int = 800,
) -> None:
    print(f"Inserting {total} claims...")

    status_weights = [("approved", 0.60), ("rejected", 0.25), ("pending", 0.15)]
    labels, weights = zip(*status_weights)

    for idx in range(total):
        policy = random.choice(policies)
        policy_id = policy["policy_id"]
        claim_status = random.choices(labels, weights=weights, k=1)[0]
        claim_date = fake.date_between(start_date="-12m", end_date="today")

        processed_at = None
        if claim_status in {"approved", "rejected"}:
            processed_at = fake.date_time_between(start_date=claim_date, end_date="now")

        cur.execute(
            """
            INSERT INTO claims (
                policy_id,
                claim_type,
                claim_amount,
                claim_date,
                status,
                description,
                processed_at,
                created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                policy_id,
                random.choice(CLAIM_TYPES),
                round(random.uniform(35, 5000), 2),
                claim_date,
                claim_status,
                fake.sentence(nb_words=10),
                processed_at,
                fake.date_time_between(start_date="-12m", end_date="now"),
            ),
        )

        if (idx + 1) % 200 == 0:
            print(f"  Inserted {idx + 1}/{total} claims")


def main() -> None:
    conn = get_connection()

    try:
        with conn:
            with conn.cursor() as cur:
                should_reset = os.getenv("SEED_RESET", "true").lower() == "true"
                if should_reset:
                    reset_existing_data(cur)

                customer_ids = seed_customers(cur, 500)
                policies = seed_policies(cur, customer_ids, 1500)
                seed_claims(cur, policies, 800)

        print("Seeding complete.")
        print("Customers: 500")
        print("Policies: 1500")
        print("Claims: 800")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
