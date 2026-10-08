import csv
import psycopg2
from datetime import datetime


DB_CONFIG = {
    "host": "postgres_dev",
    "port": 5432,
    "user": "demo",
    "password": "demo",
    "dbname": "test",
}

FILES = [
    "/opt/airflow/dq_data/dailyActivity.csv",
    "/opt/airflow/dq_data/dailyActivity (1).csv",
]


def to_int(value):
    if value is None or value == "":
        return None
    return int(float(value))


def to_float(value):
    if value is None or value == "":
        return None
    return float(value)


def read_csv(path):
    rows = []

    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        for row in reader:
            rows.append((
                int(row["Id"]),
                datetime.strptime(row["ActivityDay"], "%Y-%m-%d").date(),
                to_float(row["Calories"]),
                to_int(row["minutes_instances"]),
                to_int(row["SedentaryMinutes"]),
                to_int(row["LightActiveMinutes"]),
                to_int(row["ModeratelyActiveMinutes"]),
                to_int(row["VeryActiveMinutes"]),
                to_int(row["Steps"]),
                to_int(row["TotalMinutesAsleep"]),
                to_int(row["HalfDream"]),
                to_int(row["MinutesInBed"]),
                to_int(row["SleepsInstances"]),
            ))

    return rows


def main():
    all_rows = []

    for path in FILES:
        rows = read_csv(path)
        print(f"{path}: {len(rows)} rows")
        all_rows.extend(rows)

    print(f"Total rows to load: {len(all_rows)}")

    conn = psycopg2.connect(**DB_CONFIG)

    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE activity_data")

            cur.executemany(
                """
                INSERT INTO activity_data (
                    id,
                    activity_day,
                    calories,
                    minutes_instances,
                    sedentary_minutes,
                    light_active_minutes,
                    moderately_active_minutes,
                    very_active_minutes,
                    steps,
                    total_minutes_asleep,
                    half_dream,
                    minutes_in_bed,
                    sleeps_instances
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                all_rows,
            )

        conn.commit()
        print("Data loaded successfully.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
