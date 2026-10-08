#!/usr/bin/env python
# coding: utf-8

# In[2]:


from pathlib import Path
import logging

import pandas as pd


# Configuration

BASE_DIR = Path(__file__).resolve().parent.parent

DATA_DIR = BASE_DIR / "data"

FILES = [
    DATA_DIR / "dailyActivity.csv.csv",
    DATA_DIR / "dailyActivity (1).csv",
]

LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

RESULTS_DIR = BASE_DIR / "results"
RESULTS_DIR.mkdir(exist_ok=True)

RESULT_FILE = RESULTS_DIR / "dq_results.csv"
LOG_FILE = LOG_DIR / "dq_pipeline.log"


# Logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


# Data loading

def load_data():
    """Load and combine source files."""

    frames = []

    for file_path in FILES:
        logger.info("Loading file: %s", file_path)

        df = pd.read_csv(file_path)

        # Standardize column names
        df = df.rename(columns={
            "Id": "id",
            "ActivityDay": "activity_day",
            "Calories": "calories",
            "minutes_instances": "minutes_instances",
            "SedentaryMinutes": "sedentary_minutes",
            "LightActiveMinutes": "light_active_minutes",
            "ModeratelyActiveMinutes": "moderately_active_minutes",
            "VeryActiveMinutes": "very_active_minutes",
            "Steps": "steps",
            "TotalMinutesAsleep": "total_minutes_asleep",
            "HalfDream": "half_dream",
            "MinutesInBed": "minutes_in_bed",
            "SleepsInstances": "sleeps_instances",
        })

        frames.append(df)

    data = pd.concat(frames, ignore_index=True)

    logger.info("Total records loaded: %s", len(data))

    return data


# DQ checks

def run_checks(df):
    """Run all registered DQ rules."""

    results = []

    def add_result(rule_id, dimension, violations, severity):
        total = len(df)

        pass_rate = (
            (1 - violations / total) * 100
            if total > 0
            else 0
        )

        results.append({
            "rule_id": rule_id,
            "dimension": dimension,
            "severity": severity,
            "violations": int(violations),
            "total": int(total),
            "pass_rate": round(pass_rate, 2),
        })

    # DQ-01

    violations = df["id"].isna().sum()

    add_result(
        "DQ-01",
        "Completeness",
        violations,
        "Critical"
    )

    # DQ-02
    
    activity_day = pd.to_datetime(
        df["activity_day"],
        errors="coerce"
    )

    violations = activity_day.isna().sum()

    add_result(
        "DQ-02",
        "Validity",
        violations,
        "Critical"
    )

    # DQ-03
    
    violations = df.duplicated(
        ["id", "activity_day"],
        keep=False
    ).sum()

    add_result(
        "DQ-03",
        "Uniqueness",
        violations,
        "Critical"
    )

    # DQ-04
    
    numeric_columns = [
        "calories",
        "minutes_instances",
        "sedentary_minutes",
        "light_active_minutes",
        "moderately_active_minutes",
        "very_active_minutes",
        "steps",
        "total_minutes_asleep",
        "half_dream",
        "minutes_in_bed",
        "sleeps_instances",
    ]

    violations = (
        df[numeric_columns]
        .lt(0)
        .any(axis=1)
        .sum()
    )

    add_result(
        "DQ-04",
        "Validity",
        violations,
        "Critical"
    )

    # DQ-05
    
    activity_sum = (
        df["sedentary_minutes"]
        + df["light_active_minutes"]
        + df["moderately_active_minutes"]
        + df["very_active_minutes"]
    )

    violations = (
        activity_sum != df["minutes_instances"]
    ).sum()

    add_result(
        "DQ-05",
        "Consistency",
        violations,
        "Critical"
    )

    # DQ-06
    
    violations = (
        df["minutes_instances"] > 1440
    ).sum()

    add_result(
        "DQ-06",
        "Validity",
        violations,
        "Critical"
    )

    # DQ-07
    
    violations = (
        df["minutes_instances"] < 1440
    ).sum()

    add_result(
        "DQ-07",
        "Completeness",
        violations,
        "Warning"
    )

    # DQ-08
    
    violations = (
        (df["sleeps_instances"] > 0)
        & (df["total_minutes_asleep"] == 0)
    ).sum()

    add_result(
        "DQ-08",
        "Consistency",
        violations,
        "Warning"
    )

    # DQ-09
    
    violations = (
        (df["total_minutes_asleep"] > 0)
        & (df["half_dream"] == 0)
    ).sum()

    add_result(
        "DQ-09",
        "Completeness",
        violations,
        "Warning"
    )

    return pd.DataFrame(results)


# Pipeline

def main():

    logger.info("Starting DQ pipeline")

    try:

        # 1. Load data
        df = load_data()

        # 2. Run DQ checks
        results = run_checks(df)

        # 3. Add overall status
        results["status"] = results.apply(
            lambda row:
                "FAIL"
                if row["violations"] > 0
                and row["severity"] == "Critical"
                else (
                    "WARNING"
                    if row["violations"] > 0
                    else "PASS"
                ),
            axis=1
        )

        # 4. Save results
        results.to_csv(
            RESULT_FILE,
            index=False,
            encoding="utf-8"
        )

        logger.info(
            "DQ results saved to: %s",
            RESULT_FILE
        )

        # 5. Console summary
        print("\nDQ SUMMARY")
        print("=" * 70)
        print(results.to_string(index=False))

        logger.info("DQ pipeline completed successfully")

    except Exception:
        logger.exception("DQ pipeline failed")
        raise


if __name__ == "__main__":
    main()

