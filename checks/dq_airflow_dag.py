from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


SODA_CONFIG = "/opt/airflow/soda/configuration.yml"
SODA_CHECKS = "/opt/airflow/soda/checks/dq_activity_checks.yml"


def run_soda():
    import json
    import os
    import psycopg2
    from soda.scan import Scan

    scan = Scan()
    scan.set_data_source_name("postgres_dev")
    scan.add_configuration_yaml_file(SODA_CONFIG)
    scan.add_sodacl_yaml_file(SODA_CHECKS)
    scan.execute()

    if scan.has_error_logs():
        raise Exception(
            f"Soda scan error:\n{scan.get_error_logs_text()}"
        )

    results = scan.get_scan_results()

    # Save full Soda result
    results_dir = "/opt/airflow/soda/results"
    os.makedirs(results_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = (
        f"{results_dir}/activity_scan_{timestamp}.json"
    )

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(
            results,
            f,
            indent=2,
            ensure_ascii=False,
            default=str,
        )

    # Save check results to PostgreSQL
    conn = psycopg2.connect(
        host="postgres_dev",
        port=5432,
        user="demo",
        password="demo",
        dbname="test",
    )

    try:
        with conn.cursor() as cursor:

            for check in results.get("checks", []):
                check_name = check.get("name")
                outcome = check.get("outcome")

                check_status = (
                    1 if outcome == "pass" else 0
                )

                diagnostics = check.get("diagnostics")

                result_value = None

                if isinstance(diagnostics, dict):
                    result_value = diagnostics.get("value")

                cursor.execute(
                    """
                    INSERT INTO dq_checks_log
                    (
                        table_name,
                        check_name,
                        check_status,
                        result_type,
                        result_value,
                        create_timestamp
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        "activity_data",
                        check_name,
                        check_status,
                        "soda",
                        str(result_value),
                        datetime.now(),
                    ),
                )

        conn.commit()

    finally:
        conn.close()

    print(f"Soda results saved to: {output_path}")


default_args = {
    "owner": "data_quality",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


with DAG(
    dag_id="dq_activity_pipeline",
    description="IoT activity data quality pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=[
        "data_quality",
        "activity",
        "soda",
    ],
) as dag:

    load_data = BashOperator(
        task_id="load_activity_data",
        bash_command=(
            "python /opt/airflow/dags/load_activity_data.py"
        ),
    )

    run_soda_checks = PythonOperator(
        task_id="run_soda_checks",
        python_callable=run_soda,
    )

    load_data >> run_soda_checks
