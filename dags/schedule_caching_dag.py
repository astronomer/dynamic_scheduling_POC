import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

# Cleaned-up schedule.json content
SCHEDULE_DATA = [
  {
    "date": "2024-10-21",
    "time": "13:50",
    "event": "Crude Oil Inventories",
    "schedule_interval": "0 13 * * MON"
  },
  {
    "date": "2024-10-18",
    "time": "14:00",
    "event": "FOMC Minutes",
    "schedule_interval": "0 14 * * FRI"
  },
  {
    "date": "2024-10-19",
    "time": "08:30",
    "event": "Retail Sales",
    "schedule_interval": "0 8 * * SAT"
  },
  {
    "date": "2024-10-20",
    "time": "10:00",
    "event": "Building Permits",
    "schedule_interval": "@once"
  },
  {
    "date": "2024-10-21",
    "time": "09:30",
    "event": "Existing Home Sales",
    "schedule_interval": "30 9 * * MON"
  },
  {
    "date": "2024-10-22",
    "time": "11:00",
    "event": "New Home Sales",
    "schedule_interval": "@daily"
  },
  {
    "date": "2024-10-23",
    "time": "08:00",
    "event": "Jobless Claims",
    "schedule_interval": "0 8 * * THU"
  },
  {
    "date": "2024-10-24",
    "time": "09:00",
    "event": "GDP Growth Rate",
    "schedule_interval": "@weekly"
  }
]

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
}

# Function to cache schedule data into an Airflow variable
def cache_schedule():
    Variable.set("schedule_json", json.dumps(SCHEDULE_DATA))

# Define the DAG
with DAG(
    "cache_schedule_to_variable",
    default_args=default_args,
    schedule_interval=None,  # Trigger manually as needed
    catchup=False,
    tags=["cache", "schedule"],
) as dag:

    # Task to cache the schedule data into Airflow variable
    cache_schedule_task = PythonOperator(
        task_id="cache_schedule_task",
        python_callable=cache_schedule,
    )