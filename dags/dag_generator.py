from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime

"""
### Dynamic DAG Generation Based on Schedule Data

This DAG dynamically generates other DAGs based on event schedule data stored in an Airflow variable named `schedule_json`. Each entry in `schedule_json` represents a unique event with a defined schedule. The generated DAGs are assigned IDs based on the event names.

#### Features
- **Dynamic DAG Creation**: Each DAG is created dynamically based on entries in the `schedule_json` Airflow variable.
- **Event-Based Execution**: DAGs are named and executed according to specific events.
- **Flexible Scheduling**: Each event has a customizable `schedule_interval` and `start_date`.
- **Automatic Registration**: DAGs are registered with Airflow upon parsing `schedule_json`, without manual code changes.

#### Variables
- `schedule_json`: This Airflow variable should contain a JSON array of entries where each entry has:
  - `event` (str): The event name.
  - `date` (str): The event date in `YYYY-MM-DD` format.
  - `time` (str): The event time in `HH:MM` format.
  - `schedule_interval` (str): The interval at which the event should recur (e.g., `"@daily"` or `"0 12 * * *"` for noon daily).

#### Example
If `schedule_json` contains:
```json
[
  {
    "event": "Sample Event",
    "date": "2024-11-04",
    "time": "14:00",
    "schedule_interval": "@daily"
  }
]
"""

# Load the schedule data from Airflow variable named 'schedule_json'
schedule_json = Variable.get("schedule_json", deserialize_json=True)

# Function to parse the date and time into a datetime object
def parse_datetime(date_str, time_str):
    return datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")

# DAG generator function
def generate_dag(event_name, execution_date, schedule_interval):
    default_args = {
        "owner": "airflow",
        "start_date": execution_date,
        "retries": 0,
    }

    with DAG(
        dag_id=f"generated_{event_name.lower().replace(' ', '_')}_dag",
        default_args=default_args,
        schedule_interval=schedule_interval,  # Dynamic schedule interval
        catchup=False,
        is_paused_upon_creation=False,  # Ensures DAG is active upon creation
    ) as dag:
        # Define the task for this DAG
        task = PythonOperator(
            task_id="print_event_name",
            python_callable=lambda: print(f"Executing DAG for event: {event_name}"),
        )

    return dag

# Register the DAGs dynamically using the schedule data from 'schedule_json'
for entry in schedule_json:
    event_name = entry["event"]
    execution_date = parse_datetime(entry["date"], entry["time"])
    schedule_interval = entry["schedule_interval"]

    # Create and register the DAG with the "generated_" prefix
    globals()[f"generated_{event_name.lower().replace(' ', '_')}_dag"] = generate_dag(
        event_name, execution_date, schedule_interval
    )