from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime

# Load the schedule data from Airflow variable named 'schedule_json'
schedule_json = Variable.get("schedule_json", deserialize_json=True)

# Function to parse the date and time into a datetime object
def parse_datetime(date_str, time_str):
    return datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")

# DAG generator function
def generate_dag(event_name, execution_date):
    default_args = {
        "owner": "airflow",
        "start_date": execution_date,
        "retries": 0,
    }

    with DAG(
        dag_id=f"generated_{event_name.lower().replace(' ', '_')}_dag",
        default_args=default_args,
        schedule_interval=None,  # Ensures the DAG runs only once
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

    # Create and register the DAG with the "generated_" prefix
    globals()[f"generated_{event_name.lower().replace(' ', '_')}_dag"] = generate_dag(
        event_name, execution_date
    )