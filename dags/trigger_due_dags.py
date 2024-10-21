import json
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
import re

# Helper function to generate valid DAG IDs
def sanitize_dag_id(event_name):
    """Sanitize the event name to create a valid DAG ID."""
    return re.sub(r"[^\w\-\.]", "_", event_name.lower())

# Function to load the schedule from the Airflow variable
def load_schedule():
    """Load schedule from the Airflow variable."""
    schedule_json = Variable.get("schedule_json")
    return json.loads(schedule_json)

# Updated filter_due_events function
def filter_due_events():
    """Filter events that are scheduled to run at the current time."""
    schedule = load_schedule()
    now = pendulum.now()  # Get the current timestamp

    # Align both timestamps to the start of the minute for comparison
    due_events = [
        event for event in schedule
        if pendulum.parse(f"{event['date']} {event['time']}").start_of('minute') == now.start_of('minute')
    ]
    return due_events

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
}

# Define the dynamic event trigger DAG
with DAG(
    "dynamic_event_trigger_dag",
    default_args=default_args,
    schedule_interval="* * * * *",
    catchup=False,
    tags=["dynamic", "event", "trigger"],
) as dag:

    # Task to fetch and filter due events
    def get_due_events(**context):
        """Fetch due events to be triggered."""
        return filter_due_events()

    fetch_due_events = PythonOperator(
        task_id="fetch_due_events",
        python_callable=get_due_events,
        provide_context=True,
    )

    # Task group to dynamically map TriggerDagRunOperator tasks
    with TaskGroup(group_id="trigger_dags") as trigger_group:

        def create_trigger_task(event):
            """Create a TriggerDagRunOperator for each event."""
            # Generate valid child DAG ID from the event name
            dag_id = sanitize_dag_id(event["event"])

            # Create a TriggerDagRunOperator for the child DAG
            return TriggerDagRunOperator(
                task_id=f"trigger_{dag_id}",
                trigger_dag_id=f"child_dags.{dag_id}",  # Reference child DAG by path
                reset_dag_run=True,
                wait_for_completion=False,
            )

        # Map TriggerDagRunOperator dynamically to each fetched event
        trigger_tasks = fetch_due_events.output.map(create_trigger_task)

    # Set task dependencies
    fetch_due_events >> trigger_group