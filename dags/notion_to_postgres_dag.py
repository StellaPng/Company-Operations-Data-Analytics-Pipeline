import json
import requests
from datetime import datetime

# Import the Airflow classes we need
from airflow.decorators import dag, task
from airflow.providers.http.hooks.http import HttpHook

# This is the connection ID we created in the Airflow UI
NOTION_CONNECTION_ID = "notion_conn"

# --- 1. A helper task to get our credentials ---
@task
def get_notion_credentials():
    """
    Pulls the Host, API Token, and Database ID from our
    secure Airflow Connection ('notion_conn').
    """
    print("Fetching credentials from Airflow connection...")
    
    # Use an HttpHook to safely get the connection details
    # This is the standard, secure way to access connections
    hook = HttpHook(method='POST', http_conn_id=NOTION_CONNECTION_ID)
    
    # The 'host' and 'password' (our token) are pulled from the
    # 'Extra (JSON)' field we configured.
    connection_data = hook.get_connection(NOTION_CONNECTION_ID)
    extras = connection_data.extra_dejson
    
    host = extras.get('host')
    token = extras.get('password')
    database_id = extras.get('database_id')

    if not all([host, token, database_id]):
        raise ValueError("Connection details (host, password, database_id) are missing!")

    print("Successfully retrieved credentials.")
    return {"host": host, "token": token, "database_id": database_id}


# --- 2. The main extraction task ---
@task
def extract_notion_data(credentials: dict):
    """
    Connects to the Notion API and fetches all data from the database.
    This version uses the 'requests' library, just like our test script.
    """
    print("Starting data extraction...")

    # Get the credentials passed from the previous task
    host = credentials['host']
    token = credentials['token']
    database_id = credentials['database_id']

    # Set up the headers and URL
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28",
    }

    query_url = f"{host}/v1/databases/{database_id}/query"
    payload = {} # Get all pages

    try:
        print(f"Querying database at: {query_url}...")

        # Use the requests library to make the API request
        response = requests.post(query_url, headers=headers, json=payload)

        # Check if the request was successful
        if response.status_code == 200:
            data = response.json()
            results = data.get("results", [])
            print(f"Successfully connected!")
            print(f"Found {len(results)} rows in your database.")

            # Here we just print the names like our test script
            for row in results:
                properties = row.get("properties", {})
                name_property = properties.get("Name", {})
                title_list = name_property.get("title", [])
                if title_list:
                    plain_text = title_list[0].get("plain_text", "No Name")
                    print(f"Found user: {plain_text}")

            print("Extraction task finished successfully.")
            return True # Indicate success

        else:
            # If the request failed (e.g., 401, 404)
            print(f"Error: Failed to connect.")
            print(f"Status Code: {response.status_code}")
            print(f"Error Message: {response.text}")
            raise Exception("Failed to extract data from Notion.")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise e



# --- 3. Define the DAG (The Pipeline) ---
@dag(
    dag_id="notion_to_postgres_pipeline",       # The name of our pipeline
    start_date=datetime(2025, 10, 1),      # When it starts
    schedule="@daily",            # How often to run it (once a day)
    catchup=False,                         # Don't run for past dates
    tags=["notion", "pipeline", "etl"],    # Tags for filtering in the UI
)
def notion_to_postgres():
    """
    A full ETL pipeline to extract data from Notion
    and (eventually) load it into PostgreSQL.
    """
    
    # Define our task dependencies
    # Task 1: Get credentials
    creds = get_notion_credentials()
    
    # Task 2: Extract data (depends on Task 1)
    # This means 'extract_notion_data' will only run
    # after 'get_notion_credentials' finishes successfully.
    extract_notion_data(creds)

# This line tells Airflow to create the pipeline
notion_to_postgres()