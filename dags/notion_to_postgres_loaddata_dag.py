import json
import requests
from datetime import datetime

# Import the Airflow classes we need
from airflow.decorators import dag, task

# NEW: Import the PostgresHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# This is the connection ID we created in the Airflow UI
NOTION_CONNECTION_ID = "notion_conn"
# This is the default connection ID for the PostgreSQL database
# that was created by our docker-compose.yaml file.
POSTGRES_CONNECTION_ID = "postgres_default"


# --- 1. Get Credentials Task ---
@task
def get_notion_credentials():
    """
    Pulls the Host, API Token, and Database ID from our
    secure Airflow Connection ('notion_conn').
    """
    print("Fetching credentials from Airflow connection...")
    
    # We use a trick here by importing the hook *inside* the task.
    # This avoids an import error if the provider isn't installed.
    from airflow.providers.http.hooks.http import HttpHook
    hook = HttpHook(method='POST', http_conn_id=NOTION_CONNECTION_ID)
    
    connection_data = hook.get_connection(NOTION_CONNECTION_ID)
    extras = connection_data.extra_dejson
    
    host = extras.get('host')
    token = extras.get('password')
    database_id = extras.get('database_id')

    if not all([host, token, database_id]):
        raise ValueError("Connection details (host, password, database_id) are missing!")

    print("Successfully retrieved credentials.")
    return {"host": host, "token": token, "database_id": database_id}


# --- 2. Extract Task (MODIFIED) ---
@task
def extract_notion_data(credentials: dict):
    """
    Connects to the Notion API and fetches all data.
    MODIFIED: This task now RETURNS the data.
    """
    print("Starting data extraction...")
    
    host = credentials['host']
    token = credentials['token']
    database_id = credentials['database_id']

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28",
    }
    
    query_url = f"{host}/v1/databases/{database_id}/query"
    payload = {} 

    try:
        print(f"Querying database at: {query_url}...")
        response = requests.post(query_url, headers=headers, json=payload)
        
        if response.status_code == 200:
            data = response.json()
            results = data.get("results", [])
            print(f"Successfully connected! Found {len(results)} rows.")
            
            # This is the big change: we return the data
            # so the next task can use it.
            return results
        
        else:
            print(f"Error: Failed to connect.")
            print(f"Status Code: {response.status_code}")
            print(f"Error Message: {response.text}")
            raise Exception("Failed to extract data from Notion.")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise e

# --- 3. Load Task (NEW) ---
@task
def load_data_to_postgres(notion_data: list):
    """
    Takes the extracted data and loads it into a PostgreSQL table.
    """
    print("Starting data load into PostgreSQL...")
    if not notion_data:
        print("No data to load. Skipping.")
        return

    # This hook connects to the 'postgres_default' connection
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONNECTION_ID)
    
    # SQL to create our table (if it doesn't exist)
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS notion_people (
        user_id VARCHAR(255) PRIMARY KEY,
        name VARCHAR(255),
        email VARCHAR(255),
        status VARCHAR(100),
        raw_data JSONB
    );
    """
    
    print("Ensuring 'notion_people' table exists...")
    pg_hook.run(create_table_sql)

    print(f"Loading {len(notion_data)} rows into table...")
    
    # Loop through each "row" from Notion
    for row in notion_data:
        user_id = row.get("id")
        properties = row.get("properties", {})
        
        # Extract Name
        name_data = properties.get("Name", {}).get("title", [])
        name = name_data[0].get("plain_text", None) if name_data else None

        # Extract Email
        email_data = properties.get("Email (Org)", {}).get("email", None)
        email = email_data

        # Extract Status
        status_data = properties.get("Mandate (Status)", {}).get("status", {})
        status = status_data.get("name", None)

        # We will use "INSERT ... ON CONFLICT" to avoid duplicate errors.
        # This will update the row if the user_id already exists.
        insert_sql = """
        INSERT INTO notion_people (user_id, name, email, status, raw_data)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (user_id) DO UPDATE SET
            name = EXCLUDED.name,
            email = EXCLUDED.email,
            status = EXCLUDED.status,
            raw_data = EXCLUDED.raw_data;
        """
        
        # Execute the insert statement for this row
        pg_hook.run(insert_sql, parameters=(
            user_id, 
            name, 
            email, 
            status, 
            json.dumps(row) # Store the full raw JSON
        ))

    print("Data load complete.")


# --- 4. Define the DAG (MODIFIED) ---
@dag(
    dag_id="notion_to_postgres_loaddata_dag",  # <-- Your new, correct dag_id
    start_date=datetime(2025, 10, 1),
    schedule="@daily",
    catchup=False,
    tags=["notion", "pipeline", "etl"],
)
def notion_to_postgres():
    """
    A full ETL pipeline to extract data from Notion
    and load it into PostgreSQL.
    """
    
    # Define our task dependencies
    creds = get_notion_credentials()
    
    # Task 2 depends on Task 1
    extracted_data = extract_notion_data(creds)
    
    # Task 3 (new) depends on Task 2
    load_data_to_postgres(extracted_data)

# This line tells Airflow to create the pipeline
notion_to_postgres()