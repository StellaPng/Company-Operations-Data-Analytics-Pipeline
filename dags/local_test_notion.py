import os
import requests
import json

# --- THIS IS A LOCAL TEST SCRIPT, NOT A REAL AIRFLOW DAG ---
# The goal is to test two things:
# 1. Can we successfully connect to the Notion API?
# 2. Can we read the data from our Test People Directory?

# --- 1. CONFIGURATION ---
# We will manually set our credentials for this local test.
# !! IMPORTANT: We do this ONLY for local testing.
# In production, Airflow will provide these secrets.

# TODO: Paste your Notion Secret Token here (starts with "secret_...")
NOTION_TOKEN = "YOUR_SECRET_TOKEN_HERE" 

# TODO: Get your Database ID
# 1. Open your "Test People Directory" in Notion in your browser.
# 2. Look at the URL. It will look like this:
#    https://www.notion.so/YOUR_WORKSPACE/DATABASE_ID?v=VIEW_ID
# 3. Copy the "DATABASE_ID" (a long string of letters/numbers)
#    and paste it here.
DATABASE_ID = "294842c43e8180da8bc0caf01bb615f3"


# --- 2. SETUP THE CONNECTION ---
# We set up the headers and the URL for the API request.
headers = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}

# This is the API endpoint to "query" a database
url = f"https://api.notion.com/v1/databases/{DATABASE_ID}/query"

# This is the "body" of our request. An empty {} query
# just means "get all pages" (up to the 100-page limit).
payload = {}


# --- 3. DEFINE THE MAIN FUNCTION ---
def read_notion_database():
    """Connects to Notion and fetches data."""
    print("Starting local test...")
    print(f"Attempting to connect to database ID: {DATABASE_ID}...")

    try:
        # This is where the script uses the 'requests' library
        # to send a POST request to the Notion API.
        response = requests.post(url, headers=headers, json=payload)
        
        # Check if the request was successful (e.g., status code 200)
        if response.status_code == 200:
            print("Successfully connected to Notion!")
            
            # Get the data from the response
            data = response.json()
            
            # Get the list of pages (rows) from the data
            results = data.get("results", [])
            
            print(f"Found {len(results)} rows in your database.")
            print("-" * 30)

            # Loop through each row and print the name
            for row in results:
                # Get the 'properties' object from the row
                properties = row.get("properties", {})
                
                # Get the 'Name' property
                name_property = properties.get("Name", {})
                
                # Get the 'title' list from the 'Name' property
                title_list = name_property.get("title", [])
                
                if title_list:
                    # Get the first item in the 'title' list
                    first_title_item = title_list[0]
                    # Get the 'plain_text' from that item
                    plain_text = first_title_item.get("plain_text", "No Name")
                    print(f"Found user: {plain_text}")

            print("-" * 30)
            print("Local test finished successfully.")

        else:
            # If the request failed (e.g., 401 Unauthorized, 404 Not Found)
            print(f"Error: Failed to connect.")
            print(f"Status Code: {response.status_code}")
            print(f"Error Message: {response.text}")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")


# --- 4. RUN THE FUNCTION ---
# This part makes the script runnable from the command line.
if __name__ == "__main__":
    read_notion_database()