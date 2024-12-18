from prefect import flow, task
from datetime import timedelta, datetime
from sqlalchemy import create_engine
import pandas as pd
import requests

# Replace with your current postgres credentials
USERNAME = ''
PASSWORD = ''
HOST = ''
PORT = ''
DATABASE = ''

# Create PostgreSQL engine
def create_postgres_engine(username, password, host, port, database):
    connection_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"
    return create_engine(connection_string)

# Insert data into postgres
def insert_data_to_postgresql(engine, df, table_name):
    if not df.empty:
        try:
            df.to_sql(table_name, engine, if_exists="append", index=False)  # Append data to avoid overwriting
            print(f"Data inserted into {table_name} successfully!")
        except Exception as e:
            print(f"Error inserting data into {table_name}: {e}")
    else:
        print(f"No data to insert into {table_name}.")
        
# if interested in pulling the data, visit the EPA website for your own API key and insert here
API_KEY = ''

@task(retries=3, retry_delay_seconds=60)
def fetch_streaming_data():
    emissions_streaming_data = pd.DataFrame()
    today = datetime.today()

    # Determine the most recent month in the current quarter, modify as needed if wanting to pull full quarter
    if today.month in [1, 2, 3]:
        month_start = datetime(today.year - 1, 12, 1)
        month_end = datetime(today.year - 1, 12, 31)
    elif today.month in [4, 5, 6]:
        month_start = datetime(today.year, 3, 1)
        month_end = datetime(today.year, 3, 31)
    elif today.month in [7, 8, 9]:
        month_start = datetime(today.year, 6, 1)
        month_end = datetime(today.year, 6, 30)
    else:
        month_start = datetime(today.year, 9, 1)
        month_end = datetime(today.year, 9, 30)

    print(f"Fetching data from {month_start.strftime('%Y-%m-%d')} to {month_end.strftime('%Y-%m-%d')}.")
    streaming_url = "https://api.epa.gov/easey/streaming-services/emissions/apportioned/hourly"
    current_date = month_start
    
# pull data only when unit is operating (no emissions data otherwise) for most recent month in quarter, adjust code as needed to fit your interests. . 
    while current_date <= month_end:
        parameters_streaming = {
            'api_key': API_KEY,
            'beginDate': current_date.strftime("%Y-%m-%d"),
            'endDate': current_date.strftime("%Y-%m-%d"),
            'operatingHoursOnly': "true"
        }

        print(f"Fetching data for {parameters_streaming['beginDate']}...")

        try:
            streaming_response = requests.get(streaming_url, params=parameters_streaming)
            streaming_response.raise_for_status()
            daily_streaming_data = streaming_response.json()
            if daily_streaming_data:
                daily_df = pd.DataFrame(daily_streaming_data)
                emissions_streaming_data = pd.concat([emissions_streaming_data, daily_df], ignore_index=True)
            else:
                print(f"No data for {parameters_streaming['beginDate']}.")
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {parameters_streaming['beginDate']}: {e}")
        current_date += timedelta(days=1)

    return emissions_streaming_data

@task
def upload_to_postgres(data: pd.DataFrame):
    if data.empty:
        print("No data to upload to PostgreSQL.")
        return

    engine = create_postgres_engine(USERNAME, PASSWORD, HOST, PORT, DATABASE)
    insert_data_to_postgresql(engine, data, "streaming_emissions")


@flow(name="Quarterly Data Pull")
def quarterly_data_pull():
    data = fetch_streaming_data()
    upload_to_postgres(data)

if __name__ == "__main__":
    quarterly_data_pull.serve(name="Quarterly Data Pull Deployment", cron="0 0 */90 * *")


