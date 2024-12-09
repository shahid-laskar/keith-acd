import psycopg2
import re
import pandas as pd



def get_active_callcenter_db_urls():
    """
    Connect to the main 'adc' database, fetch all db_url from the pbx table
    where state='active' and group='callcenter'.
    """
    try:
        # Connect to the 'adc' database
        conn = psycopg2.connect("postgresql://postgres:postgres@localhost/acd")
        cursor = conn.cursor()
        
        # Fetch data from pbx table
        cursor.execute(
            "SELECT db_url FROM pbx WHERE state = 'active' AND group_cat = 'callcenter';"
        )
        
        # Retrieve all db_url values
        db_urls = [row[0] for row in cursor.fetchall()]
        
        # Close the connection to adc
        cursor.close()
        conn.close()
        
        return db_urls

    except Exception as e:
        print(f"Error connecting to the adc database: {e}")
        return []


def process_individual_db(url):
    """
    For each database URL, connect and run a custom operation
    Example: A connection is made, and data is fetched/processed as required.
    """
    # Regex to parse the db_url into user, password, host, and database
    pattern = re.compile(r"postgres:postgres@(.*?)/(\w+)")
    match = pattern.search(url)

    if not match:
        print(f"Skipping invalid URL format: {url}")
        return

    # Extract host and database name from the URL
    host = match.group(1)
    db_name = match.group(2)

    # Connection configuration
    db_config = {
        "dbname": db_name,
        "user": "postgres",
        "password": "postgres",  # Replace with your actual password
        "host": host,
        "port": 5432,
    }

    try:
        # Connect to the specific database
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # Perform a sample operation - Replace this with the script logic
        cursor.execute("SELECT NOW();")
        result = cursor.fetchone()
        
        print(f"Connected to {db_name} on {host}. Current time: {result[0]}")

        # Close the cursor and connection
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Failed to connect to {db_name} at {host}: {e}")

def connect_and_process(db_url, start_time, end_time):
    """
    Connects to the provided db_url, extracts the data, processes it, and saves to CSV.
    """
    # Connect to database
    try:
        conn = psycopg2.connect(db_url)
        cursor = conn.cursor()
        print("Connected to database.")
    except Exception as e:
        print(f"Failed to connect to database: {e}")
        return

    # Define SQL query with parameters
    select_query_queue_log_call_log = """
        WITH finished_call_ids AS (
            SELECT callid  
            FROM asterisk.queue_log 
            WHERE "q_event" IN ('EXITEMPTY', 'COMPLETEAGENT', 'COMPLETECALLER', 'ABANDON') 
                AND "time" BETWEEN %s AND %s
        )
        SELECT ql.* 
        FROM asterisk.queue_log  ql
        JOIN finished_call_ids fci ON ql.callid = fci.callid;
    """
    try:
        # Execute query safely
        cursor.execute(select_query_queue_log_call_log, (start_time, end_time))
        call_log_data = cursor.fetchall()
        call_log_columns = [desc[0] for desc in cursor.description]
        print("Data fetched successfully.")
    except Exception as e:
        print(f"Error executing query: {e}")
        cursor.close()
        conn.close()
        return
    finally:
        cursor.close()
        conn.close()

    # Convert fetched data into a DataFrame
    call_log = (
        pd.DataFrame(call_log_data, columns=call_log_columns)
        .assign(
            queuename=lambda x: x.q_name.astype('category'),
            agent=lambda x: x.q_agent.astype('category'),
            event=lambda x: x.q_event.astype('category')
        )
        .drop(columns=['q_name', 'q_agent', 'q_event'])
    )

    # Function to compute hold times
    def find_hold_time(group: pd.DataFrame):
        if not group.shape[0] > 2:
            return 0
        if group['event'].iloc[-2] == 'HOLD':
            group['event'].iloc[-1] = 'UNHOLD'
        return group.assign(
            duration=lambda x: pd.to_datetime(x['time']).diff()
        ).query('event == "UNHOLD"')['duration'].sum()

    # Process Data
    try:
        record_of_calls = (
            call_log.query("event == 'ENTERQUEUE'")[["callid", "time", "queuename", "data2"]]
            .rename(columns={"data2": "src", "time": "ENTERQUEUE"})
            .merge(
                call_log.query("event in ('ABANDON', 'EXITEMPTY')")[["callid", "time", "data3", "event"]]
                .assign(waited_duration_abandon=lambda x: x.data3)[
                    ["callid", "time", "waited_duration_abandon", "event"]
                ]
                .pivot_table(
                    index=["callid", "waited_duration_abandon"],
                    columns=["event"],
                    values="time",
                    observed="false",
                    aggfunc='first'
                )
                .reset_index(),
                on=["callid"],
                how="outer",
            )
            .merge(
                call_log.query("event == 'CONNECT'")[["callid", "time", "agent", "data1"]]
                .assign(waited_duration=lambda x: x.data1.astype(float))[["callid", "time", "agent", "waited_duration"]]
                .rename(columns={"time": "CONNECT"}),
                on=["callid"],
                how="outer",
            )
            .merge(
                call_log.query("event in ('COMPLETECALLER','COMPLETEAGENT')")[["callid", "time", "data2", "event"]]
                .assign(call_duration=lambda x: x.data2.astype(float), agent_completed=lambda x: x['event'] == 'COMPLETEAGENT')[["callid", "time", "call_duration", "agent_completed"]]
                .rename(columns={"time": "COMPLETE"}),
                on=["callid"],
                how="outer",
            )
            .merge(
                call_log[["time", "callid", "event"]]
                .query("event in ('HOLD','UNHOLD','COMPLETECALLER','COMPLETEAGENT')")
                .sort_values(["callid", "time"])
                .groupby("callid")
                .apply(find_hold_time, include_groups=False)
                .reset_index()
                .rename(columns={0: "hold_duration"}),
                on=["callid"],
                how="left",
            )
            .assign(
                waited_duration=lambda x: x.waited_duration.fillna(x.waited_duration_abandon),
                call_duration=lambda x: x.call_duration.fillna(0),
                hold_duration=lambda x: x.hold_duration.fillna(0)
            )[[
                "callid", "queuename", "src", "ENTERQUEUE", "ABANDON", "EXITEMPTY",
                "CONNECT", "COMPLETE", "agent", "waited_duration", "call_duration",
                "hold_duration", "agent_completed"
            ]].replace({pd.NaT: None})
            .dropna(subset=["queuename"])
        )
        # Save Data to CSV
        record_of_calls.to_csv('record_of_calls.csv', index=False)
        print("Processed data saved to 'record_of_calls.csv'")
    except Exception as e:
        print(f"Error processing data: {e}")


def main():
    """
    Main logic to process all db_urls from the ADC database.
    """
    # Step 1: Fetch all database URLs
    db_urls = get_active_callcenter_db_urls()
    start_time = '2024-10-14 00:00:00'
    end_time = '2024-11-02 00:00:00'
    
    if not db_urls:
        print("No database URLs found to process.")
        return

    # Step 2: Process each URL
    for url in db_urls:
        process_individual_db(url)
        print(f"Processing database URL: {url}")
        connect_and_process(url, start_time, end_time)


if __name__ == "__main__":
    main()








