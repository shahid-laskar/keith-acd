import psycopg2
import re
import pandas as pd
import logging
import time
import os


# Set up logging
logging.basicConfig(
    filename="application.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


# Retry connection logic with retries
def get_database_connection(db_url, retries=3):
    """
    Attempt to connect with retry logic to the database using the extracted db_url.
    Handles database name extraction correctly.
    """
    # Extract dbname
    pattern = re.compile(r"postgres:postgres@(.*?)/(\w+)")
    #pattern = re.compile(r"postgres:postgres@localhost/(\w+)")
    match = pattern.search(db_url)

    if not match:
        logging.error(f"Invalid database URL: {db_url}")
        return None

    host = match.group(1)
    dbname = match.group(2)

    # Retry logic with retries
    for attempt in range(retries):
        try:
            # Attempt connection
            conn = psycopg2.connect(
                dbname=dbname,
                user="postgres",
                password="postgres",
                host=host,
                port=5432,
            )
            logging.info(f"Successfully connected to database '{dbname}'")
            return conn, dbname
        except Exception as e:
            logging.error(
                f"Connection attempt {attempt + 1}/{retries} failed: {e}. Retrying..."
            )
            time.sleep(2 ** attempt)  # Exponential backoff
    logging.error(f"Failed to connect to database '{dbname}' after {retries} attempts.")
    return None


def get_active_callcenter_db_urls():
    """
    Connect to the main 'adc' database, fetch all db_url from the pbx table
    where state='active' and group='callcenter'.
    """
    try:
        # Connect to the ADC database
        conn = psycopg2.connect("dbname=acd user=postgres password=postgres host=localhost port=5432")
        cursor = conn.cursor()
        
        # Fetch all db URLs
        cursor.execute(
            "SELECT db_url FROM pbx WHERE state = 'active' AND group_cat = 'callcenter';"
        )
        
        db_urls = [row[0] for row in cursor.fetchall()]
        
        logging.info("Successfully retrieved database URLs.")
        
        # Close connection
        cursor.close()
        conn.close()
        
        return db_urls

    except Exception as e:
        logging.error(f"Failed to fetch database URLs: {e}")
        return []


def connect_and_process(db_url, start_time, end_time):
    """
    Process the database connection safely and fetch data safely.
    """
    conn,dbname = get_database_connection(db_url)
    if not conn:
        logging.error(f"Could not establish connection to database: {db_url}")
        return

    cursor = conn.cursor()
    
    query = """
        WITH finished_call_ids AS (
            SELECT callid  
            FROM asterisk.queue_log 
            WHERE "q_event" IN ('EXITEMPTY', 'COMPLETEAGENT', 'COMPLETECALLER', 'ABANDON') 
                AND "time" BETWEEN %s AND %s
        )
        SELECT ql.* 
        FROM asterisk.queue_log ql
        JOIN finished_call_ids fci ON ql.callid = fci.callid;
    """
    
    try:
        cursor.execute(query, (start_time, end_time))
        call_log_data = cursor.fetchall()
        call_log_columns = [desc[0] for desc in cursor.description]

        logging.info(f"Query executed and data fetched for time range {start_time} to {end_time}.")
        
        # Data transformation for logging
        #df = pd.DataFrame(call_log_data, columns=call_log_columns)

        # Example transformation or processing here
        #df.to_csv("processed_call_log.csv", index=False)

        
        logging.info("Processed data saved to 'processed_call_log.csv'")
    except Exception as e:
        logging.error(f"Error processing query data: {e}")
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
        filename=f"{dbname}.csv"
        record_of_calls.to_csv(filename, index=False)
        print(f"Processed data saved to {filename}")
    except Exception as e:
        print(f"Error processing data: {e}")

def main():
    """
    Main processing function. Fetch database URLs and process each in sequence.
    """
    start_time = os.getenv("START_TIME", "2024-10-14 00:00:00")
    end_time = os.getenv("END_TIME", "2024-11-02 00:00:00")

    db_urls = get_active_callcenter_db_urls()
    
    if not db_urls:
        logging.error("No database URLs found.")
        return

    for db_url in db_urls:
        logging.info(f"Processing database URL: {db_url}")
        connect_and_process(db_url, start_time, end_time)
        logging.info(f"Processing completed for database URL: {db_url}")


if __name__ == "__main__":
    main()
