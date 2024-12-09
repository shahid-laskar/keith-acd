import psycopg2
import re
import pandas as pd
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import os


# Set up logging
logging.basicConfig(
    filename="application.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def get_database_connection(db_url, retries=3):
    """
    Attempt to connect with retry logic to the database using the extracted db_url.
    Handles database name extraction correctly.
    """
    pattern = re.compile(r"postgres:postgres@localhost/(\w+)")
    match = pattern.search(db_url)

    if not match:
        logging.error(f"Invalid database URL: {db_url}")
        return None, None

    dbname = match.group(1)

    for attempt in range(retries):
        try:
            conn = psycopg2.connect(
                dbname=dbname,
                user="postgres",
                password="postgres",
                host="localhost",
                port=5432,
            )
            logging.info(f"Connected to database '{dbname}' successfully.")
            return conn, dbname
        except Exception as e:
            logging.error(
                f"Connection attempt {attempt + 1}/{retries} failed for '{dbname}': {e}"
            )
            time.sleep(2 ** attempt)

    logging.error(f"Failed to connect to database '{dbname}' after {retries} attempts.")
    return None, None

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

        # Generate the table name dynamically
        table_name = f"asterisk.{dbname}"

        # Create a temporary table if needed (you can modify this part based on schema requirements)
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join([f"{col} TEXT" for col in call_log_columns])}
            );
        """
        cursor.execute(create_table_query)

        # Insert data into the table
        insert_query = f"""
                    INSERT INTO {table_name} ({', '.join(call_log_columns)})
                    VALUES ({', '.join(['%s'] * len(call_log_columns))})
                    ON CONFLICT (callid) DO NOTHING;
                    """
        cursor.executemany(insert_query, call_log_data)
        conn.commit()

        logging.info(f"Data successfully pushed to table '{table_name}'")
        # Example transformation or processing here
        #df.to_csv("processed_call_log.csv", index=False)

        
        logging.info("Processed data saved to 'processed_call_log.csv'")
    except Exception as e:
        logging.error(f"Error processing query data: {e}")
    finally:
        cursor.close()
        conn.close()
   
       
        
    except Exception as e:
        logging.error(f"Error processing database '{dbname}': {e}")
    finally:
        cursor.close()
        conn.close()

def fetch_and_save_data(db_url, start_time, end_time):
    """
    Fetch data from the database, process and save it to a CSV with the database name.
    This function will be executed for each database URL.
    """
    conn, dbname = get_database_connection(db_url)
    if not conn:
        logging.error(f"Could not connect to database: {db_url}")
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

        # Generate the table name dynamically
        table_name = f"asterisk.{dbname}"

        # Create a temporary table if needed (you can modify this part based on schema requirements)
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join([f"{col} TEXT" for col in call_log_columns])}
            );
        """
        cursor.execute(create_table_query)

        # Insert data into the table
        insert_query = f"""
                    INSERT INTO {table_name} ({', '.join(call_log_columns)})
                    VALUES ({', '.join(['%s'] * len(call_log_columns))})
                    ON CONFLICT (callid) DO NOTHING;
                    """
        cursor.executemany(insert_query, call_log_data)
        conn.commit()

        logging.info(f"Data successfully pushed to table '{table_name}'")
       
        
    except Exception as e:
        logging.error(f"Error processing database '{dbname}': {e}")
    finally:
        cursor.close()
        conn.close()


def get_active_callcenter_db_urls():
    """
    Fetch database URLs from ADC database for processing.
    """
    try:
        conn = psycopg2.connect(
            "dbname=acd user=postgres password=postgres host=localhost port=5432"
        )
        cursor = conn.cursor()
        cursor.execute(
            "SELECT db_url FROM pbx WHERE state = 'active' AND group_cat = 'callcenter';"
        )
        db_urls = [row[0] for row in cursor.fetchall()]
        logging.info("Successfully retrieved database URLs.")
        cursor.close()
        conn.close()
        return db_urls
    except Exception as e:
        logging.error(f"Failed to fetch database URLs: {e}")
        return []


def main():
    """
    Use ThreadPoolExecutor to process all database URLs concurrently.
    """
    start_time = os.getenv("START_TIME", "2024-10-14 00:00:00")
    end_time = os.getenv("END_TIME", "2024-10-14 10:00:00")

    db_urls = get_active_callcenter_db_urls()

    if not db_urls:
        logging.error("No database URLs found.")
        return

    # Use ThreadPoolExecutor to process database URLs concurrently
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_db_url = {
            executor.submit(connect_and_process, db_url, start_time, end_time): db_url
            for db_url in db_urls
        }

        # Wait for all futures to complete
        for future in as_completed(future_to_db_url):
            try:
                future.result()
                logging.info(f"Processed database: {future_to_db_url[future]}")
            except Exception as e:
                logging.error(f"Error processing database '{future_to_db_url[future]}': {e}")


if __name__ == "__main__":
    main()
