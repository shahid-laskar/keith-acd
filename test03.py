import psycopg2
from psycopg2 import extras
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
    Connect to the database using db_url with retry logic.
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

def find_hold_time(group: pd.DataFrame):
    """
    Compute total hold time for call groups.
    """
    if not group.shape[0] > 2:
        return 0
    if group["event"].iloc[-2] == "HOLD":
        group["event"].iloc[-1] = "UNHOLD"
    return (
        group.assign(duration=lambda x: pd.to_datetime(x["time"]).diff())
        .query("event == 'UNHOLD'")["duration"]
        .sum()
    )

def process_call_log(call_log: pd.DataFrame) -> pd.DataFrame:
    """
    Transform and process the call log DataFrame.
    """
    return (
        call_log.query("event == 'ENTERQUEUE'")[["callid", "time", "queuename", "data2"]]
        .rename(columns={"data2": "src", "time": "ENTERQUEUE"})
        .merge(
            call_log.query("event in ('ABANDON', 'EXITEMPTY')")[["callid", "time", "data3", "event"]]
            .assign(waited_duration_abandon=lambda x: x.data3.astype(float))
            .pivot_table(
                index=["callid"],
                columns=["event"],
                values="time",
                observed=False,
                aggfunc="first",
            )
            .reset_index(),
            on=["callid"],
            how="outer",
        )
        .merge(
            call_log.query("event == 'CONNECT'")[["callid", "time", "agent", "data1"]]
            .assign(waited_duration=lambda x: x.data1.astype(float))
            .rename(columns={"time": "CONNECT"}),
            on=["callid"],
            how="outer",
        )
        .merge(
            call_log.query("event in ('COMPLETECALLER','COMPLETEAGENT')")[["callid", "time", "data2", "event"]]
            .assign(call_duration=lambda x: x.data2.astype(float))
            .rename(columns={"time": "COMPLETE"}),
            on=["callid"],
            how="outer",
        )
        .merge(
            call_log[["time", "callid", "event"]]
            .query("event in ('HOLD', 'UNHOLD')")
            .groupby("callid")
            .apply(find_hold_time,include_groups=False)
            .reset_index()
            .rename(columns={0: "hold_duration"}),
            on=["callid"],
            how="left",
        )
        .fillna({"waited_duration": 0, "call_duration": 0, "hold_duration": 0})
    )
def connect_and_process(db_url, start_time, end_time):
    """
    Connect to a database, fetch data, and process it.
    """
    conn, dbname = get_database_connection(db_url)
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
        SELECT *
        FROM asterisk.queue_log
        WHERE callid IN (SELECT callid FROM finished_call_ids);
    """

    try:
        cursor.execute(query, (start_time, end_time))
        call_log_data = cursor.fetchall()
        call_log_columns = [desc[0] for desc in cursor.description]

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

        processed_data = process_call_log(call_log)
               


        table_name = f"asterisk.call_logs"

        # Create table if it doesn't exist
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                callid VARCHAR(50) PRIMARY KEY,
                queuename VARCHAR(20),
                src VARCHAR(50),
                ENTERQUEUE TIMESTAMP,
                ABANDON TIMESTAMP,
                EXITEMPTY TIMESTAMP,
                CONNECT TIMESTAMP,
                COMPLETE TIMESTAMP,
                agent VARCHAR(50),
                waited_duration FLOAT,
                call_duration FLOAT,
                hold_duration FLOAT
            );
        """
        cursor.execute(create_table_query)
        
        # Insert data
        insert_query = f"""
            INSERT INTO {table_name} VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (callid) DO NOTHING;
        """
        # Debugging the number of fields in data being inserted
        #for row in processed_data.values.tolist():
            #logging.info(f"Row being inserted: {row}, length: {len(row)}")

        psycopg2.extras.execute_batch(cursor, insert_query, processed_data.values.tolist())
        conn.commit()
        logging.info(f"Data successfully pushed to {table_name}")
    except Exception as e:
        logging.error(f"Error processing data for database {dbname}: {e}")
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
    Main function to process databases concurrently.
    """
    start_time = "2024-10-14 00:00:00"
    end_time = "2024-10-14 10:00:00"
    db_urls = get_active_callcenter_db_urls()

    if not db_urls:
        logging.error("No database URLs found.")
        return

    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_db_url = {
            executor.submit(connect_and_process, db_url, start_time, end_time): db_url
            for db_url in db_urls
        }

        for future in as_completed(future_to_db_url):
            try:
                future.result()
                logging.info(f"Processed: {future_to_db_url[future]}")
            except Exception as e:
                logging.error(f"Error: {e}")


if __name__ == "__main__":
    main()
