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

def push_data_to_db(df, db_conn):
    """
    Inserts data from DataFrame into the database table.
    """
    try:
        cursor = db_conn.cursor()

        # Ensure the table exists, create it if it doesn't
        create_table_query = """
        CREATE TABLE IF NOT EXISTS asterisk.call_logs (
            callid VARCHAR PRIMARY KEY,
            queuename VARCHAR,
            src VARCHAR,
            enterqueue TIMESTAMP,
            abandon TIMESTAMP,
            exitempty TIMESTAMP,
            connect TIMESTAMP,
            complete TIMESTAMP,
            agent VARCHAR,
            waited_duration FLOAT,
            call_duration FLOAT,
            hold_duration FLOAT,
            agent_completed BOOLEAN
        );
        """
        cursor.execute(create_table_query)
        logging.info("Ensured table exists in the database.")

        # Prepare the query to insert or upsert rows
        insert_query = """
        INSERT INTO asterisk.call_logs (
            callid, queuename, src, enterqueue, abandon, exitempty, 
            connect, complete, agent, waited_duration, call_duration, 
            hold_duration, agent_completed
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (callid) DO NOTHING;            
        """
        
        # Insert rows into the database
        for index, row in df.iterrows():
            cursor.execute(
                insert_query,
                (
                    row['callid'],
                    row['queuename'],
                    row['src'],
                    row['ENTERQUEUE'],
                    row['ABANDON'],
                    row['EXITEMPTY'],
                    row['CONNECT'],
                    row['COMPLETE'],
                    row['agent'],
                    row['waited_duration'],
                    row['call_duration'],
                    row['hold_duration'],
                    row['agent_completed'],
                )
            )

        # Commit changes
        db_conn.commit()
        logging.info("Inserted data into database successfully.")
    except Exception as e:
        logging.error(f"Failed to insert data: {e}")
        db_conn.rollback()
    #finally:
        #cursor.close()

def connect_and_process(db_url, start_time, end_time):
    """
    Process the database connection safely and fetch data safely.
    """
    conn,dbname = get_database_connection(db_url)
    if not conn:
        logging.error(f"Could not establish connection to database: {db_url}")
        return

    cursor = conn.cursor()

    try:
         # Step 1: Set flag=1 for rows to process
        update_flag_query_1 = """
        UPDATE asterisk.queue_log 
        SET flag = 1 
        WHERE flag = 0 AND "q_event" IN ('EXITEMPTY', 'COMPLETEAGENT', 'COMPLETECALLER', 'ABANDON') 
            AND "time" BETWEEN %s AND %s;
        """
        cursor.execute(update_flag_query_1, (start_time, end_time))
        conn.commit()
        logging.info("Rows marked as flag=1 for processing.")

        # Step 2: Fetch only rows with flag=1
        query = """
            WITH finished_call_ids AS (
                SELECT callid
                FROM asterisk.queue_log 
                WHERE flag = 1
            )
            SELECT ql.* 
            FROM asterisk.queue_log ql
            JOIN finished_call_ids fci ON ql.callid = fci.callid;
        """
         
        cursor.execute(query, (start_time, end_time))
        call_log_data = cursor.fetchall()
        call_log_columns = [desc[0] for desc in cursor.description]
        #logging.info(f"Database returned columns: {call_log_columns}")

        logging.info(f"Query executed and data fetched for time range {start_time} to {end_time}.")
        
        
    except Exception as e:
        logging.error(f"Error processing query data: {e}")
    
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
        try:
            if not group.shape[0] > 2:
                return 0
            if group['event'].iloc[-2] == 'HOLD':
                group['event'].iloc[-1] = 'UNHOLD'
            return group.assign(
                duration=lambda x: pd.to_datetime(x['time']).diff()
            ).query('event == "UNHOLD"')['duration'].sum()
        except Exception as e:
            logging.error(f"Error in hold time calculation: {e}")
            return 0

    # Process Data
    try:
        #logging.info("Unique events in call_log: %s", call_log['event'].unique())
        
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
                    aggfunc='first',
                    fill_value=pd.NaT  # Avoid missing values issues
                )
                .reset_index()
                .assign(
                    ABANDON=lambda x: x.get('ABANDON', pd.Series(pd.NaT, index=x.index)),
                    EXITEMPTY=lambda x: x.get('EXITEMPTY', pd.Series(pd.NaT, index=x.index))
                 ),                
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

        
        push_data_to_db(record_of_calls, conn)

       # Step 3: Set flag=2 after processing
        update_flag_query_2 = """
        UPDATE asterisk.queue_log 
        SET flag = 2 
        WHERE flag = 1;
        """
        cursor.execute(update_flag_query_2)
        conn.commit()
        logging.info("Rows updated to flag=2 after processing.")

    except Exception as e:
        print(f"Error processing data: {e}")
    finally:
        cursor.close()
        conn.close()
    
def main():
    """
    Main processing function. Fetch database URLs and process each in sequence.
    """
    start_time = os.getenv("START_TIME", "2024-10-16 00:00:00")
    end_time = os.getenv("END_TIME", "2024-10-16 19:55:00")

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
