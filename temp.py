def fetch_and_save_data(db_url, start_time, end_time):
    """
    Fetch data from the database and push it to the corresponding database table.
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
            VALUES ({', '.join(['%s'] * len(call_log_columns))});
        """
        cursor.executemany(insert_query, call_log_data)
        conn.commit()

        logging.info(f"Data successfully pushed to table '{table_name}'")
    except Exception as e:
        logging.error(f"Error processing database '{dbname}': {e}")
    finally:
        cursor.close()
        conn.close()
