import psycopg2
import re



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


def main():
    """
    Main logic to process all db_urls from the ADC database.
    """
    # Step 1: Fetch all database URLs
    db_urls = get_active_callcenter_db_urls()
    
    if not db_urls:
        print("No database URLs found to process.")
        return

    # Step 2: Process each URL
    for url in db_urls:
        process_individual_db(url)
        print(f"Processing database URL: {url}")


if __name__ == "__main__":
    main()
