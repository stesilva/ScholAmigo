import psycopg2
import traceback
from datetime import datetime

PG_CONFIG = {
    'dbname': 'kafka',
    'user': 'kafka',
    'password': 'kafka_password',
    'host': 'postgres_kafka_consumer',
    'port': '5432'
}

TABLES = ["search_table", "filter_table", "details_table", "faq_table"]

def clean_old_clickstream():
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cursor = conn.cursor()

        for table in TABLES:
            cursor.execute(f"DELETE FROM {table} WHERE timestamp < NOW() - INTERVAL '1 day';")
            print(f"Old data deleted from {table}")

        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        print("Error during cleanup:", e)
        traceback.print_exc()
        raise

if __name__ == "__main__":
    clean_old_clickstream()
