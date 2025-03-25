import logging
from simple_salesforce import Salesforce
import psycopg2
import psycopg2.extras
import datetime
import calendar
import toml
import os

# Configure logging
log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'iolta_deposits_log.log')
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

try:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, 'config.toml')

    # Salesforce credentials
    with open(config_path, 'r') as file:
        config = toml.load(file)['database']

        db_config = {
            'user': config['DB_USER'],
            'password': config['DB_PASSWORD'],
            'host': config['DB_HOST'],
            'port': config['DB_PORT'],
            'database': config['DB_NAME'],
        }

    with open(config_path, 'r') as file:
        config = toml.load(file)['litify']
        
        username = config['LF_USER']
        password = config['LF_PASSWORD']
        security_token = config['LF_TOKEN']

    logging.info("Successfully loaded configurations.")

    sf = Salesforce(username=username, password=password, security_token=security_token)
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT 
        id, iolta_deposits_total, open_date
        FROM public.litify_matters_extracted
        WHERE open_date >= '2024-11-01'
    """)

    rows = cursor.fetchall()
    column_names = ['id', 'iolta_deposits_total', 'open_date']
    db_records = [dict(zip(column_names, row)) for row in rows]

    logging.info(f"Found {len(db_records)} Matter records in the database.")
    
    count = 0
    error_count = 0
    null_count = 0

    for row in db_records:
        try:
            open_date = row['open_date']
            id = row['id']
            last_iolta_day = datetime.date(
                open_date.year, open_date.month, calendar.monthrange(open_date.year, open_date.month)[1]
            )

            soql_query = f"""
                SELECT Matter__c, SUM(CmentorPay__Amount__c) paid_amount
                FROM CmentorPay__Payment_Integration_Transaction__c 
                WHERE CmentorPay__Payment_Date__c <= {last_iolta_day.strftime('%Y-%m-%d')}
                AND (CmentorPay__Status__c = 'Completed' OR CmentorPay__Status__c = 'Success') 
                AND lps_Invoice_Type__c = 'Matter Fee'
                AND Matter__c = '{row['id']}'
                GROUP BY Matter__c
            """

            sf_data = sf.query_all(soql_query)

            if sf_data['totalSize'] > 0:
                paid_amount = sf_data['records'][0]['paid_amount']
            else:
                paid_amount = 0
                null_count += 1

            upsert_query = """ 
                INSERT INTO public.litify_matters_extracted (
                    id,
                    iolta_deposits_total
                ) VALUES (
                    %s, %s
                )
                ON CONFLICT (id) DO UPDATE
                SET
                    iolta_deposits_total = EXCLUDED.iolta_deposits_total
            """
            
            try:
                cursor.execute(upsert_query, (id, paid_amount))
                count += 1
            except Exception as upsert_error:
                error_count += 1
                logging.error(f"Failed to upsert record with ID {id}: {upsert_error}")
                continue

        except Exception as query_error:
            error_count += 1
            logging.error(f"Failed to process record with ID {id}: {query_error}")
            continue

    logging.info(f"Script completed: {count} records updated, {null_count} records had null data, {error_count} errors encountered.")

    logging.info(f"{count} IOLTA records upserted.")
    logging.info(f"{null_count} null IOLTA records upserted (NO PAYMENTS FOUND).")

    # Commit and close
    conn.commit()
    logging.info("Database changes committed successfully.")

except Exception as e:
    logging.error(f"An error occurred: {e}")
finally:
    if 'cursor' in locals() and cursor:
        cursor.close()
        logging.info("Cursor closed.")
    if 'conn' in locals() and conn:
        conn.close()
        logging.info("Database connection closed.")
