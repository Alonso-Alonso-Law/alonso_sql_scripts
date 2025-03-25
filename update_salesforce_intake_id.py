import logging
from simple_salesforce import Salesforce
import psycopg2
import psycopg2.extras
import datetime
import calendar
import toml
import os

# Configure logging
log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'salesforce_id_fix.log')
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
        id, salesforce_intake_id, salesforce_account_id
        FROM intake.lucky_13_submission ls 
        WHERE form_filled_date between '2025-01-01 05:56:58.541' and '2025-01-31 05:56:58.541'
    """)

    rows = cursor.fetchall()
    column_names = ['id', 'salesforce_intake_id', 'salesforce_account_id']
    db_records = [dict(zip(column_names, row)) for row in rows]

    logging.info(f"Found {len(db_records)} intake records in the database.")
    
    count = 0
    error_count = 0
    null_count = 0

    for row in db_records:
        try:
            id = row['id']

            soql_query = f"""
                select jotforms_id__c, Intake__c, Intake__r.litify_pm__Client__c
                from Qualification_intake__c 
                where CreatedDate >= 2024-07-28T09:41:26.952-06:00 
                and Intake__c != null and jotforms_id__c != null
                and jotforms_id__c = '{row['id']}'
            """

            sf_data = sf.query_all(soql_query)

            if sf_data['totalSize'] > 0:
                salesforce_intake_id = sf_data['records'][0]['Intake__c']
                salesforce_account_id = sf_data['records'][0]['Intake__r']['litify_pm__Client__c']
            else:
                null_count += 1

            upsert_query = """ 
                INSERT INTO intake.lucky_13_submission (
                    id, salesforce_intake_id, salesforce_account_id
                ) VALUES (
                    %s, %s,%s
                )
                ON CONFLICT (id) DO UPDATE
                SET
                    salesforce_intake_id = EXCLUDED.salesforce_intake_id,
                    salesforce_account_id = EXCLUDED.salesforce_account_id
            """
            
            try:
                cursor.execute(upsert_query, (row['id'], salesforce_intake_id, salesforce_account_id))
                count += 1
                if count % 1000 == 0:
                    logging.info(f"{count} intake records upserted.")
            except Exception as upsert_error:
                error_count += 1
                logging.error(f"Failed to upsert record with ID {id}: {upsert_error}")
                continue

        except Exception as query_error:
            error_count += 1
            logging.error(f"Failed to process record with ID {id}: {query_error}")
            continue

    logging.info(f"Script completed: {count} records updated, {null_count} records had null data, {error_count} errors encountered.")

    logging.info(f"{count} intake records upserted.")
    logging.info(f"{null_count} null intake records upserted (NO INTAKE FOUND).")

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
