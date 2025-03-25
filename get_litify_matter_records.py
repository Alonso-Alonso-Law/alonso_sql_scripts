import logging
import os
from simple_salesforce import Salesforce
import psycopg2
import psycopg2.extras
import time
import toml

# Configure logging
log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'litify_matters_records_log.log')
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logging.info("Script started")

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

    # Salesforce SOQL Query
    soql_query = """
        SELECT 
            Id, 
            Name, 
            litify_pm__Primary_Intake__c, 
            litify_pm__Primary_Intake__r.Name,
            litify_pm__Open_Date__c, 
            lps_Client_Name__c,
            litify_pm__Client__r.Legacy_Id__c, 
            litify_pm__Client__r.Client_Number__c,
            litify_pm__Client__r.litify_pm__Date_of_birth__c,
            litify_pm__Client__r.litify_pm__Phone_Mobile__c,
            litify_pm__Client__r.litify_pm__Email__c,
            litify_pm__Client__r.Street_City_State_Zip_DocGen__c,
            litify_pm__Client__r.lps_Country_of_Birth__c,
            litify_pm__Client__r.lps_Client_Country__c,
            litify_pm__Primary_Intake__r.litify_pm__Retainer_Agreement_Signed__c,
            lps_Display_Name_custom__c, 
            lps_Type_of_Case__c,
            lps_Case_Type_Picklist__c, 
            lps_Case_Sub_Type__c,
            lps_Case_Sub_Sub_Type__c, 
            litify_pm__Status__c,
            Total_Contract_Amount__c, 
            Total_Paid_Matter_Fee__c,
            litify_pm__Primary_Intake__r.lps_Down_Payment_Total__c,
            litify_pm__Primary_Intake__r.lps_Down_Payment_Paid__c,
            lps_Filing_Fee_Total__c, 
            lps_Filing_Fee_Paid__c,
            litify_pm__Primary_Intake__r.lps_Strategy_Specialist__c,
            litify_pm__Primary_Intake__r.lps_Contract_Coordinator__c,
            CreatedById,
            litify_pm__Primary_Intake__r.Consulta_Scheduled_Date__c,
            litify_pm__Primary_Intake__r.lps_Strategy_Analyst__c,
            CreatedDate,
            litify_pm__Primary_Intake__r.lps_Source_picklist__c 
        FROM 
            litify_pm__Matter__c 
        WHERE 
            litify_pm__Open_Date__c >= 2024-07-27
            AND (NOT lps_Client_Name__c LIKE '%test%')
            AND (NOT lps_Client_Name__c LIKE '%train%')
            AND CreatedById != '0058a00000LlJRfAAN'
    """

    try:
        sf_start_time = time.time()
        sf_data = sf.query_all(soql_query)
        record_count = sf_data['totalSize']
        sf_end_time = time.time()
        logging.info(f"Retrieved {record_count} records from Salesforce in {sf_end_time - sf_start_time:.2f} seconds")
    except Exception as e:
        logging.error(f"Error retrieving data from Salesforce: {e}")
        raise

    # Insert/Update records in PostgreSQL
    upsert_query = """
        INSERT INTO public.litify_matters_extracted (
            id, matter_number, intake_id, intake_name, open_date, client_full_name, 
            client_legacy_id, client_account_number, client_dob, client_phone_number, 
            client_email, client_address, client_birth_country, client_residence_country, 
            intake_signed_date, display_name, department, case_type, case_sub_type, 
            sub_sub_type, status, contract_total, matter_fee_paid, initial_deposit_total, 
            initial_deposit_paid, filing_fee_total, filing_fee_paid, strategy_specialist, 
            contract_coordinator, created_by_id, consulta_scheduled_date, created_timestamp, marketing_source
        )
        VALUES (
            %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s,  
            %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, 
            %s, %s, %s
        )
        ON CONFLICT (id) DO UPDATE
        SET 
            status = EXCLUDED.status,
            matter_number = EXCLUDED.matter_number,
            intake_id = EXCLUDED.intake_id,
            intake_name = EXCLUDED.intake_name,
            open_date = EXCLUDED.open_date,
            client_full_name = EXCLUDED.client_full_name,
            client_legacy_id = EXCLUDED.client_legacy_id,
            client_account_number = EXCLUDED.client_account_number,
            client_dob = EXCLUDED.client_dob,
            client_phone_number = EXCLUDED.client_phone_number,
            client_email = EXCLUDED.client_email,
            client_address = EXCLUDED.client_address,
            client_birth_country = EXCLUDED.client_birth_country,
            client_residence_country = EXCLUDED.client_residence_country,
            intake_signed_date = EXCLUDED.intake_signed_date,
            display_name = EXCLUDED.display_name,
            department = EXCLUDED.department,
            case_type = EXCLUDED.case_type,
            case_sub_type = EXCLUDED.case_sub_type,
            sub_sub_type = EXCLUDED.sub_sub_type,
            contract_total = EXCLUDED.contract_total,
            matter_fee_paid = EXCLUDED.matter_fee_paid,
            initial_deposit_total = EXCLUDED.initial_deposit_total,
            initial_deposit_paid = EXCLUDED.initial_deposit_paid,
            filing_fee_total = EXCLUDED.filing_fee_total,
            filing_fee_paid = EXCLUDED.filing_fee_paid,
            strategy_specialist = EXCLUDED.strategy_specialist,
            contract_coordinator = EXCLUDED.contract_coordinator,
            created_by_id = EXCLUDED.created_by_id,
            consulta_scheduled_date = EXCLUDED.consulta_scheduled_date,
            created_timestamp = EXCLUDED.created_timestamp,
            marketing_source = EXCLUDED.marketing_source;
    """

    count = 0
    error_count = 0
    try:
        for record in sf_data['records']:
            try:
                if record['litify_pm__Primary_Intake__r'] == None:
                    intake_id = None
                    intake_name = None
                    intake_signed_date = None
                    strategy_specialist = None
                    contract_coordinator = None
                    initial_deposit_total = None
                    initial_deposit_paid = None
                    consulta_scheduled_date = None
                    marketing_source = None
                else:
                    intake_id = record['litify_pm__Primary_Intake__c']
                    intake_name = record['litify_pm__Primary_Intake__r']['Name'] 
                    intake_signed_date = record['litify_pm__Primary_Intake__r']['litify_pm__Retainer_Agreement_Signed__c']
                    strategy_specialist = record['litify_pm__Primary_Intake__r']['lps_Strategy_Specialist__c']
                    contract_coordinator = record['litify_pm__Primary_Intake__r']['lps_Contract_Coordinator__c']
                    initial_deposit_total = record['litify_pm__Primary_Intake__r']['lps_Down_Payment_Total__c']
                    initial_deposit_paid = record['litify_pm__Primary_Intake__r']['lps_Down_Payment_Paid__c']
                    consulta_scheduled_date = record['litify_pm__Primary_Intake__r']['Consulta_Scheduled_Date__c']
                    marketing_source = record['litify_pm__Primary_Intake__r']['lps_Source_picklist__c']

                cursor.execute(upsert_query, (
                    record['Id'], #matter_id
                    record['Name'], #matter_number -- the pattern assigned by Salesforce 'MAT-00000001'
                    intake_id, #intake_id
                    intake_name,   #intake_name -- the pattern assigned by Salesforce 'INT-00000001'
                    record['litify_pm__Open_Date__c'] if 'litify_pm__Open_Date__c' in record else None , #matter_open_date -- this is what is actually being used for the signing_date field
                    record['lps_Client_Name__c'] if 'lps_Client_Name__c' in record else None, #client_full_name
                    record['litify_pm__Client__r']['Legacy_Id__c'] if 'litify_pm__Client__r' in record else None, #client_legacy_id
                    record['litify_pm__Client__r']['Client_Number__c'] if 'litify_pm__Client__r' in record else None, #client_account_number
                    record['litify_pm__Client__r']['litify_pm__Date_of_birth__c'] if 'litify_pm__Client__r' in record else None, #client_dob
                    record['litify_pm__Client__r']['litify_pm__Phone_Mobile__c'] if 'litify_pm__Client__r' in record else None,     #client_phone_number
                    record['litify_pm__Client__r']['litify_pm__Email__c'] if 'litify_pm__Client__r' in record else None, #client_email 
                    record['litify_pm__Client__r']['Street_City_State_Zip_DocGen__c'] if 'litify_pm__Client__r' in record else None, #client_address
                    record['litify_pm__Client__r']['lps_Country_of_Birth__c'] if 'litify_pm__Client__r' in record else None, #client_birth_country
                    record['litify_pm__Client__r']['lps_Client_Country__c'] if 'litify_pm__Client__r' in record else None, #client_residence_country
                    intake_signed_date, #intake_signed_date -- this is not being used for the signing_date field
                    record['lps_Display_Name_custom__c'] if 'lps_Display_Name_custom__c' in record else None, #case_number
                    record['lps_Type_of_Case__c'] if 'lps_Type_of_Case__c' in record else None, #department of case type
                    record['lps_Case_Type_Picklist__c'] if 'lps_Case_Type_Picklist__c' in record else None, #case_type
                    record['lps_Case_Sub_Type__c'] if 'lps_Case_Sub_Type__c' in record else None,  #case_sub_type
                    record['lps_Case_Sub_Sub_Type__c'] if 'lps_Case_Sub_Sub_Type__c' in record else None, #case_sub_sub_type -- this is USC/LPR
                    record['litify_pm__Status__c'] if 'litify_pm__Status__c' in record else None, #case_status -- OPEN, CLOSED, etc
                    record['Total_Contract_Amount__c'] if 'Total_Contract_Amount__c' in record else None, #contract_total
                    record['Total_Paid_Matter_Fee__c'] if 'Total_Paid_Matter_Fee__c' in record else None, #matter_fee_paid -- aka total_paid, but has no limit as to how much has been paid, hence the additional script that retrieves the IOLTA deposits
                    initial_deposit_total, #initial_deposit_total -- aka down_payment_total
                    initial_deposit_paid, #initial_deposit_paid -- aka down_payment_paid
                    record['lps_Filing_Fee_Total__c'] if 'lps_Filing_Fee_Total__c' in record else None, #filing_fee_total -- this is the total filing fee not related to the matter fee
                    record['lps_Filing_Fee_Paid__c'] if 'lps_Filing_Fee_Paid__c' in record else None, #filing_fee_paid -- this is the total filing fee paid
                    strategy_specialist, #strategy_specialist -- this is the salesforce user id of the strategist assigned in the Intake level
                    contract_coordinator, #contract_coordinator -- this is the salesforce user id of the coordinator assigned in the Intake level
                    record['CreatedById'] if 'CreatedById' in record else None, #created_by_id -- this is the salesforce user id of the user who created the matter
                    consulta_scheduled_date, #consulta_scheduled_date -- this is the date of the scheduled consultation in the Intake level
                    record['CreatedDate'] if 'CreatedDate' in record else None, #created_timestamp -- this is the date and time the matter was created, should be the same as open date,
                    marketing_source
                ))
                count += 1
            except Exception as e:
                logging.error(f"Error processing record {record['Id']}: {e}")
                error_count += 1

        conn.commit()
        logging.info(f"{count} records upserted successfully")
        if error_count > 0:
            logging.warning(f"{error_count} records failed to upsert")
    except Exception as e:
        logging.error(f"Error during upsert process: {e}")
        conn.rollback()

    # Cleanup
    cursor.close()
    conn.close()
    logging.info("Script completed")

except Exception as e:
    logging.error(f"An error occurred: {e}")
finally:
    if 'cursor' in locals() and cursor:
        cursor.close()
        logging.info("Cursor closed.")
    if 'conn' in locals() and conn:
        conn.close()
        logging.info("Database connection closed.")
