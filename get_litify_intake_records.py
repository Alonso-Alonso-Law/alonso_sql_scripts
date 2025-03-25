from simple_salesforce import Salesforce
import psycopg2
import psycopg2.extras
import time
import logging
import toml

import os
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

# PostgreSQL connection
with open(config_path, 'r') as file:
    config = toml.load(file)['litify']
        
    # Detalles de la cuenta de Salesforce
    username = config['LF_USER']
    password = config['LF_PASSWORD']
    security_token = config['LF_TOKEN'] 

sf = Salesforce(username=username, password=password, security_token=security_token)
conn = psycopg2.connect(**db_config)
cursor = conn.cursor()

# Salesforce SOQL Query
soql_query = """
    SELECT 
    Id,
    Name,
    CreatedDate, 
    CreatedBy.Email, 
    lps_Consulta_QA_Status__c,
    lps_Consulta_QA__c,
    lps_Consulta_QA__r.Email, 
    lps_Consulta_QA__r.Name, 
    lps_Client_s_Name__c,
    litify_pm__Phone__c, 
    litify_pm__Client__r.litify_pm__Email__c, 
    ClientAge__c, 
    litify_pm__Client__r.BillingPostalCode, 
    Consulta_Scheduled_Date__c, 
    litify_pm__Status__c,
    lps_Strategy_Specialist__r.Email, 
    lps_Strategy_Specialist__r.Name,
    lps_Strategy_Specialist_Status__c,
    lps_Strategy_Analyst_Status__c,
    litify_pm__Matter__c, 
    litify_pm__Matter__r.Total_Contract_Amount__c, 
    lps_Down_Payment_Total__c, 
    lps_Down_Payment_Paid__c, 
    lps_DNQ__c,
    lps_Reason_for_DNQ__c, 
    LastModifiedBy.Name,
    LastModifiedDate,
    litify_pm__Retainer_Agreement_Signed__c,
    Case_Type_Picklist__c ,
    lps_Refund_Requested__c,
    lps_Contract_Status__c,
    Not_Converted_Reason__c,
    litify_pm__Converted_Date__c,
    lps_Source_picklist__c, 
    Referral_By__r.Name,
    Referral_By__r.Client_Number__c
    FROM litify_pm__Intake__c
    WHERE 
    (lps_Strategy_Specialist__c != null or litify_pm__Matter__c != null)
    AND (lps_Consulta_QA_Status__c = 'Approved'
    OR litify_pm__Status__c = 'Remedy Review' 
    or litify_pm__Status__c = 'Consulted'
    or litify_pm__Status__c = 'Contract Sent'
    or litify_pm__Status__c = 'Contract Scheduled'
    or litify_pm__Status__c = 'Converted')
    AND (NOT lps_Client_s_Name__c LIKE '%test%')
    AND (NOT lps_Client_s_Name__c LIKE '%train%')
    AND (CreatedDate >  2024-07-27T12:06:40.000+0000 or litify_pm__Converted_Date__c > 2024-07-27T12:06:40.000+0000)
"""

sf_s_time = time.time()
sf_data = sf.query_all(soql_query)
record_count = sf_data['totalSize']
sf_e_time = time.time()
sf_total_time = sf_e_time - sf_s_time

estimated_runtime = 0.08775161452263407 * record_count
print(f"Total records returned from Salesforce: {record_count}. Retrieved in {sf_total_time} seconds")
print(f"Estimated runtime: {estimated_runtime}")

upsert_query = """
    INSERT INTO public.litify_intakes_extracted (id, name, created_date, created_by_email, consulta_qa_status, consulta_qa_email, 
    consulta_qa_name, client_name, client_phone, client_email, client_age, client_billing_postal_code, 
    consulta_scheduled_date, client_status, strategy_specialist_email, strategy_specialist_name, 
    strategy_specialist_status, strategy_analyst_status, matter_total_contract_amount, down_payment_total, 
    down_payment_paid, matter_id, dnq, reason_for_dnq, last_modified_by_name, last_modified_date, 
    contract_signing_date, intake_case_type,refund_request, contract_status,not_converted_reason, converted_date,
    marketing_source, referred_by_name, referred_by_client_number)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id) 
    DO UPDATE SET
        consulta_qa_status = EXCLUDED.consulta_qa_status,
        consulta_qa_email = EXCLUDED.consulta_qa_email,
        consulta_qa_name = EXCLUDED.consulta_qa_name,
        client_status = EXCLUDED.client_status,
        strategy_specialist_email = EXCLUDED.strategy_specialist_email,
        strategy_specialist_name = EXCLUDED.strategy_specialist_name,
        strategy_specialist_status = EXCLUDED.strategy_specialist_status,
        strategy_analyst_status = EXCLUDED.strategy_analyst_status,
        matter_total_contract_amount = EXCLUDED.matter_total_contract_amount,
        down_payment_total = EXCLUDED.down_payment_total,
        down_payment_paid = EXCLUDED.down_payment_paid,
        matter_id = EXCLUDED.matter_id,
        dnq = EXCLUDED.dnq,
        reason_for_dnq = EXCLUDED.reason_for_dnq,
        last_modified_by_name = EXCLUDED.last_modified_by_name,
        last_modified_date = EXCLUDED.last_modified_date,
        contract_signing_date = EXCLUDED.contract_signing_date,
        intake_case_type = EXCLUDED.intake_case_type,
        refund_request = EXCLUDED.refund_request,
        contract_status = EXCLUDED.contract_status,
        not_converted_reason = EXCLUDED.not_converted_reason,
        converted_date = EXCLUDED.converted_date,
        marketing_source = EXCLUDED.marketing_source,
        referred_by_name = EXCLUDED.referred_by_name,
        referred_by_client_number = EXCLUDED.referred_by_client_number;
"""

count = 0
error_count = 0
db_s_time = time.time()

for record in sf_data['records']:
    cursor.execute(upsert_query, (
        record['Id'],
        record['Name'],
        record['CreatedDate'],
        record['CreatedBy']['Email'],
        record['lps_Consulta_QA_Status__c'],
        record['lps_Consulta_QA__r']['Email'] if record['lps_Consulta_QA__c'] else None,
        record['lps_Consulta_QA__r']['Name'] if record['lps_Consulta_QA__c'] else None,
        record['lps_Client_s_Name__c'],
        record['litify_pm__Phone__c'],
        record['litify_pm__Client__r']['litify_pm__Email__c'] if record['litify_pm__Client__r'] else None,
        record['ClientAge__c'],
        record['litify_pm__Client__r']['BillingPostalCode'] if record['litify_pm__Client__r'] else None,
        record['Consulta_Scheduled_Date__c'],
        record['litify_pm__Status__c'],
        record['lps_Strategy_Specialist__r']['Email'] if record['lps_Strategy_Specialist__r'] else None,
        record['lps_Strategy_Specialist__r']['Name'] if record['lps_Strategy_Specialist__r'] else None,
        record['lps_Strategy_Specialist_Status__c'],
        record['lps_Strategy_Analyst_Status__c'],
        record['litify_pm__Matter__r']['Total_Contract_Amount__c'] if record['litify_pm__Matter__c'] else None,
        record['lps_Down_Payment_Total__c'],
        record['lps_Down_Payment_Paid__c'],
        record['litify_pm__Matter__c'],
        record['lps_DNQ__c'],
        record['lps_Reason_for_DNQ__c'],
        record['LastModifiedBy']['Name'],
        record['LastModifiedDate'],
        record['litify_pm__Retainer_Agreement_Signed__c'],
        record['Case_Type_Picklist__c'],
        record['lps_Refund_Requested__c'],
        record['lps_Contract_Status__c'],
        record['Not_Converted_Reason__c'],
        record['litify_pm__Converted_Date__c'],
        record['lps_Source_picklist__c'],
        record['Referral_By__r']['Name'] if record.get('Referral_By__r') else None,
        record['Referral_By__r']['Client_Number__c'] if record.get('Referral_By__r') else None
    ))
    count = count + 1
    if (count % 500 == 0):
        print(count, 'Records upserted')

        # print(count, 'Record upserted: ', record['Id'])

db_e_time = time.time()
db_total_time = db_e_time - db_s_time
avg_time = db_total_time / record_count

print(count, ' records upserted in ', db_total_time, ' seconds')
print('Average time per upsert: ', avg_time, 'seconds')
print(error_count, ' records failed upsert')

# Commit and close
conn.commit()
cursor.close()
conn.close()