select 
full_name,
email_or_alt_email,
phone_number,
client_id_cl,
birth_date,
cl_age,
country_origin,
residence_country,
cl_address,
camplegal_link,
sd_consulta_date,
signing_date,
sd_case_type,
sd_contract_total_price,
sd_opening_deposit,
sd_initial_payment,
sd_initial_payment_received_date,
sd_strategy_specialist,
cc_contract_author,
cc_contract_calls ,
case_number,
department,
case_type,
legacy_case,
is_signed,
cl_opening_case_date,
primary_camp_legal_url,
cl_case_id,
disposition_id,
case_type_id,
jotform_submission_id,
case_status,
is_converted,
sub_directory,
referral_source,
salesforce_account_id,
intake_filled_date,
intake_disposition_date
from client_list.camp_legal_cases_archive cl 
UNION
 SELECT DISTINCT ON (lm.id)
    lm.client_full_name AS full_name,
    lm.client_email AS email_or_alt_email,
    lm.client_phone_number AS phone_number,
    lm.client_legacy_id::bigint AS client_id_cl,
    lm.client_dob AS birth_date,
    EXTRACT(year FROM age(CURRENT_DATE::timestamp with time zone, lm.client_dob::timestamp with time zone)) AS cl_age,
    lm.client_birth_country AS country_origin,
    lm.client_residence_country AS residence_country,
    lm.client_address AS cl_address,
    NULL::text AS camplegal_link,
    COALESCE(sd.consulta_date, lm.consulta_scheduled_date::date) AS sd_consulta_date,
    CASE
        WHEN lm.open_date < '2024-09-01'::date AND ((lm.intake_signed_date AT TIME ZONE 'utc') AT TIME ZONE 'cst')::date < '2024-09-01'::date THEN ((lm.intake_signed_date AT TIME ZONE 'utc') AT TIME ZONE 'cst')::date
        ELSE lm.open_date
    END AS signing_date,
    lm.case_type AS sd_case_type,
    lm.contract_total AS sd_contract_total_price,
    lm.initial_deposit_total AS sd_opening_deposit,
    lm.initial_deposit_paid AS sd_initial_payment,
    sd.deposit_received_date AS sd_initial_payment_received_date,
    COALESCE(ss.f_name || ' ' || ss.l_name, cby.strategist_name::text) AS sd_strategy_specialist,
    cb.f_name || ' ' || cb.l_name AS cc_contract_author,
    cca.f_name || ' ' || cca.l_name AS cc_contract_calls,
    TRIM(BOTH FROM split_part(lm.display_name, ' - ', 1)) || ' - ' || TRIM(BOTH FROM split_part(lm.display_name, ' - ', 2)) AS case_number,
    COALESCE(NULLIF(lm.department, ''), CASE
        WHEN lm.case_type = 'VAWA' THEN 'Family'
        WHEN lm.case_sub_type = 'T-visa' OR lm.case_sub_type LIKE 'U-visa%' THEN 'Removal'
    END) AS department,
    regexp_replace(lm.case_sub_type, '\s\d{2}$', '') AS case_type,
    lm.client_legacy_id IS NOT NULL AS legacy_case,
    lm.id IS NOT NULL AS is_signed,
    lm.open_date AS cl_opening_case_date,
    ls.camplegal_link AS primary_camp_legal_url,
    NULL::bigint AS cl_case_id,
    sd.id AS disposition_id,
    sd.case_type_id,
    ls.id AS jotform_submission_id,
    UPPER(lm.status) AS case_status,
    lm.id IS NOT NULL AS is_converted,
    lm.sub_directory,
    lie.marketing_source AS referral_source,
    lm.id AS salesforce_account_id,
    COALESCE(ls.initial_submission_date, ls.form_filled_date) AS intake_filled_date,
    cd.time_stamp AS intake_disposition_date
FROM litify_matters_extracted lm
LEFT JOIN intake.lucky_13_submission ls ON lm.client_phone_number = ls.phone_number
LEFT JOIN screener_sales.consulta_details cd ON ls.id = cd.jotform_submission_id AND cd.intake_result_id = 1
LEFT JOIN strategist_sales.strategist_decision sd ON ls.id = sd.jotform_submission_id
LEFT JOIN strategist_sales.strategist_specialists_info cci ON sd.contract_coordinator_author = cci.strategist_id
LEFT JOIN strategist_sales.strategist_specialists_info cby ON sd.consulted_by_id = cby.strategist_id
LEFT JOIN alonso_staff ss ON lm.strategy_specialist = ss.salesforce_id
LEFT JOIN alonso_staff cca ON lm.contract_coordinator = cca.salesforce_id
LEFT JOIN alonso_staff cb ON lm.created_by_id = cb.salesforce_id
LEFT JOIN strategist_sales.case_types_mapping ctm ON sd.case_type_id = ctm.id
LEFT JOIN litify_intakes_extracted lie ON lm.id = lie.matter_id::text;