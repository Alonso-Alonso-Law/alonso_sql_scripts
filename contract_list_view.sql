-- client_list.contract_list_view source

CREATE OR REPLACE VIEW client_list.contract_list_view
AS SELECT
        CASE
            WHEN ps.full_name IS NULL THEN (ls.first_name::text || ' '::text) || ls.last_name::text
            ELSE ps.full_name
        END AS full_name,
        CASE
            WHEN ps.email_or_alt_email IS NULL THEN ls.email::text
            ELSE ps.email_or_alt_email
        END AS email_or_alt_email,
        CASE
            WHEN ps.phone IS NULL THEN ls.phone_number::text
            ELSE ps.phone
        END AS phone_number,
        CASE
            WHEN ps.id IS NULL THEN ls.camplegal_id_int::bigint
            ELSE ps.id
        END AS client_id_cl,
    ps.date_of_birth::date AS birth_date,
    EXTRACT(year FROM age(CURRENT_DATE::timestamp without time zone, ps.date_of_birth::timestamp without time zone)) AS cl_age,
    ps.country_birth AS country_origin,
        CASE
            WHEN ps.country_mailing IS NULL THEN ls.residence_country::text
            ELSE ps.country_mailing
        END AS residence_country,
    (((((ps.address_line_mailing || ', '::text) || ps.city_mailing) || ', '::text) || ps.state_mailing) || ', '::text) || ps.zip_mailing AS cl_address,
        CASE
            WHEN ps.id IS NULL THEN ls.camplegal_link::text
            ELSE ('https://lawyer.camplegal.com/index.html#/contacts/details/person/'::text || ''::text) || ps.id::text
        END AS camplegal_link,
    sd.consulta_date AS sd_consulta_date,
        CASE
            WHEN sd.signing_date IS NULL THEN cs.created_at::date
            ELSE sd.signing_date
        END AS signing_date,
        CASE
            WHEN sd.case_type_id IS NOT NULL THEN ctm.case_type
            ELSE sd.case_type
        END AS sd_case_type,
        CASE
            WHEN cn.case_number::text ~~ '%-1'::text THEN sd.contract_price
            WHEN cn.case_number::text !~~ '%-1'::text AND sd.bundle_case_contract = false THEN sd.contract_price
            ELSE 0::numeric
        END AS sd_contract_total_price,
        CASE
            WHEN cn.case_number::text ~~ '%-1'::text THEN sd.down_payment_amount
            WHEN cn.case_number::text !~~ '%-1'::text AND sd.bundle_case_contract = false THEN sd.contract_price
            ELSE 0::numeric
        END AS sd_opening_deposit,
        CASE
            WHEN cn.case_number::text ~~ '%-1'::text THEN sd.down_payment_received
            WHEN cn.case_number::text !~~ '%-1'::text AND sd.bundle_case_contract = false THEN sd.contract_price
            ELSE 0::numeric
        END AS sd_initial_payment,
    sd.deposit_received_date AS sd_initial_payment_received_date,
    cb.strategist_name AS sd_strategy_specialist,
    cci.strategist_name AS cc_contract_author,
    cca.strategist_name AS cc_contract_calls,
    cn.case_number,
    ctm.department,
    ctm.case_sub_type AS case_type,
        CASE
            WHEN cn.legacy_id IS NOT NULL OR cn.legacy_id <> 0 THEN true
            ELSE false
        END AS legacy_case,
        CASE
            WHEN cs.id IS NULL THEN false
            ELSE true
        END AS is_signed,
    cs.created_at AS cl_opening_case_date,
    sd.primary_camp_legal_url,
    cs.id AS cl_case_id,
    sd.id AS disposition_id,
    cn.case_type_id,
    sd.jotform_submission_id,
        CASE
            WHEN (( SELECT ct.activity
               FROM case_timeline ct
              WHERE ct.case_id = cs.id
              ORDER BY ct.instant DESC
             LIMIT 1)) IS NOT NULL THEN (( SELECT ct.activity
               FROM case_timeline ct
              WHERE ct.case_id = cs.id
              ORDER BY ct.instant DESC
             LIMIT 1))::character varying
            ELSE cs.case_status
        END AS case_status,
        CASE
            WHEN cs.case_status::text = 'OPEN'::text AND cs.id IS NOT NULL THEN true
            ELSE false
        END AS is_converted,
    ctm.sub_directory,
    ls.marketing_source AS referral_source,
    NULL::text AS salesforce_account_id,
        CASE
            WHEN ls.initial_submission_date IS NULL THEN ls.form_filled_date
            ELSE ls.initial_submission_date
        END AS intake_filled_date,
    cd.time_stamp AS intake_disposition_date
   FROM strategist_sales.strategist_decision sd
     LEFT JOIN intake.lucky_13_submission ls ON sd.jotform_submission_id = ls.id
     LEFT JOIN screener_sales.consulta_details cd ON ls.id = cd.jotform_submission_id AND cd.intake_result_id = 1
     LEFT JOIN strategist_sales.status s ON sd.status_id = s.id
     LEFT JOIN strategist_sales.strategist_specialists_info cci ON sd.contract_coordinator_author = cci.strategist_id
     LEFT JOIN strategist_sales.strategist_specialists_info cca ON sd.contract_coordinator_calls_assigned = cca.strategist_id
     LEFT JOIN strategist_sales.strategist_specialists_info cb ON sd.consulted_by_id = cb.strategist_id
     LEFT JOIN client_list.case_numbers cn ON sd.id = cn.contract_id
     LEFT JOIN strategist_sales.case_types_mapping ctm ON cn.case_type_id = ctm.id
     LEFT JOIN ( SELECT DISTINCT ON (cs1.case_number_identifier) cs1.id,
            cs1.case_number,
            cs1.case_status,
            cs1.petition_type,
            cs1.client,
            cs1.company,
            cs1."client_or_companyId",
            cs1.linked_user,
            cs1.created_at,
            cs1.case_update_text,
            cs1.case_update_date,
            cs1.case_update_by,
            cs1.case_update_color,
            cs1.deadline,
            cs1.deadline_status,
            cs1.current_milestone,
            cs1.originator_first_name,
            cs1.originator_last_name,
            cs1.originator_email,
            cs1.case_number_identifier,
            cs1.unique_case_identifier_int
           FROM cases_scraped cs1
          ORDER BY cs1.case_number_identifier, cs1.created_at) cs ON cn.case_number::text = TRIM(BOTH '-'::text FROM cs.case_number_identifier)
     LEFT JOIN people_scraped ps ON ((cs.client ->> 'id'::text)::bigint) = ps.id
  WHERE sd.status_id = 1 OR sd.status_id = 5 OR sd.status_id = 6
UNION
 SELECT ps.full_name,
    ps.email_or_alt_email,
    ps.phone AS phone_number,
    ps.id AS client_id_cl,
    ps.date_of_birth::date AS birth_date,
    EXTRACT(year FROM age(CURRENT_DATE::timestamp without time zone, ps.date_of_birth::timestamp without time zone)) AS cl_age,
    ps.country_birth AS country_origin,
    ps.country_mailing AS residence_country,
    (((((ps.address_line_mailing || ', '::text) || ps.city_mailing) || ', '::text) || ps.state_mailing) || ', '::text) || ps.zip_mailing AS cl_address,
    ('https://lawyer.camplegal.com/index.html#/contacts/details/person/'::text || ''::text) || ps.id::text AS camplegal_link,
    cd.appointment_date::date AS sd_consulta_date,
        CASE
            WHEN ccl.signing_date IS NULL THEN cs.created_at::date
            ELSE ccl.signing_date
        END AS signing_date,
    ccl.case_type AS sd_case_type,
    replace(replace(ccl.contract_total, '$'::text, ''::text), ','::text, ''::text)::numeric AS sd_contract_total_price,
        CASE
            WHEN ccl.deposit_amount IS NULL THEN replace(replace(ccl.initial_deposit, '$'::text, ''::text), ','::text, ''::text)::numeric
            ELSE replace(replace(ccl.deposit_amount::text, '$'::text, ''::text), ','::text, ''::text)::numeric
        END AS sd_opening_deposit,
    replace(replace(ccl.initial_deposit, '$'::text, ''::text), ','::text, ''::text)::numeric AS sd_initial_payment,
    ( SELECT b2.paid_on
           FROM billing b2
          WHERE b2.case_id = cs.id
          ORDER BY b2.paid_on
         LIMIT 1) AS sd_initial_payment_received_date,
    ccl.consultant_name AS sd_strategy_specialist,
    ccl.contract_coordinator AS cc_contract_author,
    ccl.contract_coordinator_calls AS cc_contract_calls,
    ccl.unique_case_identifier AS case_number,
        CASE
            WHEN ccl.consult_attorney = 'VA'::text THEN 'Family'::text
            ELSE 'Removal'::text
        END AS department,
    ccl.case_name AS case_type,
    false AS legacy_case,
        CASE
            WHEN ccl.signing_date IS NULL THEN false
            ELSE true
        END AS is_signed,
    cs.created_at AS cl_opening_case_date,
    ''::character varying AS primary_camp_legal_url,
    cs.id AS cl_case_id,
    0 AS disposition_id,
    0 AS case_type_id,
    ls.id AS jotform_submission_id,
    cs.case_status,
        CASE
            WHEN cs.case_status::text = 'OPEN'::text AND ccl.unique_case_identifier IS NOT NULL AND cs.id IS NOT NULL THEN true
            ELSE false
        END AS is_converted,
    ccl.sub_directory,
    ccl.referral_source,
    NULL::text AS salesforce_account_id,
        CASE
            WHEN ls.initial_submission_date IS NULL THEN ls.form_filled_date
            ELSE ls.initial_submission_date
        END AS intake_filled_date,
    cd.time_stamp AS intake_disposition_date
   FROM cases_client_list ccl
     LEFT JOIN cases_scraped cs ON ccl.unique_case_identifier = TRIM(BOTH '-'::text FROM cs.case_number_identifier)
     LEFT JOIN people_scraped ps ON ((cs.client ->> 'id'::text)::bigint) = ps.id
     LEFT JOIN ( SELECT ls_1.camplegal_id_int,
            min(ls_1.id) AS id,
            min(ls_1.form_filled_date) AS min
           FROM intake.lucky_13_submission ls_1
          GROUP BY ls_1.camplegal_id_int) ls2 ON ls2.camplegal_id_int = ps.id
     LEFT JOIN intake.lucky_13_submission ls ON ls2.id = ls.id AND ls.form_filled_date <= cs.created_at
     LEFT JOIN screener_sales.consulta_details cd ON ls.id = cd.jotform_submission_id AND cd.intake_result_id = 1
UNION
 SELECT DISTINCT ON (lm.id) lm.client_full_name AS full_name,
    lm.client_email AS email_or_alt_email,
    lm.client_phone_number AS phone_number,
    lm.client_legacy_id::bigint AS client_id_cl,
    lm.client_dob AS birth_date,
    EXTRACT(year FROM age(CURRENT_DATE::timestamp with time zone, lm.client_dob::timestamp with time zone)) AS cl_age,
    lm.client_birth_country AS country_origin,
    lm.client_residence_country AS residence_country,
    lm.client_address AS cl_address,
    NULL::text AS camplegal_link,
        CASE
            WHEN sd.consulta_date IS NULL THEN lm.consulta_scheduled_date::date
            ELSE sd.consulta_date
        END AS sd_consulta_date,
        CASE
            WHEN lm.open_date < '2024-09-01'::date AND ((lm.intake_signed_date AT TIME ZONE 'utc'::text) AT TIME ZONE 'cst'::text)::date < '2024-09-01'::date THEN ((lm.intake_signed_date AT TIME ZONE 'utc'::text) AT TIME ZONE 'cst'::text)::date
            ELSE lm.open_date
        END AS signing_date,
    lm.case_type AS sd_case_type,
    lm.contract_total AS sd_contract_total_price,
    lm.initial_deposit_total AS sd_opening_deposit,
    lm.initial_deposit_paid AS sd_initial_payment,
    sd.deposit_received_date AS sd_initial_payment_received_date,
        CASE
            WHEN lm.strategy_specialist IS NULL THEN cby.strategist_name::text
            ELSE (ss.f_name::text || ' '::text) || ss.l_name::text
        END AS sd_strategy_specialist,
    (cb.f_name::text || ' '::text) || cb.l_name::text AS cc_contract_author,
    (cca.f_name::text || ' '::text) || cca.l_name::text AS cc_contract_calls,
    (TRIM(BOTH FROM split_part(lm.display_name, ' - '::text, 1)) || ' - '::text) || TRIM(BOTH FROM split_part(lm.display_name, ' - '::text, 2)) AS case_number,
        CASE
            WHEN lm.department <> ''::text THEN lm.department::character varying
            ELSE
            CASE
                WHEN lm.department IS NOT NULL THEN lm.department::character varying
                WHEN lm.case_type = 'VAWA'::text THEN 'Family'::character varying
                WHEN lm.case_sub_type = 'T-visa'::text OR lm.case_sub_type ~~ 'U-visa%'::text THEN 'Removal'::character varying
                ELSE NULL::character varying
            END
        END AS department,
    regexp_replace(lm.case_sub_type, '\s\d{2}$'::text, ''::text) AS case_type,
        CASE
            WHEN lm.client_legacy_id IS NOT NULL THEN true
            ELSE false
        END AS legacy_case,
        CASE
            WHEN lm.id IS NULL THEN false
            ELSE true
        END AS is_signed,
    lm.open_date AS cl_opening_case_date,
    ls.camplegal_link AS primary_camp_legal_url,
    NULL::bigint AS cl_case_id,
    sd.id AS disposition_id,
    sd.case_type_id,
    ls.id AS jotform_submission_id,
    upper(lm.status) AS case_status,
        CASE
            WHEN lm.id IS NOT NULL THEN true
            ELSE false
        END AS is_converted,
    lm.sub_directory,
    lie.marketing_source AS referral_source,
    lm.id AS salesforce_account_id,
        CASE
            WHEN ls.initial_submission_date IS NULL THEN ls.form_filled_date
            ELSE ls.initial_submission_date
        END AS intake_filled_date,
    cd.time_stamp AS intake_disposition_date
   FROM litify_matters_extracted lm
     LEFT JOIN intake.lucky_13_submission ls ON lm.client_phone_number = ls.phone_number::text
     LEFT JOIN screener_sales.consulta_details cd ON ls.id = cd.jotform_submission_id AND cd.intake_result_id = 1
     LEFT JOIN strategist_sales.strategist_decision sd ON ls.id = sd.jotform_submission_id
     LEFT JOIN strategist_sales.strategist_specialists_info cci ON sd.contract_coordinator_author = cci.strategist_id
     LEFT JOIN strategist_sales.strategist_specialists_info cby ON sd.consulted_by_id = cby.strategist_id
     LEFT JOIN alonso_staff ss ON lm.strategy_specialist = ss.salesforce_id
     LEFT JOIN alonso_staff cca ON lm.contract_coordinator = cca.salesforce_id
     LEFT JOIN alonso_staff cb ON lm.created_by_id = cb.salesforce_id
     LEFT JOIN strategist_sales.case_types_mapping ctm ON sd.case_type_id = ctm.id
     LEFT JOIN litify_intakes_extracted lie ON lm.id::text = lie.matter_id;