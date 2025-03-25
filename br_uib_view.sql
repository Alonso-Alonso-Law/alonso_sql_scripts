-- client_list.br_uib_view source

CREATE OR REPLACE VIEW client_list.br_uib_view
AS SELECT DISTINCT ON (clv.case_number) clv.cl_case_id,
        CASE
            WHEN clv.department::text = 'Family'::text THEN 'Vanessa R. Alonso'::character varying
            WHEN clv.department::text = 'Removal'::text THEN 'Christopher L. Alonso'::character varying
            ELSE NULL::character varying
        END AS assigned_attorney,
    clv.cl_opening_case_date AS case_created_at,
    clv.signing_date,
    clv.department,
    clv.sub_directory,
    clv.sd_case_type::text AS case_type,
    clv.case_number,
    clv.case_type AS case_sub_type,
    cs.case_number AS case_name,
    clv.case_status,
    clv.full_name AS client_name,
    clv.cl_address AS client_address,
    clv.referral_source,
    clv.sd_contract_total_price,
    clv.sd_opening_deposit,
        CASE
            WHEN lm.id IS NOT NULL THEN COALESCE(LEAST(COALESCE(clv.sd_initial_payment, 0::numeric), COALESCE(clv.sd_opening_deposit, 0::numeric)), 0::numeric)::double precision
            ELSE ( SELECT LEAST(COALESCE(sum(b2.amount), 0::double precision), COALESCE(clv.sd_opening_deposit, 0::numeric)::double precision) AS "least"
               FROM billing b2
              WHERE b2.case_id = clv.cl_case_id AND b2.payment_status::text = 'COMPLETED'::text)
        END AS deposit_running_total,
        CASE
            WHEN lm.id IS NOT NULL THEN COALESCE(COALESCE(clv.sd_opening_deposit, 0::numeric) - LEAST(COALESCE(clv.sd_initial_payment, 0::numeric), COALESCE(clv.sd_opening_deposit, 0::numeric)), 0::numeric)::double precision
            ELSE COALESCE(COALESCE(clv.sd_opening_deposit, 0::numeric)::double precision - (( SELECT LEAST(COALESCE(sum(b2.amount), 0::double precision), COALESCE(clv.sd_opening_deposit, 0::numeric)::double precision) AS "least"
               FROM billing b2
              WHERE b2.case_id = clv.cl_case_id AND b2.payment_status::text = 'COMPLETED'::text)), 0::double precision)
        END AS remaining_balance,
    clv.camplegal_link,
    clv.sd_strategy_specialist,
    clv.cc_contract_author,
    clv.cc_contract_calls,
    clv.is_signed,
        CASE
            WHEN lm.id IS NULL THEN COALESCE(( SELECT sum(b2.amount) AS sum
               FROM billing b2
              WHERE b2.case_id = cs.id AND b2.paid_on >= date_trunc('MONTH'::text, ( SELECT min(b3.created_at) AS min
                       FROM billing b3
                      WHERE b3.case_id = cs.id AND b3.payment_status::text = 'COMPLETED'::text
                     LIMIT 1)) AND b2.paid_on <= (date_trunc('MONTH'::text, cs.created_at) + '1 mon'::interval - '1 day'::interval) AND b2.payment_status::text = 'COMPLETED'::text), 0::double precision)
            ELSE lm.iolta_deposits_total::double precision
        END AS transactions_running_total,
    sd.bundle_case_contract AS bundle,
    cnv.contract_number,
    clv.client_id_cl,
    ( SELECT e.formatted_name
           FROM task_case tc
             LEFT JOIN camplegal_staff_list e ON tc.assigned_to = e.id
          WHERE tc.case_id = clv.cl_case_id AND tc.name = 'New File Review'::text AND (tc.assigned_to <> ALL (ARRAY[10324, 2220, 35776, 33213])) AND e.user_type <> 'LAWYER'::text
          ORDER BY tc.completed_at
         LIMIT 1) AS paralegal_assigned,
    clv.jotform_submission_id,
    (s.f_name::text || ' '::text) || s.l_name::text AS screener_author,
    COALESCE(
        CASE
            WHEN ozc.office_code::text = 'SAT'::text THEN 'SAT'::text
            WHEN ozc.office_code::text = 'DAL'::text AND clv.cl_opening_case_date >= '2023-07-08 00:00:00'::timestamp without time zone THEN 'DAL'::text
            WHEN ozc.office_code::text = 'HOU'::text AND clv.cl_opening_case_date >= '2023-06-03 00:00:00'::timestamp without time zone THEN 'HOU'::text
            WHEN ozc.office_code::text = 'ATX'::text AND clv.cl_opening_case_date >= '2023-08-05 00:00:00'::timestamp without time zone THEN 'ATX'::text
            WHEN ozc.office_code::text = 'PHX'::text AND clv.cl_opening_case_date >= '2024-04-27 00:00:00'::timestamp without time zone THEN 'PHX'::text
            WHEN ozc.office_code::text = 'MCTX'::text AND clv.cl_opening_case_date >= '2023-11-10 00:00:00'::timestamp without time zone THEN 'MCTX'::text
            ELSE 'VIRTUAL'::text
        END, 'VIRTUAL'::text)::character varying AS office_code,
    ((('https://alonsoalonsoattorneysatlaw.lightning.force.com/lightning/r/litify_pm__Matter__c/'::text || ''::text) || clv.salesforce_account_id) || ''::text) || '/view'::text AS litify_matter_url,
    lm.matter_number,
    ss.work_email AS strategy_specialist_email,
    lm.iolta_deposits_total::double precision AS iolta_transactions_total,
    lm.intake_signed_date AS contract_signed_date,
    ((('https://alonsoalonsoattorneysatlaw.lightning.force.com/lightning/r/litify_pm__Intake__c/'::text || ''::text) || lm.intake_id) || ''::text) || '/view'::text AS litify_intake_url,
    clv.phone_number AS client_phone_number,
    cd.call_type,
    clv.salesforce_account_id,
    clv.sd_initial_payment,
    lm.id AS litify_id,
    lm.client_account_number,
    clv.intake_filled_date,
    clv.sd_consulta_date AS consulta_scheduled_date,
    clv.intake_disposition_date
   FROM client_list.contract_list_view clv
     LEFT JOIN cases_scraped cs ON cs.id = clv.cl_case_id
     LEFT JOIN people_scraped ps ON clv.client_id_cl = ps.id
     LEFT JOIN litify_matters_extracted lm ON clv.salesforce_account_id = lm.id::text
     LEFT JOIN catalog_tables.office_zip_codes ozc ON ps.zip_mailing = ozc.zip_code::text OR lm.client_zip_code::text = ozc.zip_code::text
     LEFT JOIN strategist_sales.strategist_decision sd ON sd.id = clv.disposition_id
     LEFT JOIN cases_scraped_extra_info csi ON csi.id = clv.cl_case_id
     LEFT JOIN intake.lucky_13_submission ls ON clv.jotform_submission_id = ls.id
     LEFT JOIN screener_sales.consulta_details cd ON ls.id = cd.jotform_submission_id AND cd.intake_result_id = 1
     LEFT JOIN alonso_staff s ON cd.screener_owner = s.work_email
     LEFT JOIN alonso_staff ss ON lm.strategy_specialist = ss.salesforce_id OR sd.consulted_by_id = ss.id
     LEFT JOIN client_list.contract_numbers_view cnv ON cnv.case_holder_id = clv.client_id_cl
     LEFT JOIN strategist_sales.case_types_mapping ctm ON clv.case_type_id = ctm.id;