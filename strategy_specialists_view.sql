-- strategist_sales.strategy_specialists_metrics_view source

CREATE OR REPLACE VIEW strategist_sales.strategy_specialists_metrics_view
AS SELECT sd.id::character varying AS id,
    sd.jotform_submission_id AS intake_id,
    sd.consulta_date AS consulta_scheduled_date,
    sd.signing_date,
    (ls.first_name::text || ' '::text) || ls.last_name::text AS client_name,
    ls.phone_number,
    ssi.strategist_name AS consultation_strategist_name,
    ssi.strategist_id,
    as2.work_email AS consultation_strategist_email,
    ssi.team_name AS consultation_strategist_team,
    ssi.office_location_name AS consultation_strategist_office_location,
    s.id AS consulta_status_id,
        CASE
            WHEN s.id = 6 AND cs.id IS NOT NULL THEN 'Converted to contract'::character varying
            ELSE s.name
        END AS consulta_status,
        CASE
            WHEN cn.case_number_client_count <> 1 THEN 0::numeric
            ELSE sd.contract_price
        END AS contract_price,
        CASE
            WHEN cn.case_number_client_count <> 1 THEN 0::numeric
            ELSE sd.down_payment_amount
        END AS down_payment_amount,
    ( SELECT LEAST(COALESCE(sum(b2.amount), 0::double precision), COALESCE(sd.down_payment_amount, 0::numeric)::double precision) AS "least"
           FROM billing b2
          WHERE b2.case_id = cs.id AND b2.payment_status::text = 'COMPLETED'::text) AS down_payment_received,
    ctm.department,
    ctm.sub_directory,
        CASE
            WHEN sd.case_type_id IS NOT NULL THEN ctm.case_type
            ELSE sd.case_type
        END AS case_type,
    ctm.case_sub_type,
    ls.marketing_source,
    cn.case_number,
    ( SELECT LEAST(COALESCE(sum(b2.amount), 0::double precision), COALESCE(sd.down_payment_amount, 0::numeric)::double precision) AS "least"
           FROM billing b2
          WHERE b2.case_id = cs.id AND b2.payment_status::text = 'COMPLETED'::text) AS down_payment_total,
    ls.id AS jotform_submission_id,
    cs.created_at::date AS case_created_at,
        CASE
            WHEN cn.case_number_client_count <> 1 THEN 0::numeric
            ELSE sd.down_payment_amount
        END AS earliest_down_payment,
    false AS refund_request,
    NULL::character varying AS contract_status,
    rnc.name AS not_converted_reason,
    rfd.name AS reason_for_dnq,
        CASE
            WHEN ls.initial_submission_date IS NOT NULL THEN ls.initial_submission_date
            ELSE ls.form_filled_date
        END AS initial_submission_date,
    cd.time_stamp AS cd_timestamp,
    er."timestamp" AS er_timestamp,
    sd."timestamp" AS sd_timestamp
   FROM strategist_sales.strategist_decision sd
     JOIN ( SELECT sd_1.jotform_submission_id,
            max(sd_1.id) AS id,
            max(sd_1."timestamp") AS first_disposition_timestamp
           FROM strategist_sales.strategist_decision sd_1
          GROUP BY sd_1.jotform_submission_id) sd_max ON sd.id = sd_max.id
     LEFT JOIN intake.lucky_13_submission ls ON sd.jotform_submission_id = ls.id
     LEFT JOIN ( SELECT cd_1.jotform_submission_id,
            cd_1.time_stamp
           FROM screener_sales.consulta_details cd_1
             JOIN ( SELECT consulta_details.jotform_submission_id,
                    max(consulta_details.id) AS id,
                    max(consulta_details.time_stamp) AS first_disposition_timestamp
                   FROM screener_sales.consulta_details
                  GROUP BY consulta_details.jotform_submission_id) cd_max ON cd_1.id = cd_max.id) cd ON cd.jotform_submission_id = ls.id
     LEFT JOIN ( SELECT er_1.lucky_13_submission_id,
            er_1."timestamp"
           FROM strategist_sales.editor_review er_1
             JOIN ( SELECT editor_review.lucky_13_submission_id,
                    max(editor_review.id) AS id,
                    max(editor_review."timestamp") AS first_disposition_timestamp
                   FROM strategist_sales.editor_review
                  GROUP BY editor_review.lucky_13_submission_id) er_max ON er_1.id = er_max.id) er ON er.lucky_13_submission_id = ls.id
     LEFT JOIN strategist_sales.strategist_specialists_info ssi ON sd.consulted_by_id = ssi.strategist_id
     LEFT JOIN alonso_staff as2 ON sd.consulted_by_id = as2.id
     LEFT JOIN strategist_sales.status s ON sd.status_id = s.id
     LEFT JOIN client_list.case_numbers cn ON sd.id = cn.contract_id
     LEFT JOIN strategist_sales.case_types_mapping ctm ON cn.case_type_id = ctm.id
     LEFT JOIN cases_scraped cs ON cn.case_number::text = TRIM(BOTH '-'::text FROM cs.case_number_identifier)
     LEFT JOIN strategist_sales.reason_not_converted rnc ON sd.not_converted_reason = rnc.id
     LEFT JOIN strategist_sales.reason_for_dnq rfd ON sd.reason_for_dnq_id = rfd.id
  WHERE sd."timestamp" >= '2024-04-15 00:00:00-05'::timestamp with time zone AND sd."timestamp" <= '2024-07-28 00:00:00-05'::timestamp with time zone
UNION
 SELECT lie.id::character varying AS id,
    regexp_replace(lie.name, '[^0-9]'::text, ''::text, 'g'::text)::bigint AS intake_id,
        CASE
            WHEN lie.matter_id IS NOT NULL THEN ((lie.converted_date AT TIME ZONE 'utc'::text) AT TIME ZONE 'cst'::text)::date
            WHEN lie.consulta_scheduled_date IS NULL THEN ((lie.created_date AT TIME ZONE 'utc'::text) AT TIME ZONE 'cst'::text)::date
            ELSE ((lie.consulta_scheduled_date AT TIME ZONE 'utc'::text) AT TIME ZONE 'cst'::text)::date
        END AS consulta_scheduled_date,
    ((lie.contract_signing_date AT TIME ZONE 'utc'::text) AT TIME ZONE 'cst'::text)::date AS signing_date,
    lie.client_name,
    lie.client_phone AS phone_number,
    lie.strategy_specialist_name AS consultation_strategist_name,
    ssi.strategist_id,
    lie.strategy_specialist_email AS consultation_strategist_email,
    ssi.team_name AS consultation_strategist_team,
    ssi.office_location_name AS consultation_strategist_office_location,
        CASE
            WHEN lme.id IS NOT NULL THEN 1
            WHEN lie.dnq THEN 3
            WHEN lie.strategy_analyst_status = 'Informative Consultation'::text THEN 2
            WHEN lie.strategy_analyst_status = 'Missed Consultation'::text THEN 4
            WHEN lie.strategy_analyst_status = 'Not converted'::text THEN 7
            ELSE NULL::integer
        END AS consulta_status_id,
        CASE
            WHEN lme.id IS NOT NULL THEN 'Converted to contract'::text
            WHEN lie.dnq THEN 'Does not qualify'::text
            ELSE lie.strategy_analyst_status
        END AS consulta_status,
    lie.matter_total_contract_amount AS contract_price,
    lie.down_payment_total AS down_payment_amount,
    lie.down_payment_paid AS down_payment_received,
    lme.department,
    lme.sub_directory,
    regexp_replace(lme.case_sub_type, '\s\d{2}$'::text, ''::text) AS case_type,
    lme.case_sub_type,
    ls.marketing_source,
    (TRIM(BOTH FROM split_part(lme.display_name, ' - '::text, 1)) || ' - '::text) || TRIM(BOTH FROM split_part(lme.display_name, ' - '::text, 2)) AS case_number,
    lme.iolta_deposits_total AS down_payment_total,
        CASE
            WHEN ls.id IS NULL THEN ls2.id
            WHEN ls2.id IS NULL THEN TRIM(LEADING 'INT-'::text FROM 'INT-250111504311'::text)::bigint
            ELSE ls.id
        END AS jotform_submission_id,
        CASE
            WHEN ((lie.contract_signing_date AT TIME ZONE 'utc'::text) AT TIME ZONE 'cst'::text) < '2024-09-01 00:00:00'::timestamp without time zone AND lme.open_date < '2024-09-01'::date THEN ((lie.contract_signing_date AT TIME ZONE 'utc'::text) AT TIME ZONE 'cst'::text)::date
            ELSE ((lie.converted_date AT TIME ZONE 'utc'::text) AT TIME ZONE 'cst'::text)::date
        END AS case_created_at,
    COALESCE(lie.down_payment_paid, 0::numeric) AS earliest_down_payment,
    lie.refund_request,
    lie.contract_status,
    lie.not_converted_reason,
    lie.reason_for_dnq,
    lie.last_modified_date AS initial_submission_date,
    cd.time_stamp AS cd_timestamp,
    er."timestamp" AS er_timestamp,
    sd."timestamp" AS sd_timestamp
   FROM litify_intakes_extracted lie
     LEFT JOIN ( SELECT DISTINCT ON (lucky_13_submission.salesforce_intake_id) lucky_13_submission.id,
            lucky_13_submission.marketing_source,
            lucky_13_submission.salesforce_intake_id,
            lucky_13_submission.initial_submission_date,
            lucky_13_submission.form_filled_date
           FROM intake.lucky_13_submission
          ORDER BY lucky_13_submission.salesforce_intake_id, lucky_13_submission.form_filled_date) ls ON ls.salesforce_intake_id = SUBSTRING(lie.id FROM 1 FOR length(lie.id) - 3)
     LEFT JOIN ( SELECT DISTINCT ON (lucky_13_submission.salesforce_intake_id) lucky_13_submission.id,
            lucky_13_submission.marketing_source,
            lucky_13_submission.salesforce_intake_id
           FROM intake.lucky_13_submission
          ORDER BY lucky_13_submission.salesforce_intake_id, lucky_13_submission.form_filled_date) ls2 ON ls2.salesforce_intake_id = lie.id
     LEFT JOIN ( SELECT sd_1.jotform_submission_id,
            sd_1."timestamp"
           FROM strategist_sales.strategist_decision sd_1
             JOIN ( SELECT strategist_decision.jotform_submission_id,
                    max(strategist_decision.id) AS id,
                    max(strategist_decision."timestamp") AS first_disposition_timestamp
                   FROM strategist_sales.strategist_decision
                  GROUP BY strategist_decision.jotform_submission_id) sd_max ON sd_1.id = sd_max.id) sd ON sd.jotform_submission_id = ls.id
     LEFT JOIN ( SELECT cd_1.jotform_submission_id,
            cd_1.time_stamp
           FROM screener_sales.consulta_details cd_1
             JOIN ( SELECT consulta_details.jotform_submission_id,
                    max(consulta_details.id) AS id,
                    max(consulta_details.time_stamp) AS first_disposition_timestamp
                   FROM screener_sales.consulta_details
                  GROUP BY consulta_details.jotform_submission_id) cd_max ON cd_1.id = cd_max.id) cd ON cd.jotform_submission_id = ls.id
     LEFT JOIN ( SELECT er_1.lucky_13_submission_id,
            er_1."timestamp"
           FROM strategist_sales.editor_review er_1
             JOIN ( SELECT editor_review.lucky_13_submission_id,
                    max(editor_review.id) AS id,
                    max(editor_review."timestamp") AS first_disposition_timestamp
                   FROM strategist_sales.editor_review
                  GROUP BY editor_review.lucky_13_submission_id) er_max ON er_1.id = er_max.id) er ON er.lucky_13_submission_id = ls.id
     LEFT JOIN alonso_staff as2 ON lie.strategy_specialist_email = as2.work_email
     LEFT JOIN strategist_sales.strategist_specialists_info ssi ON as2.id = ssi.strategist_id
     LEFT JOIN litify_matters_extracted lme ON lme.id::text = lie.matter_id;