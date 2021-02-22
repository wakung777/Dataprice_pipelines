
------------- t_order -------------
	
insert into t_order (t_order_id ,t_visit_id ,b_item_id ,order_staff_verify ,order_verify_date_time ,order_staff_execute ,order_executed_date_time ,order_staff_discontinue ,order_discontinue_date_time ,order_staff_dispense ,order_dispense_date_time ,order_service_point ,f_item_group_id ,order_charge_complete ,f_order_status_id ,order_secret ,order_continue ,order_price ,order_qty ,b_item_billing_subgroup_id ,t_patient_id ,b_item_subgroup_id ,order_common_name ,order_refer_out ,order_request ,order_staff_order ,order_date_time ,order_complete ,order_staff_report ,order_report_date_time ,order_cost ,order_notice ,order_cause_cancel_resultlab ,order_drug_allergy ,order_home ,b_item_16_group_id ,serial_number 
,print_mar_type ,tpucode ,drugcode24 ,order_price_type ,order_by_doctor ,order_by_doctor_type ,order_xray_film_price ,order_share_doctor ,order_share_hospital ,order_editable_price ,order_edited_price ,order_limit_price_min ,order_limit_price_max ,order_urgent_status ,order_dx_item_risk ,order_dx_item_risk_reason) 
select '195' ||to_char(current_timestamp, 'YYYYMMDDHH24MISS')||rpad(row_number() OVER (partition by t_visit.t_visit_id,t_order.b_item_id)::text,9,trunc(random()*10^10)::text) as t_order_id
    ,t_visit.t_visit_id
    ,t_order.b_item_id as b_item_id
    ,t_order.order_staff_verify as order_staff_verify
    ,current_timestamp as order_verify_date_time
    ,null as order_staff_execute
    ,null as order_executed_date_time
    ,null as order_staff_discontinue
    ,null as order_discontinue_date_time
    ,null as order_staff_dispense
    ,null as order_dispense_date_time
    ,t_order.order_service_point as order_service_point
    ,t_order.f_item_group_id as f_item_group_id
    ,'0' as order_charge_complete
    ,'1' as f_order_status_id
    ,t_order.order_secret as order_secret
    ,'0' as order_continue
    ,t_order.order_price as order_price
    ,t_order.order_qty as order_qty
    ,t_order.b_item_billing_subgroup_id as b_item_billing_subgroup_id
    ,t_order.t_patient_id as t_patient_id
    ,t_order.b_item_subgroup_id as b_item_subgroup_id
    ,t_order.order_common_name as order_common_name
    ,'0' as order_refer_out
    ,'0' as order_request
    ,t_order.order_staff_order as order_staff_order
    ,current_timestamp as order_date_time
    ,'0' as order_complete
    ,null as order_staff_report
    ,null as order_report_date_time
    ,t_order.order_cost as order_cost
    ,null as order_notice
    ,null as order_cause_cancel_resultlab
    ,t_order.order_drug_allergy as order_drug_allergy
    ,'0' as order_home
    ,t_order.b_item_16_group_id as b_item_16_group_id
    ,'' as serial_number
    ,t_order.print_mar_type as print_mar_type
    ,t_order.tpucode as tpucode
    ,t_order.drugcode24 as drugcode24
    ,t_order.order_price_type as order_price_type
    ,t_order.order_by_doctor as order_by_doctor
    ,t_order.order_by_doctor_type as order_by_doctor_type
    ,t_order.order_xray_film_price as order_xray_film_price
    ,t_order.order_share_doctor as order_share_doctor
    ,t_order.order_share_hospital as order_share_hospital
    ,'0' as order_editable_price
    ,'0' as order_edited_price
    ,t_order.order_limit_price_min as order_limit_price_min
    ,t_order.order_limit_price_max as order_limit_price_max
    ,'0' as order_urgent_status
    ,t_order.order_dx_item_risk as order_dx_item_risk
    ,t_order.order_dx_item_risk_reason as order_dx_item_risk_reason
from t_visit
    inner join t_order on t_visit.t_visit_id = t_order.t_visit_id
            and t_order.f_order_status_id not in ('0','3')
            and t_order.order_continue = '1'
            and t_order.f_item_group_id not in ('2','3')
    inner join t_order_continue on t_order.t_order_id = t_order_continue.t_order_id
where t_visit.f_visit_status_id = '1'  
and t_visit.f_visit_type_id = '1'  
and t_visit.visit_ipd_discharge_status <> '1'
order by  t_visit.visit_begin_admit_date_time
    ,t_order.order_verify_date_time;

------------- t_order_drug -------------

insert into t_order_drug (t_order_drug_id ,t_order_id ,b_item_drug_instruction_id ,b_item_drug_frequency_id ,order_drug_dose ,b_item_drug_uom_id_use ,order_drug_caution ,order_drug_description ,f_item_day_time_id ,order_drug_printable ,b_item_drug_uom_id_purch ,order_drug_special_prescription ,order_drug_special_prescription_text ,b_item_id ,order_drug_active ,order_drug_modifier ,order_drug_modify_datetime ,order_drug_order_status ,b_item_manufacturer_id ,b_item_distributor_id ,pregnancy_category ,print_equal_quantity , drug_sticker_quantity , drug_stat_status)
select '196' ||to_char(current_timestamp, 'YYYYMMDDHH24MISS')||rpad(row_number() OVER (partition by t_visit.t_visit_id,t_order.b_item_id)::text,9,trunc(random()*10^10)::text) as t_order_drug_id
    ,drug_continue.t_order_id as t_order_id
    ,t_order_drug.b_item_drug_instruction_id as b_item_drug_instruction_id
    ,t_order_drug.b_item_drug_frequency_id as b_item_drug_frequency_id
    ,t_order_drug.order_drug_dose as order_drug_dose
    ,t_order_drug.b_item_drug_uom_id_use as b_item_drug_uom_id_use
    ,t_order_drug.order_drug_caution as order_drug_caution
    ,t_order_drug.order_drug_description as order_drug_description
    ,t_order_drug.f_item_day_time_id as f_item_day_time_id
    ,t_order_drug.order_drug_printable as order_drug_printable
    ,t_order_drug.b_item_drug_uom_id_purch as b_item_drug_uom_id_purch
    ,t_order_drug.order_drug_special_prescription as order_drug_special_prescription
    ,t_order_drug.order_drug_special_prescription_text as order_drug_special_prescription_text
    ,t_order_drug.b_item_id as b_item_id
    ,t_order_drug.order_drug_active as order_drug_active
    ,t_order_drug.order_drug_modifier as order_drug_modifier
    ,t_order_drug.order_drug_modify_datetime as order_drug_modify_datetime
    ,'1' as order_drug_order_status
    ,t_order_drug.b_item_manufacturer_id as b_item_manufacturer_id
    ,t_order_drug.b_item_distributor_id as b_item_distributor_id
    ,t_order_drug.pregnancy_category as pregnancy_category
    ,t_order_drug.print_equal_quantity as print_equal_quantity
    ,t_order_drug.drug_sticker_quantity as drug_sticker_quantity
    ,'0' as drug_stat_status
from t_visit
    inner join t_order on t_visit.t_visit_id = t_order.t_visit_id
            and t_order.f_order_status_id not in ('0','3')
            and t_order.order_continue = '1'
    inner join t_order_drug on t_order.t_order_id = t_order_drug.t_order_id
            and t_order_drug.order_drug_active = '1'
    inner join t_order_continue on t_order.t_order_id = t_order_continue.t_order_id
    inner join (select t_order.t_order_id
                    ,t_visit.t_visit_id
                    ,t_order.b_item_id
                from t_visit
                    inner join t_order on t_visit.t_visit_id = t_order.t_visit_id
                            and t_order.f_order_status_id not in ('0','3')
                            and t_order.f_item_group_id = '1'
                    left join t_order_drug on t_order.t_order_id = t_order_drug.t_order_id
                where t_visit.f_visit_status_id = '1'  
                and t_visit.f_visit_type_id = '1'  
                and t_visit.visit_ipd_discharge_status <> '1'
                and t_order_drug.t_order_drug_id is null
               ) as drug_continue on drug_continue.t_visit_id = t_visit.t_visit_id
                                  and drug_continue.b_item_id = t_order.b_item_id
where t_visit.f_visit_status_id = '1'  
and t_visit.f_visit_type_id = '1'  
and t_visit.visit_ipd_discharge_status <> '1'
order by  t_visit.visit_begin_admit_date_time
    ,t_order.order_verify_date_time;

------------- t_order_ned -------------

INSERT INTO t_order_ned (t_order_ned_id, t_order_id, f_ned_reason_id, other_reason, user_record_id, record_date_time) 
select '988' ||to_char(current_timestamp, 'YYYYMMDDHH24MISS')||rpad(row_number() OVER (partition by t_visit.t_visit_id,t_order.b_item_id)::text,9,trunc(random()*10^10)::text) as t_order_ned_id
    ,order_ned.t_order_id as t_order_id
    ,t_order_ned.f_ned_reason_id as f_ned_reason_id
    ,t_order_ned.other_reason as other_reason
    ,t_order_ned.user_record_id as user_record_id
    ,current_timestamp as record_date_time
from t_visit
    inner join t_order on t_visit.t_visit_id = t_order.t_visit_id
            and t_order.f_order_status_id not in ('0','3')
            and t_order.order_continue = '1'
    inner join t_order_ned on t_order.t_order_id = t_order_ned.t_order_id
    inner join t_order_continue on t_order.t_order_id = t_order_continue.t_order_id
    inner join (select t_order.t_order_id
                    ,t_visit.t_visit_id
                    ,t_order.b_item_id
                from t_visit
                    inner join t_order on t_visit.t_visit_id = t_order.t_visit_id
                            and t_order.f_order_status_id not in ('0','3')
                    inner join b_map_ned on b_map_ned.b_item_subgroup_id = t_order.b_item_subgroup_id
                    left join t_order_ned on t_order.t_order_id = t_order_ned.t_order_id
                where t_visit.f_visit_status_id = '1'  
                and t_visit.f_visit_type_id = '1'  
                and t_visit.visit_ipd_discharge_status <> '1'
                and t_order_ned.t_order_ned_id is null
               ) as order_ned on order_ned.t_visit_id = t_visit.t_visit_id
                                  and order_ned.b_item_id = t_order.b_item_id
where t_visit.f_visit_status_id = '1'  
and t_visit.f_visit_type_id = '1'  
and t_visit.visit_ipd_discharge_status <> '1'
order by  t_visit.visit_begin_admit_date_time
    ,t_order.order_verify_date_time;

------------- t_order_due -------------

INSERT INTO t_order_due (t_order_due_id, t_order_id, map_due_type, b_due_type_detail_id, evaluate_detail, user_record_id, record_date_time)
select '989' ||to_char(current_timestamp, 'YYYYMMDDHH24MISS')||rpad(row_number() OVER (partition by t_visit.t_visit_id,t_order.b_item_id)::text,9,trunc(random()*10^10)::text) as t_order_due_id
    ,order_due.t_order_id as t_order_id
    ,t_order_due.map_due_type as map_due_type
    ,t_order_due.b_due_type_detail_id as b_due_type_detail_id
    ,t_order_due.evaluate_detail as evaluate_detail
    ,t_order_due.user_record_id as user_record_id
    ,current_timestamp as record_date_time
from t_visit
    inner join t_order on t_visit.t_visit_id = t_order.t_visit_id
            and t_order.f_order_status_id not in ('0','3')
            and t_order.order_continue = '1'
    inner join t_order_due on t_order.t_order_id = t_order_due.t_order_id
    inner join t_order_continue on t_order.t_order_id = t_order_continue.t_order_id
    inner join (select t_order.t_order_id
                    ,t_visit.t_visit_id
                    ,t_order.b_item_id
                from t_visit
                    inner join t_order on t_visit.t_visit_id = t_order.t_visit_id
                            and t_order.f_order_status_id not in ('0','3')
                    inner join b_map_due on b_map_due.b_item_id = t_order.b_item_id
                    left join t_order_due on t_order.t_order_id = t_order_due.t_order_id
                where t_visit.f_visit_status_id = '1'  
                and t_visit.f_visit_type_id = '1'  
                and t_visit.visit_ipd_discharge_status <> '1'
                and t_order_due.t_order_due_id is null
               ) as order_due on order_due.t_visit_id = t_visit.t_visit_id
                                  and order_due.b_item_id = t_order.b_item_id
where t_visit.f_visit_status_id = '1'  
and t_visit.f_visit_type_id = '1'  
and t_visit.visit_ipd_discharge_status <> '1'
order by  t_visit.visit_begin_admit_date_time
    ,t_order.order_verify_date_time;

------------- t_order_drug_interaction -------------
	
insert into t_order_drug_interaction (t_order_drug_interaction_id ,order_item_id ,order_item_drug_standard_id ,order_item_drug_standard_description ,interaction_item_id ,interaction_item_drug_standard_id ,interaction_item_drug_standard_description ,interaction_blood_presure ,interaction_pregnant ,interaction_type ,interaction_force ,interaction_act ,interaction_repair ,order_drug_interaction_active ,t_visit_id )
select '286' ||to_char(current_timestamp, 'YYYYMMDDHH24MISS')
        ||rpad(row_number() OVER (partition by item_drug_interaction.t_visit_id,item_drug_interaction.order_item_id)::text,9,trunc(random()*10^10)::text) as t_order_drug_interaction_id
    ,item_drug_interaction.order_item_id
    ,item_drug_interaction.order_item_drug_standard_id
    ,item_drug_interaction.order_item_drug_standard_description
    ,item_drug_interaction.interaction_item_id
    ,item_drug_interaction.interaction_item_drug_standard_id
    ,item_drug_interaction.interaction_item_drug_standard_description
    ,item_drug_interaction.interaction_blood_presure
    ,item_drug_interaction.interaction_pregnant
    ,item_drug_interaction.interaction_type
    ,item_drug_interaction.interaction_force
    ,item_drug_interaction.interaction_act
    ,item_drug_interaction.interaction_repair
    ,item_drug_interaction.order_drug_interaction_active
    ,item_drug_interaction.t_visit_id
from (select original_order.t_order_id as order_item_id
        ,b_item_drug_interaction.drug_standard_original_id as order_item_drug_standard_id
        ,drug_standard_original.item_drug_standard_description as order_item_drug_standard_description
        ,interaction_order.t_order_id as interaction_item_id
        ,b_item_drug_interaction.drug_standard_interaction_id as interaction_item_drug_standard_id
        ,drug_standard_interaction.item_drug_standard_description as interaction_item_drug_standard_description
        ,b_item_drug_interaction.item_drug_interaction_blood_presure as interaction_blood_presure
        ,b_item_drug_interaction.item_drug_interaction_pregnant as interaction_pregnant
        ,b_item_drug_interaction.item_drug_interaction_type_id as interaction_type
        ,b_item_drug_interaction.item_drug_interaction_force as interaction_force
        ,b_item_drug_interaction.item_drug_interaction_act as interaction_act
        ,b_item_drug_interaction.item_drug_interaction_repair as interaction_repair
        ,'1' as order_drug_interaction_active
        ,t_visit.t_visit_id
        ,t_visit.visit_begin_admit_date_time
        ,original_order.order_verify_date_time
    from t_visit
        inner join (select t_order.t_order_id as order_original_id
                        ,t_visit.t_visit_id
                        ,t_order.b_item_id as item_original_id
                        ,original_item.b_item_interaction_id as item_interaction_id
                    from t_visit
                        inner join t_order on t_visit.t_visit_id = t_order.t_visit_id
                                          and t_order.f_order_status_id not in ('0','3')
                        inner join b_item_drug_interaction_detail as original_item on original_item.b_item_original_id = t_order.b_item_id
                        left join t_order_drug_interaction on t_order.t_order_id = t_order_drug_interaction.order_item_id
                    where t_visit.f_visit_status_id = '1'  
                        and t_visit.f_visit_type_id = '1'  
                        and t_visit.visit_ipd_discharge_status <> '1'
                        and t_order_drug_interaction.order_item_id is null
                    group by t_order.t_order_id
                        ,t_visit.t_visit_id
                        ,t_order.b_item_id
                        ,original_item.b_item_interaction_id
                    ) as drug_interaction on drug_interaction.t_visit_id = t_visit.t_visit_id
        inner join t_order as original_order on t_visit.t_visit_id = original_order.t_visit_id
                and drug_interaction.item_original_id = original_order.b_item_id
                and drug_interaction.order_original_id = original_order.t_order_id
                and original_order.f_order_status_id not in ('0','3')
        inner join (select t_visit.t_visit_id
                        ,t_order.b_item_id
                        ,t_order.t_order_id
                    from t_visit
                        inner join t_order on t_visit.t_visit_id = t_order.t_visit_id
                                           and t_order.f_order_status_id not in ('0','3')
                        inner join (select t_visit.t_visit_id
                                        ,t_order.b_item_id
                                        ,min(t_order.order_verify_date_time) as order_verify_date_time
                                    from t_visit
                                        inner join t_order on t_visit.t_visit_id = t_order.t_visit_id
                                                           and t_order.f_order_status_id not in ('0','3')
                                    where t_visit.f_visit_status_id = '1'  
                                        and t_visit.f_visit_type_id = '1'  
                                        and t_visit.visit_ipd_discharge_status <> '1'
                                    group by t_visit.t_visit_id
                                        ,t_order.b_item_id
                                    ) as min_datetime on min_datetime.t_visit_id = t_visit.t_visit_id
                                                     and min_datetime.b_item_id = t_order.b_item_id
                                                     and min_datetime.order_verify_date_time = t_order.order_verify_date_time
                    where t_visit.f_visit_status_id = '1'  
                        and t_visit.f_visit_type_id = '1'  
                        and t_visit.visit_ipd_discharge_status <> '1'
            ) as interaction_order on t_visit.t_visit_id = interaction_order.t_visit_id
                and drug_interaction.item_interaction_id = interaction_order.b_item_id
        inner join b_item_drug_interaction_detail on b_item_drug_interaction_detail.b_item_original_id = original_order.b_item_id
                                and b_item_drug_interaction_detail.b_item_interaction_id = interaction_order.b_item_id
        inner join b_item_drug_interaction on b_item_drug_interaction_detail.b_item_drug_interaction_id
                                                = b_item_drug_interaction.b_item_drug_interaction_id
        inner join b_item_drug_standard as drug_standard_original on drug_standard_original.b_item_drug_standard_id
                                                                       = b_item_drug_interaction.drug_standard_original_id
        inner join b_item_drug_standard as drug_standard_interaction on drug_standard_interaction.b_item_drug_standard_id
                                                                       = b_item_drug_interaction.drug_standard_interaction_id
    where t_visit.f_visit_status_id = '1'  
    and t_visit.f_visit_type_id = '1'  
    and t_visit.visit_ipd_discharge_status <> '1'
    group by t_visit.t_visit_id
        ,original_order.t_order_id
        ,interaction_order.t_order_id
        ,b_item_drug_interaction.drug_standard_original_id
        ,drug_standard_original.item_drug_standard_description
        ,b_item_drug_interaction.drug_standard_interaction_id
        ,drug_standard_interaction.item_drug_standard_description
        ,b_item_drug_interaction.item_drug_interaction_blood_presure
        ,b_item_drug_interaction.item_drug_interaction_pregnant
        ,b_item_drug_interaction.item_drug_interaction_type_id
        ,b_item_drug_interaction.item_drug_interaction_force
        ,b_item_drug_interaction.item_drug_interaction_act
        ,b_item_drug_interaction.item_drug_interaction_repair
        ,t_visit.visit_begin_admit_date_time
        ,original_order.order_verify_date_time
    union
    select interaction_order.t_order_id as order_item_id
        ,b_item_drug_interaction.drug_standard_interaction_id as order_item_drug_standard_id
        ,drug_standard_interaction.item_drug_standard_description as order_item_drug_standard_description
        ,original_order.t_order_id as interaction_item_id
        ,b_item_drug_interaction.drug_standard_original_id as interaction_item_drug_standard_id
        ,drug_standard_original.item_drug_standard_description as interaction_item_drug_standard_description
        ,b_item_drug_interaction.item_drug_interaction_blood_presure as interaction_blood_presure
        ,b_item_drug_interaction.item_drug_interaction_pregnant as interaction_pregnant
        ,b_item_drug_interaction.item_drug_interaction_type_id as interaction_type
        ,b_item_drug_interaction.item_drug_interaction_force as interaction_force
        ,b_item_drug_interaction.item_drug_interaction_act as interaction_act
        ,b_item_drug_interaction.item_drug_interaction_repair as interaction_repair
        ,'1' as order_drug_interaction_active
        ,t_visit.t_visit_id
        ,t_visit.visit_begin_admit_date_time
        ,original_order.order_verify_date_time
    from t_visit
        inner join (select t_order.t_order_id as order_original_id
                        ,t_visit.t_visit_id
                        ,t_order.b_item_id as item_original_id
                        ,original_item.b_item_interaction_id as item_interaction_id
                    from t_visit
                        inner join t_order on t_visit.t_visit_id = t_order.t_visit_id
                                          and t_order.f_order_status_id not in ('0','3')
                        inner join b_item_drug_interaction_detail as original_item on original_item.b_item_original_id = t_order.b_item_id
                        left join t_order_drug_interaction on t_order.t_order_id = t_order_drug_interaction.order_item_id
                    where t_visit.f_visit_status_id = '1'  
                        and t_visit.f_visit_type_id = '1'  
                        and t_visit.visit_ipd_discharge_status <> '1'
                        and t_order_drug_interaction.order_item_id is null
                    group by t_order.t_order_id
                        ,t_visit.t_visit_id
                        ,t_order.b_item_id
                        ,original_item.b_item_interaction_id
                    ) as drug_interaction on drug_interaction.t_visit_id = t_visit.t_visit_id
        inner join t_order as original_order on t_visit.t_visit_id = original_order.t_visit_id
                and drug_interaction.item_original_id = original_order.b_item_id
                and drug_interaction.order_original_id = original_order.t_order_id
                and original_order.f_order_status_id not in ('0','3')
        inner join (select t_visit.t_visit_id
                        ,t_order.b_item_id
                        ,t_order.t_order_id
                    from t_visit
                        inner join t_order on t_visit.t_visit_id = t_order.t_visit_id
                                           and t_order.f_order_status_id not in ('0','3')
                        inner join (select t_visit.t_visit_id
                                        ,t_order.b_item_id
                                        ,min(t_order.order_verify_date_time) as order_verify_date_time
                                    from t_visit
                                        inner join t_order on t_visit.t_visit_id = t_order.t_visit_id
                                                           and t_order.f_order_status_id not in ('0','3')
                                    where t_visit.f_visit_status_id = '1'  
                                        and t_visit.f_visit_type_id = '1'  
                                        and t_visit.visit_ipd_discharge_status <> '1'
                                    group by t_visit.t_visit_id
                                        ,t_order.b_item_id
                                    ) as min_datetime on min_datetime.t_visit_id = t_visit.t_visit_id
                                                     and min_datetime.b_item_id = t_order.b_item_id
                                                     and min_datetime.order_verify_date_time = t_order.order_verify_date_time
                    where t_visit.f_visit_status_id = '1'  
                        and t_visit.f_visit_type_id = '1'  
                        and t_visit.visit_ipd_discharge_status <> '1'
            ) as interaction_order on t_visit.t_visit_id = interaction_order.t_visit_id
                and drug_interaction.item_interaction_id = interaction_order.b_item_id
        inner join b_item_drug_interaction_detail on b_item_drug_interaction_detail.b_item_original_id = original_order.b_item_id
                                and b_item_drug_interaction_detail.b_item_interaction_id = interaction_order.b_item_id
        inner join b_item_drug_interaction on b_item_drug_interaction_detail.b_item_drug_interaction_id
                                                = b_item_drug_interaction.b_item_drug_interaction_id
        inner join b_item_drug_standard as drug_standard_original on drug_standard_original.b_item_drug_standard_id
                                                                       = b_item_drug_interaction.drug_standard_original_id
        inner join b_item_drug_standard as drug_standard_interaction on drug_standard_interaction.b_item_drug_standard_id
                                                                       = b_item_drug_interaction.drug_standard_interaction_id
    where t_visit.f_visit_status_id = '1'  
    and t_visit.f_visit_type_id = '1'  
    and t_visit.visit_ipd_discharge_status <> '1'
    group by t_visit.t_visit_id
        ,original_order.t_order_id
        ,interaction_order.t_order_id
        ,b_item_drug_interaction.drug_standard_original_id
        ,drug_standard_original.item_drug_standard_description
        ,b_item_drug_interaction.drug_standard_interaction_id
        ,drug_standard_interaction.item_drug_standard_description
        ,b_item_drug_interaction.item_drug_interaction_blood_presure
        ,b_item_drug_interaction.item_drug_interaction_pregnant
        ,b_item_drug_interaction.item_drug_interaction_type_id
        ,b_item_drug_interaction.item_drug_interaction_force
        ,b_item_drug_interaction.item_drug_interaction_act
        ,b_item_drug_interaction.item_drug_interaction_repair
        ,t_visit.visit_begin_admit_date_time
        ,original_order.order_verify_date_time
    ) as item_drug_interaction
order by  item_drug_interaction.visit_begin_admit_date_time
    ,item_drug_interaction.order_verify_date_time;