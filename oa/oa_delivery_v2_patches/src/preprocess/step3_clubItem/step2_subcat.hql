SET hive.exec.compress.output=false;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.parallel=true;

USE ${hiveconf:Database};


INSERT OVERWRITE TABLE ${hiveconf:task_table} PARTITION (campaign_month = '${hiveconf:campaign_month_type}')
SELECT
a.*
, b.score
--, b.anchor_cat_subcat_nbr
, '0000'
FROM
(
    SELECT DISTINCT
       a.membership_nbr as membership_nbr
       , a.cardholder_nbr as cardholder_nbr
       , a.membership_create_date as membership_create_date
       , a.cat_subcat_nbr as cat_subcat_nbr
       , a.system_item_nbr as system_item_nbr
       , a.value_coupon_nbr as value_coupon_nbr
    FROM
    (
        SELECT DISTINCT
            a.membership_nbr as membership_nbr
            , a.cardholder_nbr as cardholder_nbr
            , a.membership_create_date as membership_create_date
            , b.cat_subcat_nbr as cat_subcat_nbr
            , b.system_item_nbr as system_item_nbr
            , b.value_coupon_nbr as value_coupon_nbr
        FROM
        ${hiveconf:member_club_pref_table} a
        --pythia.offer_assigned_member_base_w_club_preference a
        JOIN
        ${hiveconf:subcat_item_club_table} b
        --pythia.fia_subcat_item_club b 
        --on a.preferred_club_nbr = b.club_nbr or a.assigned_club_nbr = b.club_nbr
        ON (a.preferred_club_nbr = b.club_nbr and a.campaign_month='${hiveconf:campaign_month}' and b.campaign_month='${hiveconf:campaign_month_type}')
    
        UNION ALL
    
        SELECT DISTINCT
            a.membership_nbr as membership_nbr
            , a.cardholder_nbr as cardholder_nbr
            , a.membership_create_date as membership_create_date
            , b.cat_subcat_nbr as cat_subcat_nbr
            , b.system_item_nbr as system_item_nbr
            , b.value_coupon_nbr as value_coupon_nbr
        FROM
        ${hiveconf:member_club_pref_table} a
        --pythia.offer_assigned_member_base_w_club_preference a
        JOIN
        ${hiveconf:subcat_item_club_table} b
        --pythia.fia_subcat_item_club b 
        --on a.preferred_club_nbr = b.club_nbr or a.assigned_club_nbr = b.club_nbr
        ON (a.assigned_club_nbr = b.club_nbr and a.campaign_month='${hiveconf:campaign_month}' and b.campaign_month='${hiveconf:campaign_month_type}')
    ) a
) a
--join pythia.recommendation_score_debug b
JOIN ${hiveconf:reco_table} b
ON (a.membership_nbr = b.membership_nbr and a.cardholder_nbr = b.cardholder_nbr and a.membership_create_date = b.membership_create_date 
--and a.cat_subcat_nbr = b.cat_subcat_nbr and b.campaign_month='${hiveconf:campaign_month_type}')
and a.system_item_nbr = b.system_item_nbr and b.campaign_month='${hiveconf:campaign_month_type}')
;




