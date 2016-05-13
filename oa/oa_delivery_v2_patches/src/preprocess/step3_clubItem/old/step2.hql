SET hive.exec.compress.output=false;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.parallel=true;

use pythia;


INSERT OVERWRITE TABLE ${hiveconf:task_table} PARTITION (campaign_month = '${hiveconf:campaign_month_type}')
select
a.*
, b.score
, b.anchor_cat_subcat_nbr
from
(
    select distinct
       a.membership_nbr as membership_nbr
       , cardholder_nbr as cardholder_nbr
       , a.membership_create_date as membership_create_date
       , a.cat_subcat_nbr as cat_subcat_nbr
       , a.system_item_nbr as system_item_nbr
       , a.value_coupon_nbr as value_coupon_nbr
    from
    (
        select distinct
            a.membership_nbr as membership_nbr
            , a.cardholder_nbr as cardholder_nbr
            , a.membership_create_date as membership_create_date
            , b.cat_subcat_nbr as cat_subcat_nbr
            , b.system_item_nbr as system_item_nbr
            , b.value_coupon_nbr as value_coupon_nbr
        from
        pythia.offer_assigned_member_base_w_club_preference a
        join
        pythia.fia_subcat_item_club b 
        --on a.preferred_club_nbr = b.club_nbr or a.assigned_club_nbr = b.club_nbr
        on (a.preferred_club_nbr = b.club_nbr and a.campaign_month='${hiveconf:campaign_month}' and b.campaign_month='${hiveconf:campaign_month_type}')
    
        UNION ALL
    
        select distinct
            a.membership_nbr as membership_nbr
            , a.cardholder_nbr as cardholder_nbr
            , a.membership_create_date as membership_create_date
            , b.cat_subcat_nbr as cat_subcat_nbr
            , b.system_item_nbr as system_item_nbr
            , b.value_coupon_nbr as value_coupon_nbr
        from
        pythia.offer_assigned_member_base_w_club_preference a
        join
        pythia.fia_subcat_item_club b 
        --on a.preferred_club_nbr = b.club_nbr or a.assigned_club_nbr = b.club_nbr
        on (a.assigned_club_nbr = b.club_nbr and a.campaign_month='${hiveconf:campaign_month}' and b.campaign_month='${hiveconf:campaign_month_type}')
    ) a
) a
--join pythia.recommendation_score_debug b
join ${hiveconf:reco_table} b
on (a.membership_nbr = b.membership_nbr and a.cardholder_nbr = b.cardholder_nbr and a.membership_create_date = b.membership_create_date 
and a.cat_subcat_nbr = b.cat_subcat_nbr and b.campaign_month='${hiveconf:campaign_month_type}')
;




