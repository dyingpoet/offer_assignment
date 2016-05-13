SET hive.exec.compress.output=false;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.parallel=true;

USE ${hiveconf:Database};

INSERT OVERWRITE TABLE ${hiveconf:member_club_pref_table} PARTITION(campaign_month='${hiveconf:campaign_month}')
            SELECT /*+ MAPJOIN(a) */ 
                DISTINCT
                a.membership_nbr as membership_nbr
                , c.cardholder_nbr as cardholder_nbr
                , b.membership_create_date as membership_create_date
                , IF(c.preferred_club_nbr IS NULL and b.assigned_club_nbr IS NULL, b.issuing_club_nbr, c.preferred_club_nbr) as preferred_club_nbr
                , b.issuing_club_nbr as issuing_club_nbr
                , b.assigned_club_nbr as assigned_club_nbr
            FROM 
                --pythia.offer_assigned_member_base a
                ${hiveconf:offer_assigned_member_base_table} a
            JOIN 
                --customers.sams_us_clubs_sams_membership_dim b
                ${hiveconf:sams_membership_dim_table} b
            ON (a.membership_nbr = b.membership_nbr and a.creation_date = b.membership_create_date and b.current_ind = 'Y' and a.campaign_month='${hiveconf:campaign_month}' and b.membership_obsolete_date>='${hiveconf:ds_obsolete_date_lb}')
            JOIN 
                --customers.sams_us_clubs_sams_mbr_cardholder_dim c
                ${hiveconf:sams_cardholder_dim_table} c
            ON (a.membership_nbr = c.membership_nbr and a.card_holder_nbr=c.cardholder_nbr and a.creation_date = c.membership_create_date and c.current_ind='Y')
            --where b.current_ind = 'Y'
;

INSERT OVERWRITE TABLE pythia.fia_subcat_item_club PARTITION(campaign_month='${hiveconf:campaign_month_type}')
SELECT
    a.cat_subcat_nbr as cat_subcat_nbr
    , a.system_item_nbr as system_item_nbr
    , a.value_coupon_nbr as value_coupon_nbr
    , b.club_nbr as club_nbr
FROM 
    --pythia.offer_assignment_fia a
    ${hiveconf:offer_assignment_fia_table} a
--join sams_us_clubs.club_item_inventory b
--on a.system_item_nbr = b.system_item_nbr
JOIN 
    --sams_us_clubs.club_item_inventory_history b
    ${hiveconf:club_item_inventory_history_table} b
ON (a.system_item_nbr = b.system_item_nbr and b.ds='{hiveconf:ds}')
WHERE a.campaign_month='${hiveconf:campaign_month_type}'
--and b.onsite_onhand_qty >0 and (b.status='A' or b.status='S')
and (b.status='A' or b.status='S')
;
