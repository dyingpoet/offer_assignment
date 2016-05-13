SET hive.exec.compress.output=false;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.parallel=true;



--INSERT OVERWRITE TABLE pythia.offer_assigned_member_base_w_club_preference PARTITION(campaign_month='${hiveconf:campaign_month}')
--            select /*+ MAPJOIN(a) */ 
--                distinct
--                a.membership_nbr as membership_nbr
--                , c.cardholder_nbr as cardholder_nbr
--                , b.membership_create_date as membership_create_date
--                , IF(c.preferred_club_nbr IS NULL and b.assigned_club_nbr IS NULL, b.issuing_club_nbr, c.preferred_club_nbr) as preferred_club_nbr
--                , b.issuing_club_nbr as issuing_club_nbr
--                , b.assigned_club_nbr as assigned_club_nbr
--            from pythia.offer_assigned_member_base a
--            join customers.sams_us_clubs_sams_membership_dim b
--            on (a.membership_nbr = b.membership_nbr and a.creation_date = b.membership_create_date and b.current_ind = 'Y' and a.campaign_month='${hiveconf:campaign_month}' and b.membership_obsolete_date>='2014-03-14')
--            join customers.sams_us_clubs_sams_mbr_cardholder_dim c
--            on (a.membership_nbr = c.membership_nbr and a.card_holder_nbr=c.cardholder_nbr and a.creation_date = c.membership_create_date and c.current_ind='Y')
--            --where b.current_ind = 'Y'
--;

--INSERT OVERWRITE TABLE pythia.fia_subcat_item_club PARTITION(campaign_month='${hiveconf:campaign_month_type}')
--select
--    a.cat_subcat_nbr as cat_subcat_nbr
--    , a.system_item_nbr as system_item_nbr
--    , a.value_coupon_nbr as value_coupon_nbr
--    , b.club_nbr as club_nbr
select count(*)
from pythia.offer_assignment_fia a
join sams_us_clubs.club_item_inventory_history b
on (a.system_item_nbr = b.system_item_nbr and b.ds='2014-03-20')
where a.campaign_month='${hiveconf:campaign_month_type}'
and b.onsite_onhand_qty >0 and (b.status='A' or b.status='S')
;
