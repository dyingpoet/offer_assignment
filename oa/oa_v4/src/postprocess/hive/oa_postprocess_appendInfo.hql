--SET hive.exec.compress.output=false; --flat output
--SET mapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec;
set mapred.output.compress=true;
set hive.exec.compress.output=true;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
set io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.parallel=true;

-- to change 
-- 1. jli21.offerAssignment_2013Dec_v1 
-- 2. partition of customers.sams_us_clubs_member_xref_history
-- 3. sams_us_clubs.value_coupon_dim's campaign_nbr


USE ${hiveconf:Database};

INSERT OVERWRITE TABLE ${hiveconf:task_table} PARTITION (segment='${hiveconf:OApartition}')
SELECT
    '${hiveconf:CampaignNbr}'
    , NULL
    , NULL
    , memberInfo.household_id
    , memberInfo.individual_id
    , memberInfo.issuing_country_code
    , lpad(memberInfo.membership_nbr,9,'0') -- 9 digit
    , concat(substr(lpad(memberInfo.membership_nbr,9,'0'),8,2), substr(lpad(memberInfo.membership_nbr,9,'0'),3,4), substr(lpad(memberInfo.membership_nbr,9,'0'),1,3)) --member[7:9] + member[3:7] + member[0:3]
    , memberInfo.cardholder_nbr
    , memberInfo.membership_create_date
    , couponInfo.*
    , '${hiveconf:ScoreSource}'
    , NULL --"%8f"
    , NULL
    , 1
    , NULL
FROM
(
    SELECT
        b.household_id
        , b.individual_id
        , cast(a.membership_nbr AS string) AS membership_nbr 
        , a.cardholder_nbr
        , a.membership_create_date
        , a.value_coupon_nbr
        , a.issuing_country_code
    FROM
        (SELECT a.*, b.issuing_country_code FROM ${hiveconf:OATable} a LEFT OUTER JOIN ${hiveconf:SamsMembershipDimTbl} b ON a.membership_nbr = b.membership_nbr AND a.membership_create_date=b.membership_create_date where b.current_ind='Y' and a.campaign_month='${hiveconf:OApartition}') a
    LEFT OUTER JOIN
        --customers.sams_us_clubs_member_xref_history b
        --customers.sams_us_clubs_member_xref b
        ${hiveconf:SamsMemberXrefTbl} b
    ON
        a.membership_nbr = b.membership_nbr
    AND a.cardholder_nbr = b.card_holder_nbr
    --WHERE b.ds = '${hiveconf:CustomersPartition}'
) memberInfo
JOIN
(
    SELECT
        c.value_coupon_nbr
        , 1 AS treatment_type    
        , c.val_offer_type_desc --c.offer_type
        , c.val_assign_type_desc --c.assignment_type
        , c.package_desc --c.package_code
        --, c.coupon_system_item_nbr --c.item_nbr
        , d.system_item_nbr
        --, d.customer_item_nbr --d.is_item_nbr
        , c.prvdr_coupon_nbr -- the coupon_item_nbr is not available in Tables c or d
        , c.start_date
        , c.end_date
    FROM
        --sams_us_clubs.value_coupon_dim c
        ${hiveconf:SamsValueCouponDimTbl} c
    JOIN
        --sams_us_clubs.value_coupon_item_dim d
        ${hiveconf:SamsValueCouponItemDimTbl} d
    ON c.value_coupon_nbr = d.value_coupon_nbr
    WHERE c.campaign_nbr = ${hiveconf:CampaignNbr}
) couponInfo
ON
    memberInfo.value_coupon_nbr = couponInfo.value_coupon_nbr


