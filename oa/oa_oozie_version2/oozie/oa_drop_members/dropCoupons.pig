-----------------------------------------------------------------------------------------
--  Process Name : Offer_Assignment_Automation
--  Script Name  : dropCoupons.pig
--  Description  : This Pig script will drop the offers assigned.
--
--  Inputs       : 1. member_sc_cpn: member_sc_cpn file
--               : 2. fia_offers_file_refresh : Refreshed fia_offers file.
--               : 3. drop_tmp : file which has drop values.
--  Outputs      : 1. fia_offers_file_dropped : To store the fia_offers file post dropping.
--               : 2. mem_sc_cpn_final : member_sc_cpn file post drop.
--  Modification Log:
-----------------------------------------------------------------------------------------
--  Author           Version              Date                     Comments
-----------------------------------------------------------------------------------------
--  phala1            1.0              01/08/2015                  Creation
--
-----------------------------------------------------------------------------------------

Member_sc_cpn_file = LOAD '$member_sc_cpn' USING PigStorage(' ') AS (TRIPLE_KEY:chararray,SUBCAT:chararray,COUPON_NO:int);
TRIPLE_KEY_GRP = GROUP Member_sc_cpn_file BY TRIPLE_KEY;
Coupon_count = FOREACH TRIPLE_KEY_GRP GENERATE $0, COUNT($1) AS cnt;
Member_file_cpn_tmp = JOIN Member_sc_cpn_file BY TRIPLE_KEY, Coupon_count BY $0;
Member_file_cpn_cnt = FOREACH Member_file_cpn_tmp GENERATE $0,$1,$2,$3,($2 is null? 0: $4);


dropInpCol = LOAD '$drop_tmp' USING PigStorage() AS (DRP_KEY:long);
Cmp_file = JOIN Member_file_cpn_cnt BY $4 LEFT OUTER, dropInpCol BY DRP_KEY;
Mem_sc_cpn_aftdrop = FILTER Cmp_file BY $5 IS NULL;
Mem_sc_cpn_drop = FILTER Cmp_file BY $5 IS NOT NULL;
Mem_sc_cpn_2 = FOREACH Mem_sc_cpn_aftdrop GENERATE $0,$1,$2;
Mem_cpn_nbr = FOREACH Mem_sc_cpn_drop GENERATE $2;
Cpn_grp = GROUP Mem_cpn_nbr BY $0;
Mem_cpn_cnt = FOREACH Cpn_grp GENERATE $0, COUNT($1) AS cpcnt;

fia_offers_file1 = LOAD '$fia_offers_file_refresh/p*' USING PigStorage('|') AS(
CAMPAIGN_NBR:chararray
,SUBCLASS_NBR:chararray
,DEPT_NBR:chararray
,VAL_OFFER_TYPE_CODE:chararray
,VAL_OFFER_TYPE_DESC:chararray
,MDS_FAM_ID:chararray
,INVESTMENT_CNT:int
,VALUE_AMT:chararray
,VALUE_PCT:chararray
,MIN_ITEM_PURCH_QTY:chararray
,MAX_REDEMPTION_CNT:chararray
,PACKAGE_CODE:chararray
,PACKAGE_DESC:chararray
,VENDOR_FUNDED_IND:chararray
,COUPON_ITEM_NBR:chararray
,VAL_ITEM_TYPE_CODE:chararray
,VAL_ITEM_TYPE_DESC:chararray
,VALUE_COUPON_NBR:int
,PRVDR_COUPON_NBR:chararray
,CLUB_AVAIL_IND:chararray
,FILLER_DATA:chararray);

cpn_join = JOIN fia_offers_file1 BY VALUE_COUPON_NBR LEFT OUTER, Mem_cpn_cnt BY $0;

fia_offers_file2 = FOREACH cpn_join GENERATE CAMPAIGN_NBR,SUBCLASS_NBR,DEPT_NBR,VAL_OFFER_TYPE_CODE,VAL_OFFER_TYPE_DESC,MDS_FAM_ID
,($21 is null ? INVESTMENT_CNT : INVESTMENT_CNT+$22),VALUE_AMT,VALUE_PCT,MIN_ITEM_PURCH_QTY,MAX_REDEMPTION_CNT,PACKAGE_CODE,PACKAGE_DESC,VENDOR_FUNDED_IND,COUPON_ITEM_NBR,VAL_ITEM_TYPE_CODE
,VAL_ITEM_TYPE_DESC,VALUE_COUPON_NBR,PRVDR_COUPON_NBR,CLUB_AVAIL_IND,FILLER_DATA;

STORE fia_offers_file2 INTO '$fia_offers_file_dropped' USING PigStorage('|');

STORE Mem_sc_cpn_2 INTO '$mem_sc_cpn_final' USING PigStorage(' ');