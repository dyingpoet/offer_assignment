----------------------------------------------------------------------------------------
--  Process Name : Offer_Assignment_Automation
--  Script Name  : fia_coupons.pig
--  Description  : This Pig script will rebuild the fia_offers file.
--
--  Inputs       : 1. cpn_tmp: File which holds the coupons 
--               : 2. fia_offers_file_dropped :Input fia_offers file.
--   Ouput       : 1. fia_offers_file_rebuild: Path to store fia_offers_file_rebuild file
--
--  Modification Log: 
-----------------------------------------------------------------------------------------
--  Author           Version              Date                     Comments
-----------------------------------------------------------------------------------------
--  phala1            1.0              01/08/2015                  Creation 
--                                                                             
-----------------------------------------------------------------------------------------

fia_offers_file2 = LOAD '${fia_offers_file_dropped}/p*' USING PigStorage('|') AS(
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

cpnInpCol = LOAD '${cpn_tmp}' USING PigStorage() AS (cpn_key:int);
cpn_join = JOIN fia_offers_file2 BY VALUE_COUPON_NBR LEFT OUTER, cpnInpCol BY $0;
fia_offers_cpn = FILTER cpn_join BY $21 IS NOT NULL;

fia_offers_file3 = FOREACH fia_offers_cpn GENERATE CAMPAIGN_NBR,SUBCLASS_NBR,DEPT_NBR,VAL_OFFER_TYPE_CODE,VAL_OFFER_TYPE_DESC,MDS_FAM_ID
,INVESTMENT_CNT,VALUE_AMT,VALUE_PCT,MIN_ITEM_PURCH_QTY,MAX_REDEMPTION_CNT,PACKAGE_CODE,PACKAGE_DESC,VENDOR_FUNDED_IND,COUPON_ITEM_NBR,VAL_ITEM_TYPE_CODE
,VAL_ITEM_TYPE_DESC,VALUE_COUPON_NBR,PRVDR_COUPON_NBR,CLUB_AVAIL_IND,FILLER_DATA;

STORE fia_offers_file3 INTO '${fia_offers_file_rebuild}' USING PigStorage('|');