-----------------------------------------------------------------------------------------
--  Process Name : Offer_Assignment_Automation
--  Script Name  : Refresh_fia_offers.pig
--  Description  : This Pig script will refresh the fia_offers file.
--
--  Inputs       : 1. member_sc_cpn: member_sc_cpn file
--               : 2. Preprocessed fia offers file
--  Output       : 1. fia_offers_file_refresh : Refreshed fia_offers file
--
--  Modification Log:
-----------------------------------------------------------------------------------------
--  Author           Version              Date                     Comments
-----------------------------------------------------------------------------------------
--  phala1            1.0              01/08/2015                  Creation
--
-----------------------------------------------------------------------------------------


Member_sc_cpn_file = LOAD '${member_sc_cpn}' USING PigStorage(' ') AS (TRIPLE_KEY:chararray,SUBCAT:chararray,COUPON_NO:int);
COUPON_GRP = GROUP Member_sc_cpn_file BY COUPON_NO;
COUPON_cnt = FOREACH COUPON_GRP GENERATE $0, COUNT($1) AS cnt;

Preprocessed_fia_offers_file = LOAD '${FiaOfferFile}/*' USING PigStorage('|') AS(
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

COUPON_cpn_join = JOIN Preprocessed_fia_offers_file BY VALUE_COUPON_NBR LEFT OUTER, COUPON_cnt BY $0;

fia_offers_file1 = FOREACH COUPON_cpn_join GENERATE CAMPAIGN_NBR,SUBCLASS_NBR,DEPT_NBR,VAL_OFFER_TYPE_CODE,VAL_OFFER_TYPE_DESC,MDS_FAM_ID
,($21 is null ? INVESTMENT_CNT : INVESTMENT_CNT-$22),VALUE_AMT,VALUE_PCT,MIN_ITEM_PURCH_QTY,MAX_REDEMPTION_CNT,PACKAGE_CODE,PACKAGE_DESC,VENDOR_FUNDED_IND,COUPON_ITEM_NBR,VAL_ITEM_TYPE_CODE
,VAL_ITEM_TYPE_DESC,VALUE_COUPON_NBR,PRVDR_COUPON_NBR,CLUB_AVAIL_IND,FILLER_DATA;

STORE fia_offers_file1 INTO '${fia_offers_file_refresh}' USING PigStorage('|');