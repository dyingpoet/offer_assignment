-----------------------------------------------------------------------------------------
--  Process Name : OA Automation - Pre-process
--  Script Name  : load_memberfile.hql
--  Description  : This hql will load lower bound and upper bound limit into memberfile table.
--  Modification Log:
-----------------------------------------------------------------------------------------
--  Author           Version              Date                     Comments
-----------------------------------------------------------------------------------------
--  vmalaya            1.0              09/25/2014          Centralization of OA 
--                                                                                     
-----------------------------------------------------------------------------------------
use ${database};

--LOAD DATA INPATH '${memberHDFS}' OVERWRITE INTO TABLE memberfile PARTITION(campaign_iter='${campaign_iter}');

ALTER TABLE member_tmp ADD IF NOT EXISTS PARTITION(campaign_iter='${campaign_iter}') LOCATION '/user/hive/warehouse/${database}.db/member_tmp/campaign_iter=${campaign_iter}';

INSERT OVERWRITE TABLE memberfile PARTITION(campaign_iter='${campaign_iter}')
SELECT member, ${lowerbound} AS lowerbound, ${upperbound} AS upperbound 
FROM member_tmp a where a.campaign_iter = '${campaign_iter}';
