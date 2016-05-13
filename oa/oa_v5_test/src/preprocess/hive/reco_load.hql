----table
--USE pythia;
--LOAD DATA LOCAL INPATH './offersSC2013nov.txt' OVERWRITE INTO TABLE offer_assignment_fia PARTITION (campaign_month='Nov2013');

----hive external table
--USE pythia;
--alter table offer_assignment_fia drop partition (campaign_month='Nov2013');
--ALTER TABLE offer_assignment_fia ADD PARTITION(campaign_month='Nov2013');
--ALTER TABLE offer_assignment_fia ADD PARTITION(campaign_month='Dec2013');
--ALTER TABLE offer_assignment_fia ADD PARTITION(campaign_month='Jan2014');

--ALTER TABLE log_messages ADD PARTITION(year = 2012, month = 1, day = 2) LOCATION '';

USE pythia; 
ALTER TABLE ${hiveconf:reco_table} ADD PARTITION(campaign_month='${hiveconf:campaign_month_type}') LOCATION '${hiveconf:reco_loc}';


--ALTER TABLE sales 
--    ADD PARTITION (country = 'US', year = 2012, month = 12, day = 22) 
--    LOCATION 'sales/partitions/us/2012/12/22' ;


