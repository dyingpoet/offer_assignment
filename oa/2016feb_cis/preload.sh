#!/bin/bash

source reward_offer_assignment_param.cfg

MONTH_TAG=2016feb_684
FIA_INPUT=sc_offers_${MONTH_TAG}_input
#MEMBER_INPUT=member_base_${MONTH_TAG}
MEMBER_INPUT=memberAll

### pre-loading
hadoop fs -put $CONFIG_FILE_PATH/$FIA_INPUT /user/pythia/Workspaces/SamsMEP/offer_assignment_fia/campaign_month=${CAMPAIGN_MONTH}/$FIA_INPUT
hive -e "USE pythia; ALTER TABLE offer_assignment_fia ADD PARTITION(campaign_month='${CAMPAIGN_MONTH}');"
hive -e "USE pythia; LOAD DATA LOCAL INPATH '$CONFIG_FILE_PATH/${MEMBER_INPUT}' OVERWRITE INTO TABLE offer_assigned_member_base PARTITION (campaign_month='${CAMPAIGN_MONTH}')";


hive -e "SELECT * FROM pythia.offer_assignment_fia WHERE campaign_month='${CAMPAIGN_MONTH}' LIMIT 10;"
hive -e "SELECT * FROM pythia.offer_assigned_member_base WHERE campaign_month='${CAMPAIGN_MONTH}' LIMIT 10;"

