#!/bin/bash

source reward_offer_assignment_param.cfg

MONTH_TAG=2015mar_79
FIA_INPUT=sc_offers_${MONTH_TAG}_input
MEMBER_INPUT=member_base_${MONTH_TAG}

### pre-loading
hadoop fs -put $CONFIG_FILE_PATH/$FIA_INPUT /user/pythia/Workspaces/SamsMEP/offer_assignment_fia/campaign_month=${CAMPAIGN_MONTH}/$FIA_INPUT
hive -e "USE pythia; ALTER TABLE offer_assignment_fia ADD PARTITION(campaign_month='${CAMPAIGN_MONTH}');"
hive -e "USE pythia; LOAD DATA LOCAL INPATH '$CONFIG_FILE_PATH/${MEMBER_INPUT}' OVERWRITE INTO TABLE offer_assigned_member_base PARTITION (campaign_month='${CAMPAIGN_MONTH}')";



