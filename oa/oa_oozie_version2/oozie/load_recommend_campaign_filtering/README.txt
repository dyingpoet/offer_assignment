**********************************************************************************************************************
Name               : Workflow for load_recommend_campaign_filtering 
Description        : This is the workflow for load_recommend_campaign_filtering
Modification Log   :

**********************************************************************************************************************
Author               Version                  Date                        Comments
**********************************************************************************************************************
phala1                1.0                    11/17/2014                   Centralisation of OA

**********************************************************************************************************************

STEP 1 :-

Action Name: load_recommend_rank_delta
Description: This pig script will create score output file for recommend.

STEP 2 :-

Action Name: load_memberfile
Description: This action will call the HIVE script load_memberfile.hql which will load data into memberfile.

STEP 3 :-

Action Name: send-email
Description: Send email in case of error

STEP 4 :-

kill Name: kill 
Description: Kill the workflow in case of error