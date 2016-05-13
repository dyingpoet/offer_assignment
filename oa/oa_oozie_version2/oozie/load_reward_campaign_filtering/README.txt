**********************************************************************************************************************
Name               : Workflow for load_reward_campaign_filtering 
Description        : This is the workflow for load_reward_campaign_filtering
Modification Log   :

**********************************************************************************************************************
Author               Version                  Date                        Comments
**********************************************************************************************************************
phala1                1.0                    11/17/2014                   Centralisation of OA

**********************************************************************************************************************

STEP 1 :-

Depending on the "ItemLevelScoreInd" and "ClusterInd" value the corresponding Action will be called.

     STEP 1.1 CASE 1 : 
	 Action Name : item_reward_and_reco_clustering_filtering
	 Description : When ItemLevelScoreInd ="Y" and ClusterInd="Y" then this action will be called.
	 
	 STEP 1.1 CASE 2 : 
	 Action Name : item_reward_and_no_reco_clustering_filtering
	 Description : When ItemLevelScoreInd ="Y" and ClusterInd="N" then this action will be called.
	 
	 STEP 1.1 CASE 3 : 
	 Action Name : subcat_reward_and_reco_clustering_filtering
	 Description : When ItemLevelScoreInd ="N" and ClusterInd="Y" then this action will be called.
	 
	 STEP 1.1 CASE 4 : 
	 Action Name : subcat_reward_and_no_reco_clustering_filtering
	 Description : When ItemLevelScoreInd ="N" and ClusterInd="N" then this action will be called.
	
	 STEP 1.1 CASE 5 : 
	 Action Name : load_strict_reward_campaign_filtering
	 Description : When ItemLevelScoreInd ="N" this action will be called.



STEP 2 :-

Action Name: send-email
Description: Send email in case of error

STEP 3 :-

kill Name: kill 
Description: Kill the workflow in case of error