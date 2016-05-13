**********************************************************************************************************************
Name               : Workflow for load_pis_member_subcat_item_coupon_score 
Description        : This is the workflow for load_pis_member_subcat_item_coupon_score
Modification Log   :

**********************************************************************************************************************
Author               Version                  Date                        Comments
**********************************************************************************************************************
sgopa4                1.0                    11/07/2014                   Centralisation of OA
sgopa4                2.0                    11/17/2014                   OA Parameter Changes
**********************************************************************************************************************

STEP 1 :-

Depending on the "ItemLevelScoreInd" value the corresponding Action will be called.

STEP 1.1 CASE 1 :-

Action Name: load_pis_member_item_coupon
Description: Call hive script load_pis_member_item_coupon when the ItemLevelScoreInd is checked.

STEP 1.1 CASE 2 :-

Action Name:load_pis_member_subcat_coupon
Description: Call hive script load_pis_member_subcat_coupon  when the ItemLevelScoreInd is not checked.

STEP 2 :-

Action Name: send-email
Description: Send email in case of error

STEP 3:-

kill Name: kill 
Description: Kill the workflow in case of error