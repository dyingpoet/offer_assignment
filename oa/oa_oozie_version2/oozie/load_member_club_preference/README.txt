**********************************************************************************************************************
Name               : Workflow for load_member_club_preference 
Description        : This hql will extracts & loads membership details into member_club_pref_table table.
Modification Log   :

**********************************************************************************************************************
Author               Version                  Date                        Comments
**********************************************************************************************************************
sgopa4                1.0                    11/07/2014                   Centralisation of OA
sgopa4                2.0                    11/17/2014                   OA Parameter Changes
**********************************************************************************************************************

STEP 1 :-

Action Name:load_member_club_preference
Description: Call hive script load_member_club_preference  when the ItemLevelScoreInd is not checked.

STEP 2 :-

Action Name: send-email
Description: Send email in case of error

STEP 3:-

kill Name: kill 
Description: Kill the workflow in case of error