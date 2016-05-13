**********************************************************************************************************************
Name               : Workflow for backfill_subflow 
Description        : This is the workflow for automation of Offer Assignment backfill members
Modification Log   :

**********************************************************************************************************************
Author               Version                  Date                        Comments
**********************************************************************************************************************
dnaga1              1.0                    11/25/2014                   Centralisation of OA
phala1				2.0					   01/14/2015					SPE Changes
phala1				3.0					   02/15/2015                   OA_V3 Changes
**********************************************************************************************************************

STEP 1 :-
Decision Name: campaign_score_chk
Description: This action will make decision on which campaign to run(Reward/Recommend).

STEP 2 :-
Action Name:recommend_flow
Description: This action is called when CAMPAIGN_TYPE == "RECCOMEND" and it calls python script popularity_no_cluster_backfill.py.

STEP 3 :-
Action Name:check_recomm_bkfill
Description: This action will make the decision based on the file size of popularity_fail_backfill,if its is greater than 0,call action end or else call action recomm_merge_file1

STEP 4:-
Action Name:recomm_merge_file1
Description: This action will call python script mergefile.py to merge the files member_sc_cpn_popularity_backfill,member_sc_cpn_final into member_sc_cpn_backfilled got from the previous action.

STEP 5 :-
Decision Name: no_qualify_chk
Description: This action will make decision to check if NO_QUALIFY_BACKFILL (backfill to members without scores if this is not set then call recomm_merge_file3 action.

STEP 6 :-
Action Name:no_qualify_prepare
Description: This action is called to move the member_sc_cpn_backfilled to member_sc_scpn_qualify.

STEP 7 :-
Decision Name: no_qualify_backfill_chk
Description: This action will call the action no_qualify_backfill_popularity_flow if NO_QUALIFY_BACKFILL_POPULARITY == "Y" and call the action no_qualify_backfill_default_flow
             if NO_QUALIFY_BACKFILL_DEFAULT == "Y"
			 
STEP 8:-
Action Name:no_qualify_backfill_popularity_flow
Description: This action will call python script no_qualify_backfill.py to backfill members without scores and NO_QUALIFY_BACKFILL_POPULARITY == "Y".

STEP 9:-
Action Name:no_qualify_backfill_default_flow
Description: This action will call python script no_qualify_backfill_default.py to backfill members without scores and NO_QUALIFY_BACKFILL_DEFAULT == "Y".

STEP 10:-
Action Name:recomm_merge_file2
Description: This action will call python script mergefile.py to merge the files member_sc_cpn_no_qualify_backfill,member_sc_cpn_qualify into member_sc_cpn_backfilled.

STEP 11:-
Action Name:recomm_merge_file3
Description: This action will call python script mergefilefinal.py to merge the file member_sc_cpn_backfilled into offerAssignment_member_decouple

STEP 12:-
Action Name:concat_triple_key
Description: This action will call python script loadMemberDecouple.py to concat the membership_nbr, card_holder_nbr, member_create_date.

STEP 13 :-
Decision Name: load_offer_assignment
Description: This action will call the script offer_assignment.hql to load data into offer_assignment table.
 
STEP 14 :-
Action Name:send-backfill-error-email
Description: Send the error mail with appropriate body when backfill fails.

STEP 15 :-
Action Name: send-email
Description: Send email in case of error

STEP 16 :-
kill Name: kill 
Description: Kill the workflow in case of error