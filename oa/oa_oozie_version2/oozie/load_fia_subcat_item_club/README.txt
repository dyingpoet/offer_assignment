**********************************************************************************************************************
Name               : Workflow for load_fia_subcat_item_club 
Description        : This is the workflow for load_fia_subcat_item_club
Modification Log   :

**********************************************************************************************************************
Author               Version                  Date                        Comments
**********************************************************************************************************************
nkuland              1.0                    09/24/2014                   Centralisation of OA
sgopa4               2.0                    11/07/2014                   OA Version 2.0
sgopa4		     3.0                    02/17/2015                   OA Version 3.0
**********************************************************************************************************************

STEP 1 :-

Action Name: inventory_ind_chck_decision
Description: It will call the decision action to decide on inventory check flag.

STEP 2 :-

Action Name: load_fia_subcat_item_club
Description: Call hive script load_fia_subcat_item_club to load fia_subcat_item_club when the inventory_check_flag is 'N'.

STEP 3 :-
Action Name:load_fia_subcat_item_club_with_item_check
Description: Call hive script load_fia_subcat_item_club_with_item_check to load fia_subcat_item_club table by checking item inventory in club.

STEP 4 :-
Action Name:load_fia_subcat_item_club_with_subcat_check
Description: Call hive script load_fia_subcat_item_club_with_subcat_check to
load fia_subcat_item_club table by checking subcat inventory in club.


STEP 5 :-

Action Name: send-email
Description: Send email in case of error

STEP 6:-

kill Name: kill 
Description: Kill the workflow in case of error
