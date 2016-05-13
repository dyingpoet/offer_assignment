**********************************************************************************************************************
Name               : Workflow for load_fia_subcat_item_club 
Description        : This is the workflow for load_fia_subcat_item_club
Modification Log   :

**********************************************************************************************************************
Author               Version                  Date                        Comments
**********************************************************************************************************************
nkuland              1.0                    09/24/2014                   Centralisation of OA
sgopa4               2.0                    11/07/2014                   OA Version 2.0
phala1               3.0                    12/17/2014                   Multiple Run Support
**********************************************************************************************************************

STEP 1 :-

Action Name: fork
Description: It will call three Actions load_fia_offer_file, load_fia_offer2subcat_file and load_fia_sc_coupon_cnt.

STEP 1.1 :-

Action Name: load_fia_offer_file
Description: Call hive script load_fia_offerfile.hql  to load offerfile.

STEP 1.2 :-

Action Name: load_fia_offer2subcat_file
Description: Call hive script load_fia_offer2subcatfile.hql  to load offer2subcat_file.

STEP 1.3 :-

Action Name: load_fia_sc_coupon_cnt.
Description: Call hive script load_fia_sc_coupon_cnt.hql to load sc_coupon_cnt.

STEP 2 :-
Action Name: success-send-email
Description:Sends the success email if lastErrNode() is equal to null.

STEP 3 :-

Action Name: send-email
Description: Send email in case of error

STEP 4:-

kill Name: kill 
Description: Kill the workflow in case of error
