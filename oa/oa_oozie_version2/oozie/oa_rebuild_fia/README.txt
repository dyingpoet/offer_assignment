**********************************************************************************************************************
Name               : Workflow for OA Rebuild Fia Offers File Flow 
Description        : This is the sub-workflow for Backfill
Modification Log   :

**********************************************************************************************************************
Author               Version                  Date                        Comments
**********************************************************************************************************************
phala1               1.0                    01/13/2015                   Integration with SPE
**********************************************************************************************************************

STEP 1 :-
Action Name: validate_script
Description: Decision to check if the value_coupon_nbr value is sent from SPE or not.

STEP 2 :-
Action Name: create_cpn_tmp
Description: call cpn_inp.py to load the value_coupon_nbr values into a temp file.

STEP 3 :-
Action Name: rebuild_fia
Description: call fia_coupons.pig to rebuild the fia_offers file based on the values sent from SPE.

STEP 4 :-
Action Name: copy_files
Description: Action is called if value_coupon_nbr value is 999999 and will call the script copy_files.pig to copies the files.

STEP 5 :-
Action Name: send-email
Description: Send email in case of error

STEP 6 :-
kill Name: kill 
Description: Kill the workflow in case of error