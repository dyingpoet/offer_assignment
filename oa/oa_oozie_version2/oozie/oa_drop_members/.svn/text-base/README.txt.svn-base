**********************************************************************************************************************
Name               : Workflow for OA Drop Members Flow 
Description        : This is the sub-workflow for Backfill
Modification Log   :

**********************************************************************************************************************
Author               Version                  Date                        Comments
**********************************************************************************************************************
phala1               1.0                    01/13/2015                   Integration with SPE
**********************************************************************************************************************

STEP 1 :-
Action Name: validate_script
Description: Decision to check if the drop_records value is sent from SPE or not.

STEP 2 :-
Action Name: create_drop_tmp
Description: call drop_inp.py to load the drop_records values into a temp file.

STEP 3 :-
Action Name: drop_members
Description: call dropCoupons.pig to drop the coupons for the members sent from SPE.

STEP 4 :-
Action Name: copy_files
Description: Action is called if drop_records value is 999999 and will call the script copy_files.pig to copies the files.

STEP 5 :-
Action Name: send-email
Description: Send email in case of error

STEP 6 :-
kill Name: kill 
Description: Kill the workflow in case of error