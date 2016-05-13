**********************************************************************************************************************
Name               : Workflow for OA Refresh Fia Offers file Flow 
Description        : This is the main sub-workflow for Backfill
Modification Log   :

**********************************************************************************************************************
Author               Version                  Date                        Comments
**********************************************************************************************************************
phala1               1.0                    01/13/2015                   Integration with SPE
**********************************************************************************************************************

STEP 1 :-
Action Name: refresh_fia
Description: Call the Refresh_fia_offers.pig to Refresh the fia_offers file with latest offers.

STEP 2 :-
Action Name: send-email
Description: Send email in case of error

STEP 3 :-
kill Name: kill 
Description: Kill the workflow in case of error