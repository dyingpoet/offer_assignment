**********************************************************************************************************************
Name               : Workflow for OA Build Fia Offers file Flow 
Description        : This is the main sub-workflow for Backfill
Modification Log   :

**********************************************************************************************************************
Author               Version                  Date                        Comments
**********************************************************************************************************************
phala1               1.0                    01/13/2015                   Integration with SPE
**********************************************************************************************************************

STEP 1 :-
Action Name: build_fia
Description: Call the offer_file_preparation.hql to unload the data from fia_offers table.

STEP 2 :-
Action Name: send-email
Description: Send email in case of error

STEP 3 :-
kill Name: kill 
Description: Kill the workflow in case of error