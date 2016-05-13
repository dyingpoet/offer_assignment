**********************************************************************************************************************
Name               : Workflow for OA Backfill Generation Flow 
Description        : This is the main workflow for Backfill
Modification Log   :

**********************************************************************************************************************
Author               Version                  Date                        Comments
**********************************************************************************************************************
cchan1               1.0                    01/08/2015                   Integration with SPE
**********************************************************************************************************************

STEP 1 :-
Action Name: update_inprogressjoborderstatus
Description: Call a jar to update Job Order database status.

STEP 2 :-
Action Name: oa_build_fia
Description: Call workflow to build fia_offers file from fia_offers table.

STEP 3 :-
Action Name: oa_refresh_fia
Description: Call workflow to refresh fia_offers file with the latest offers.


STEP 4 :-
Action Name: oa_drop_members
Description: Call workflow to drop members in the drop group, increment available offers.

STEP 5 :-
Action Name: oa_rebuild_fia
Description: Call workflow to build fia file based on offer assignment file, drop counts and coupons from SPE.
 
STEP 6 :-
Action Name: oa_backfill_members
Description: Call workflow to perform backfill. 
 
STEP 7 :-
Action Name: create_statistics
Description: Call workflow to create statistics.

STEP 8 :-
Action Name: update_completedjoborderstatus
Description: Call a jar to update Job Order database status.

STEP 9 :-
Action Name: update_errorjoborderstatus
Description: Call a jar to update Job Order database status.

STEP 10 :-
Action Name: send-email
Description: Send email in case of error

STEP 11 :-
kill Name: kill 
Description: Kill the workflow in case of error