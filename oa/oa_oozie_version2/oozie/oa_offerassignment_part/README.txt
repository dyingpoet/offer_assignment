**********************************************************************************************************************
Name               : Workflow for OA_OffrAssignment_Part 
Description        : This is the main workflow for offer_assignment core Part
Modification Log   :

**********************************************************************************************************************
Author               Version                  Date                        Comments
**********************************************************************************************************************
sgopa4              1.0                    23/12/2014                   Centralisation of SPE Changes
**********************************************************************************************************************

STEP 1 :-
Action Name: auctionFast_with_SubcatConstraints
Description: Python script to create offer_assignment_part.

STEP 2 :-
Action Name: add_sc_py
Description: Python script to create member_sc_coupon.

STEP 3 :-
Action Name: load_offer_assignment_table
Description: Call the HIVE script load_offer_assignment_table.hql.

STEP 4 :-
kill Name: kill 
Description: Kill the workflow in case of error

STEP 5 :-
Action Name: end
Description: End the process