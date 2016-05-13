**********************************************************************************************************************
Name               : Workflow for Offer Assignment Process
Description        : This is the main workflow for automation of oa_core_generation_flow
Modification Log   :

**********************************************************************************************************************
Author               Version                  Date                        Comments
**********************************************************************************************************************
cchan1               1.0                    01/08/2015                   Integration with SPE
**********************************************************************************************************************

STEP 1 :-
Action Name: load_offer_assignment_preprocess
Description: Call subworkflow of load_offer_assignment_preprocess.

STEP 2 :-
Action Name: OA_OfferAssignmentPart
Description: Call workflow of OA_OfferAssignmentPart.

STEP 3 :-
Action Name:OA_Post_Processing
Description: Call workflow of OA_Post_Processing.
 
STEP 4 :-
Action Name:OA_UpdateStats
Description: Call workflow of OA_UpdateStats 

STEP 5 :-
Action Name: send-email
Description: Send email in case of error

STEP 6 :-
kill Name: kill 
Description: Kill the workflow in case of error