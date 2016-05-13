**********************************************************************************************************************
Name               : Workflow for OA_Postprocessing 
Description        : This is the workflow for offer_assignment Update stats
Modification Log   :

**********************************************************************************************************************
Author               Version                  Date                        Comments
**********************************************************************************************************************
phala1              1.0                    13/01/2014                   Centralisation of SPE Changes
**********************************************************************************************************************

STEP 1 :-
Action Name: calculate_core_flow_summarystats
Description: Call the HIVE script summary_statistics_metrics_n_coreflow.hql.

STEP 2 :-
Action Name: export_statistics
Description: Export the data into ORACLE table.

STEP 3 :-
Action Name: send-email
Description: Send email in case of error

STEP 4 :-
kill Name: kill 
Description: Kill the workflow in case of error