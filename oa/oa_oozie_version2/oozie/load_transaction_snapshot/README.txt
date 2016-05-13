**********************************************************************************************************************
Name               : Workflow for load_fia_subcat_item_club 
Description        : This is the workflow for load_fia_subcat_item_club
Modification Log   :

**********************************************************************************************************************
Author               Version                  Date                        Comments
**********************************************************************************************************************
nkuland              1.0                    09/24/2014                   Centralisation of OA
sgopa4               2.0                    11/07/2014                   OA Version 2.0
**********************************************************************************************************************

STEP 1 :-

Action Name: load_transaction_snapshot
Description: It will call the hive script trans_insert.hql to load data into customer_club_day_item_sales table.

STEP 2 :-

Action Name: send-email
Description: Send email in case of error

STEP 3:-

kill Name: kill 
Description: Kill the workflow in case of error
