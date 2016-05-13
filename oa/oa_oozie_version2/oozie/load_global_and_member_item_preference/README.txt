*****************************************************************************************
  Name         : Workflow for OA - load_global_and_member_item_preference 
  Description  : This is the workflow for global and member item preference

  Modification Log:
*****************************************************************************************
  Author           Version              Date                     Comments
*****************************************************************************************
  vmalaya            1.0              09/08/2014          Centralization of OA
  cchan1             2.0              11/17/2014          Attach pscore member pool to OA
*****************************************************************************************

STEP 1	:-	
Action Name:	global_item_preference
Description:	Call global item preference process

STEP 2	:-	
Decision Name:	check_member_source
Description:	Check member pool is internal or external

STEP 3	:-	
Action Name:	member_source_internal
Description:	Call member source internal hql to extract internal member_pool

STEP 4	:-	
Action Name:	member_source_external
Description:	Call member source external hql to extract member_pool form UNICA

STEP 5	:-	
Action Name:	member_item_preference
Description:	Call member item preference process

STEP 6	:-	
Join Name:	    join
Description:	Wait for step2 and step3 process to complete

STEP 7	:-	
Decision Name:	checkerror
Description:	Decision check to see if there is error in the fork process

STEP 8	:-	
Fork Name:		fork3
Description:	Call load_global_sbcat_item_pref_final and load_member_subcat_item_pref_final action in parallel

STEP 9	:-	
Action Name:	load_global_sbcat_item_pref_final
Description:	Call global subcat item preference process

STEP 10	:-	
Action Name:	load_member_subcat_item_pref_final
Description:	Call member subcat item preference process

STEP 11	:-	
Join Name:	    join
Description:	Wait for step7 and step8 process to complete

STEP 12	:-	
Decision Name:	checkerror
Description:	Decision check to see if there is error in the fork3 process

STEP 13 :-
Action Name: 	send-email
Description: 	Send email in case of error

STEP 14 :-
kill  Name: 	kill 
Description: 	Kill the workflow in case of error