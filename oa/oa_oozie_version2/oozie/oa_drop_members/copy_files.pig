-----------------------------------------------------------------------------------------
--  Process Name : Offer_Assignment_Automation
--  Script Name  : copy_files.pig
--  Description  : This Pig script will copy the files.
--
--  Inputs       : 1. fia_offers_file_refresh : Input fia_offers file.
--               : 2. member_sc_cpn           : Input member_sc_cpn file.
--  Output       : 1. fia_offers_file_dropped : Output fia_offers file.
--               : 2. mem_sc_cpn_final        : Output member_sc_cpn file
--
--  Modification Log:
-----------------------------------------------------------------------------------------
--  Author           Version              Date                     Comments
-----------------------------------------------------------------------------------------
--  phala1            1.0              01/13/2015                  Creation
--
-----------------------------------------------------------------------------------------
fia_offers_file1 = LOAD '${fia_offers_file_refresh}';
STORE fia_offers_file1 INTO '${fia_offers_file_dropped}';

member_sc_cpn_file = LOAD '${member_sc_cpn}';
STORE member_sc_cpn_file INTO '${mem_sc_cpn_final}';