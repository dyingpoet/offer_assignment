-----------------------------------------------------------------------------------------
--  Process Name : Offer_Assignment_Automation
--  Script Name  : copy_files.pig
--  Description  : This Pig script will copy the files.
--
--  Inputs       : 1. fia_offers_file_dropped : Input fia_offers file.
--  Output       : 1. fia_offers_file_rebuild : Output fia_offers file.
--
--  Modification Log:
-----------------------------------------------------------------------------------------
--  Author           Version              Date                     Comments
-----------------------------------------------------------------------------------------
--  phala1            1.0              01/13/2015                  Creation
--
-----------------------------------------------------------------------------------------
fia_offers_file2 = LOAD '${fia_offers_file_dropped}';
STORE fia_offers_file2 INTO '${fia_offers_file_rebuild}';