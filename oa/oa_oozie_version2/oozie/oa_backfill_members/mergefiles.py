#!/usr/bin/python
#-----------------------------------------------------------------------------------------
#--  Process Name : OA Automation - Post-process
#--  Script Name  : mergefiles.py
#--  Description  : This python script will merge intermediate output files and generates final output file.
#--  Input Files  : member_sc_cpn_popularity_backfill, mem_sc_cpn_final
#--  Output Files : offerAssignment_member_decouple
#--  Modification Log: 
#-----------------------------------------------------------------------------------------
#--  Author           Version              Date                     Comments
#-----------------------------------------------------------------------------------------
#--  dnaga1            1.0              11/20/2014          Centralization of OA 
#--                                                                                     
#-----------------------------------------------------------------------------------------


import sys
import subprocess
from subprocess import Popen,PIPE
#---------------------------------------------------------------------------------------------
# Opening argv[1] file as input to be merged 
# Writing to output file(argv[3])
#----------------------------------------------------------------------------------------------
input1 = subprocess.Popen(["hadoop","fs","-cat",sys.argv[1]],stdout=subprocess.PIPE)
output = subprocess.Popen(["hadoop","fs","-put","-",sys.argv[3]],stdin=PIPE)
for line in input1.stdout:
    member, sc, cpn = line.strip().split(' ')
    membership_nbr, card_holder_nbr, member_create_date =member.strip().split('_')
    result_str = ' '.join([membership_nbr, card_holder_nbr, member_create_date, sc, cpn])
    output.stdin.write(result_str+'\n')
#---------------------------------------------------------------------------------------------
# Close input1 file
#----------------------------------------------------------------------------------------------
input1.stdout.close()
input1.wait()

#---------------------------------------------------------------------------------------------
# Opening argv[2] file as input to be merged 
# Writing to output file(argv[3]) 
#----------------------------------------------------------------------------------------------

input2 = subprocess.Popen(["hadoop","fs","-cat",sys.argv[2]],stdout=subprocess.PIPE)
for line in input2.stdout:
    member, sc, cpn = line.strip().split(' ')
    membership_nbr, card_holder_nbr, member_create_date =member.strip().split('_')
    result_str = ' '.join([membership_nbr, card_holder_nbr, member_create_date, sc, cpn])
    output.stdin.write(result_str+'\n')
	
#---------------------------------------------------------------------------------------------
# Close both output file and input2 file
#----------------------------------------------------------------------------------------------
input2.stdout.close()
input2.wait()

output.stdin.close()
output.wait()



