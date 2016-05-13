#!/usr/bin/python
#-----------------------------------------------------------------------------------------
#--  Process Name : Offer_Assignment_Automation
#--  Script Name  : drop_inp.py
#--  Description  : This python script will load drop_records values as a file.
#--  Input        : drop_records values
#--  Output Files : drop_tmp file
#--  Modification Log:
#-----------------------------------------------------------------------------------------
#--  Author           Version              Date                     Comments
#-----------------------------------------------------------------------------------------
#--  phala1            1.0              01/13/2015                  Creation
#--
#-----------------------------------------------------------------------------------------


import sys
import string
import subprocess
from subprocess import Popen,PIPE

dropInp = sys.argv[1]
dropInp_list = dropInp.rstrip().split(',')
#put = subprocess.Popen(["hadoop","fs","-put","-",sys.argv[2]],stdin=PIPE)
put = subprocess.Popen(["hadoop","fs","-put","-",sys.argv[2]],stdin=PIPE)

for inp in dropInp_list:
    put.stdin.write(inp+'\n')
put.stdin.close()
put.wait()