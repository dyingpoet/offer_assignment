#!/usr/bin/python
#-----------------------------------------------------------------------------------------
#--  Process Name : Offer_Assignment_Automation
#--  Script Name  : cpn_inp.py
#--  Description  : This python script will generate cpn_tmp file.
#--  Input        : value_coupon_nbr : Coupon values from SPE
#--  Output Files : cpn_tmp : File to hold coupon values
#--  Modification Log:
#-----------------------------------------------------------------------------------------
#--  Author           Version              Date                     Comments
#-----------------------------------------------------------------------------------------
#--  phala1            1.0              01/13/2015                     Creation
#--
#-----------------------------------------------------------------------------------------

import sys
import string
import subprocess
from subprocess import Popen,PIPE

cpnInp = sys.argv[1]
cpnInp_list = cpnInp.rstrip().split(',')
put = subprocess.Popen(["hadoop","fs","-put","-",sys.argv[2]],stdin=PIPE)
for inp in cpnInp_list:
    put.stdin.write(inp+'\n')
put.stdin.close()
put.wait()
