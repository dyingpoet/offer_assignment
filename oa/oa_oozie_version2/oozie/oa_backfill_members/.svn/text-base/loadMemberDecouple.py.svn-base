#!/usr/bin/python
#-----------------------------------------------------------------------------------------
#--  Process Name : Offer Assignment Automation
#--  Script Name  : loadMemberDecouple.py
#--  Description  : This python script will perform concat for triple key generation.
#--  Input File   : offerAssignment_member_decouple
#--  Output File  : offerAssignment_member_decouple_cpy
#--  Modification Log:
#-----------------------------------------------------------------------------------------
#--  Author           Version              Date                     Comments
#-----------------------------------------------------------------------------------------
#--  phala1            1.0              01/14/2015                  SPE backfill changes                                                                     
#-----------------------------------------------------------------------------------------

import sys
import string
import subprocess
from subprocess import Popen,PIPE

cat2 = subprocess.Popen(["hadoop","fs","-cat",sys.argv[1]],stdout=subprocess.PIPE)
put = subprocess.Popen(["hadoop","fs","-put","-",sys.argv[2]],stdin=PIPE)
for line in cat2.stdout:
    membership_nbr, card_holder_nbr, member_create_date, sc, cpn = line.strip().split(' ')
    result_str1 = '_'.join([membership_nbr, card_holder_nbr, member_create_date])
    result_str  = ' '.join([result_str1, sc, cpn])
    put.stdin.write(result_str+'\n')

cat2.stdout.close()
cat2.wait()
put.stdin.close()
put.wait()
