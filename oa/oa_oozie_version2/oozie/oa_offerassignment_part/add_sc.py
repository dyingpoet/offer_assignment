#!/usr/bin/python
#-----------------------------------------------------------------------------------------
#--  Process Name : OA Automation - Post-process
#--  Script Name  : add_sc.py
#--  Description  : This python script will generate member sc coupon output file.
#--  Input Files  : sc_coupon_cnt, offerassignment
#--  Output Files : member_sc_cpn
#--  Modification Log: 
#-----------------------------------------------------------------------------------------
#--  Author           Version              Date                     Comments
#-----------------------------------------------------------------------------------------
#--  dnaga1            1.0              09/25/2014          Centralization of OA 
#--                                                                                     
#-----------------------------------------------------------------------------------------

import sys
import string
import subprocess
from subprocess import Popen,PIPE

cpn2sc = {}

cat1 = subprocess.Popen(["hadoop","fs","-cat",sys.argv[1]],stdout=subprocess.PIPE)
for line in cat1.stdout:
    sc,cpn,cnt = line.strip().split(' ')
    cpn2sc[cpn] = sc

cat1.stdout.close()

cat2 = subprocess.Popen(["hadoop","fs","-cat",sys.argv[2]],stdout=subprocess.PIPE)
put = subprocess.Popen(["hadoop","fs","-put","-",sys.argv[3]],stdin=PIPE)
for line in cat2.stdout:
    member, cpn = line.strip().split(' ')
    sc = cpn2sc[cpn]
    #print ' '.join([member, sc, cpn])
    #memdecouple = string.replace(member, '_', ' ')
    #membership_nbr,card_holder_nbr,member_create_date = memdecouple.split(' ')
    #result_str = ' '.join([membership_nbr, card_holder_nbr, member_create_date, sc, cpn])
    result_str = ' '.join([member, sc, cpn])
    put.stdin.write(result_str+'\n')

cat2.stdout.close()
cat2.wait()
put.stdin.close()
put.wait()
