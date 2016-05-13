#!/usr/bin/python
#-----------------------------------------------------------------------------------------
#--  Process Name : OA Automation - Post-process
#--  Script Name  : popularity_no_cluster_backfill.py
#--  Description  : This python script will perform recommend backfill process and generate member_sc_cpn_popularity_backfill file.
#--  Input Files  : oaFile, fiaFile
#--  Output Files : member_sc_cpn_popularity_backfill, popularity_fail_backfill
#--  Modification Log: 
#-----------------------------------------------------------------------------------------
#--  Author           Version              Date                     Comments
#-----------------------------------------------------------------------------------------
#--  dnaga1            1.0              11/20/2014          Centralization of OA 
#--  dnaga1            1.1              01/08/2015		    SPE backfill changes
#--  phala1            1.2              14/02/2015          OA_V3 Changes                                                                      
#-----------------------------------------------------------------------------------------


import sys
import subprocess
from subprocess import Popen,PIPE
import collections
from collections import defaultdict

#oaFile = sys.argv[1]
#fiaFile = sys.argv[2]
#memberClusterFile = sys.argv[3]
#recoClusterFile = sys.argv[4]

backfill_arr = sys.argv[5]
offerUpLimit = int(sys.argv[6])

fiaScMap = defaultdict(list)
recoClusterMap = defaultdict(list)
memberOffers= defaultdict(list)
memberOfferBk = defaultdict(list)
memberBackfilled = {}
#memberClusterMap = {}
#memberOffer = {}
offerAvail = {}
offerAssigned = defaultdict(int)
#coupon rank
offerRank = {}
epsilon = 1e-6


#with open(fiaFile,'r') as inputObj:

#-------------------------------------------------------------
#opening fiaFile in read mode
#-------------------------------------------------------------
inputObj1 = subprocess.Popen(["hadoop","fs","-cat",sys.argv[2]],stdout=subprocess.PIPE)

for line in inputObj1.stdout:
        words = line.rstrip().split('|')
        cpn = words[17]
        item = words[5]
        inv = int(words[6])
        sc = "%02d%02d" % (int(words[1]),int(words[2]))
        offerAvail[cpn] = inv
        if cpn not in fiaScMap[sc]:
            fiaScMap[sc].append(cpn)
inputObj1.stdout.close()

#with open(oaFile,'r') as inputObj:

#-------------------------------------------------------------
#opening oaFile in read mode
#-------------------------------------------------------------

inputObj2 = subprocess.Popen(["hadoop","fs","-cat",sys.argv[1]],stdout=subprocess.PIPE)
for line in inputObj2.stdout:
        member, sc, cpn = line.rstrip().split(' ')
        memberOffers[member].append(sc)
        offerAssigned[cpn] += 1
#       offerAvail[cpn] -= 1

inputObj2.stdout.close()

offerRankList = sorted([(cpn,offerAssigned[cpn]) for cpn in offerAvail],key=lambda tup:tup[1],reverse=True)
for i in xrange(1,len(offerRankList)+1,1):
    offerRank[offerRankList[i-1][0]] = i

backfill_list = backfill_arr.rstrip().split(',')

recobk = subprocess.Popen(["hadoop","fs","-put","-",sys.argv[3]],stdin=PIPE)
recoerr = subprocess.Popen(["hadoop","fs","-put","-",sys.argv[4]],stdin=PIPE)
  
for x in backfill_list:
  for member in memberOffers:
	if len(memberOffers[member]) < offerUpLimit :
	    if len(memberOffers[member]) == int(x):
        	memberOfferBk[member] = memberOffers[member]

  for member in memberOfferBk:
    if member not in memberBackfilled:
        lastMember = member
        temp = []
        for sc in fiaScMap:
            if sc not in memberOfferBk[member]:
                for cpn in fiaScMap[sc]:
                    temp.append([sc,cpn,epsilon/offerRank[cpn],offerAvail[cpn]])
    	tempAvail = filter(lambda x: x[3]>0, temp)
        if len(tempAvail) > 0:
    	    tempSorted = sorted(tempAvail,key=lambda tup:tup[2],reverse=True)
	    k = min(len(tempSorted),offerUpLimit - len(memberOfferBk[member]))
            if offerUpLimit - len(memberOfferBk[member]) > len(tempSorted):
		print >> sys.stderr, member + ": not enough popularity backfills"
            for i in range(k):
            	cpn = tempSorted[i][1]
            	sc = tempSorted[i][0]
	        #print ' '.join([lastMember,sc,cpn])
   	        recobk_rec = ' '.join([lastMember,sc,cpn])
	        recobk.stdin.write(recobk_rec+'\n')
                offerAvail[cpn] -= 1
                memberBackfilled[lastMember] = 1
        else:
            #print >> sys.stderr, lastMember + " no_reco"
            ### already used popularity based backfilling, need to check weird results
            #print >> sys.stderr, lastMember + " no_popularity, who had no reco scores"
	    recoerr_rec = lastMember + " no_popularity, who had no reco scores"
	    recoerr.stdin.write(recoerr_rec+'\n')
  memberOfferBk.clear()
  
del memberOffers
recobk.stdin.close()
recoerr.stdin.close()