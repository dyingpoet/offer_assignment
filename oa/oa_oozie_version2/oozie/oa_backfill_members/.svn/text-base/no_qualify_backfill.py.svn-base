#!/usr/bin/python
#-----------------------------------------------------------------------------------------
#--  Process Name : OA Automation - Post-process
#--  Script Name  : no_qualify_backfill.py
#--  Description  : This python script will perform  backfill process for members without scores and generate member_sc_cpn_no_qualify_backfill file.
#--  Input Files  : member_sc_cpn_qualify, sc_coupon_cnt, memberTest
#--  Output Files : member_sc_cpn_no_qualify_backfill, member_sc_cpn_no_qualify_fail_backfill
#--  Modification Log: 
#-----------------------------------------------------------------------------------------
#--  Author           Version              Date                     Comments
#-----------------------------------------------------------------------------------------
#--  phala1            1.0              02/20/2015                    Creation 
#--                                                                      
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


offerUpLimit = int(sys.argv[3])
fiaScMap = defaultdict(list)
recoClusterMap = defaultdict(list)
memberOffers= defaultdict(list)
memberSCs= defaultdict(list)
memberOfferBk = defaultdict(list)
memberBackfilled = {}
#memberClusterMap = {}
#memberOffer = {}
offerAvail = {}
offerAssigned = defaultdict(int)
#coupon rank
offerRank = {}
epsilon = 1e-6
memberPool = {}




#with open(fiaFile,'r') as inputObj:

#-------------------------------------------------------------
#opening fiaFile in read mode
#-------------------------------------------------------------
inputObj1 = subprocess.Popen(["hadoop","fs","-cat",sys.argv[2]],stdout=subprocess.PIPE)

for line in inputObj1.stdout:
        '''
        words = line.rstrip().split('|')
        cpn = words[17]
        item = words[5]
        inv = int(words[6])
        sc = "%02d%02d" % (int(words[1]),int(words[2]))
        offerAvail[cpn] = inv
        if cpn not in fiaScMap[sc]:
            fiaScMap[sc].append(cpn)
        '''
        sc, cpn, inv = line.rstrip().split(' ')
        offerAvail[cpn] = int(inv)
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
        memberSCs[member].append(sc)
        memberOffers[member].append(cpn)
        offerAssigned[cpn] += 1
#       offerAvail[cpn] -= 1

inputObj2.stdout.close()

offerRankList = sorted([(cpn,offerAssigned[cpn]) for cpn in offerAvail],key=lambda tup:tup[1],reverse=True)
for i in xrange(1,len(offerRankList)+1,1):
    offerRank[offerRankList[i-1][0]] = i

#-------------------------------------------------------------
#opening memberTest in read mode
#-------------------------------------------------------------

inputObj3 = subprocess.Popen(["hadoop","fs","-cat",sys.argv[4]],stdout=subprocess.PIPE)
for line in inputObj3.stdout:
        member = line.rstrip().split(' ')[0]
        memberPool[member] = 1

inputObj3.stdout.close()

recobk = subprocess.Popen(["hadoop","fs","-put","-",sys.argv[5]],stdin=PIPE)
recoerr = subprocess.Popen(["hadoop","fs","-put","-",sys.argv[6]],stdin=PIPE)


for member in memberPool:
    if member not in memberOffers:
        lastMember = member
        temp = []
        for sc in fiaScMap:
            for cpn in fiaScMap[sc]:
                    temp.append([sc,cpn,epsilon/offerRank[cpn],offerAvail[cpn]])
    	tempAvail = filter(lambda x: x[3]>0, temp)
        if len(tempAvail) > 0:
    	    tempSorted = sorted(tempAvail,key=lambda tup:tup[2],reverse=True)
            k=min(len(tempSorted),offerUpLimit)
            if offerUpLimit > len(tempSorted):
                 print >> sys.stderr, member + ": not enough backfills for no-qualify members"
	    for i in range(k):
               cpn = tempSorted[i][1]
               sc = tempSorted[i][0]
               #print ' '.join([lastMember,sc,cpn])
   	       recobk_rec = ' '.join([lastMember,sc,cpn])
	       recobk.stdin.write(recobk_rec+'\n')
               offerAvail[cpn] -= 1
	       memberOffers[lastMember] = 1               
        else:
	    #print >> sys.stderr, member + " no_reco"
            ### already used popularity based backfilling, need to check weird results
            #print >> sys.stderr, member + " no_qualify backfilling, who had no available offers"
	    recoerr_rec = lastMember + "  no_qualify backfilling, who had no available offers"
	    recoerr.stdin.write(recoerr_rec+'\n')
    else:
            # has qualified offers, but offers not satisfying the upper limit
            if len(memberOffers[member]) < offerUpLimit:
                lastMember = member
                temp = []
                for sc in fiaScMap:
                    if sc not in memberSCs[member]:
                        for cpn in fiaScMap[sc]:
                            temp.append([sc,cpn,epsilon/offerRank[cpn],offerAvail[cpn]])
                tempAvail = filter(lambda x: x[3]>0, temp)
                if len(tempAvail) > 0:
                    tempSorted = sorted(tempAvail,key=lambda tup:tup[2],reverse=True)
                    k = min(len(tempSorted),offerUpLimit - len(memberOffers[member]))
                    if offerUpLimit - len(memberOffers[member]) > len(tempSorted):
                        print >> sys.stderr, member + ": not enough popularity backfills"
                    for i in range(k):
                        cpn = tempSorted[i][1]
                        sc = tempSorted[i][0]
                        #print ' '.join([member,sc,cpn])
                        recobk_rec = ' '.join([lastMember,sc,cpn])
                        recobk.stdin.write(recobk_rec+'\n')
                        offerAvail[cpn] -= 1
                        memberOffers[lastMember] = 1
            else:
                    #print >> sys.stderr, member + " no_reco"
                    ### already used popularity based backfilling, need to check weird results
                    #print >> sys.stderr, member + " no_popularity, who had no available offers"
		    recoerr_rec = lastMember + " no_popularity, who had no available offers"
                    recoerr.stdin.write(recoerr_rec+'\n')           
memberPool.clear()
del memberOffers
recobk.stdin.close()
recoerr.stdin.close()
