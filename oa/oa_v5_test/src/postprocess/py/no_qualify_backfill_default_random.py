import sys
from collections import defaultdict
import copy
import random

oaFile = sys.argv[1]
fiaFile = sys.argv[2]
offerUpLimit = int(sys.argv[3])
memberPoolFile = sys.argv[4]
backfillFile = sys.argv[5]

fiaScMap = defaultdict(list)
fiaCpnMap = defaultdict(list)
recoClusterMap = defaultdict(list)
memberOffers= defaultdict(list)
memberSCs= defaultdict(list)
memberOfferOne = defaultdict(list)
memberBackfilled = {}
memberPool = {}
cpn2sc = {}
#memberClusterMap = {}
#memberOffer = {}
offerAvail = {}
offerAssigned = defaultdict(int)
#coupon rank
offerRank = {}
epsilon = 1e-6
backfillList = []

with open(fiaFile,'r') as inputObj:
    for line in inputObj:
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
        cpn2sc[cpn] = sc
        if cpn not in fiaScMap[sc]:
            fiaScMap[sc].append(cpn)
        if sc not in fiaScMap[cpn]:
            fiaCpnMap[cpn].append(sc)

with open(backfillFile,'r') as inputObj:
    for line in inputObj:
        words = line.rstrip().split(' ')
        cpn = words[0]
        backfillList.append(cpn)

with open(oaFile,'r') as inputObj:
    for line in inputObj:
        member, sc, cpn = line.rstrip().split(' ')
        memberSCs[member].append(sc)
        memberOffers[member].append(cpn)
        offerAssigned[cpn] += 1
        offerAvail[cpn] -= 1

offerRankList = sorted([(cpn,offerAssigned[cpn]) for cpn in offerAvail],key=lambda tup:tup[1],reverse=True)
for i in xrange(1,len(offerRankList)+1,1):
    offerRank[offerRankList[i-1][0]] = i

with open(memberPoolFile,'r') as inputObj:
    for line in inputObj:
        member = line.rstrip().split(' ')[0]
        memberPool[member] = 1

backfillSize = len(backfillList)
backfillListOrig = copy.deepcopy(backfillList)

for member in memberPool:
    random.shuffle(backfillList)
    if member not in memberOffers:
        # no offers qualified
        #lastMember = member
        temp = []
        #for sc in fiaScMap:
        #    for cpn in fiaScMap[sc]:
        #        temp.append([sc,cpn,epsilon/offerRank[cpn],offerAvail[cpn]])
        for i in xrange(backfillSize):
            cpn = backfillList[i]
            if cpn not in memberOffers[member]:
                sc = cpn2sc[cpn]
                temp.append([sc,cpn,backfillSize-i,offerAvail[cpn]])
    	tempAvail = filter(lambda x: x[3]>0, temp)
        if len(tempAvail) > 0:
    	    tempSorted = sorted(tempAvail,key=lambda tup:tup[2],reverse=True)
            k = min(len(tempSorted),offerUpLimit)
            if offerUpLimit > len(tempSorted):
                print >> sys.stderr, member + ": not enough backfills for no-qualify members"
            for i in range(k):
               cpn = tempSorted[i][1]
               sc = tempSorted[i][0]
               print ' '.join([member,sc,cpn])
               offerAvail[cpn] -= 1
        else:
            #print >> sys.stderr, member + " no_reco"
            ### already used popularity based backfilling, need to check weird results
            print >> sys.stderr, member + " no_qualify backfilling, who had no available offers"
    else:
        # has qualified offers, but offers not satisfying the upper limit
        if len(memberOffers[member]) < offerUpLimit:
            temp = []
            #for sc in fiaScMap:
            #    if sc not in memberSCs[member]:
            #        for cpn in fiaScMap[sc]:
            #            temp.append([sc,cpn,epsilon/offerRank[cpn],offerAvail[cpn]])
            for i in xrange(backfillSize):
                cpn = backfillList[i]
                if cpn not in memberOffers[member]:
                    sc = cpn2sc[cpn]
                    temp.append([sc,cpn,backfillSize-i,offerAvail[cpn]])
            tempAvail = filter(lambda x: x[3]>0, temp)
            if len(tempAvail) > 0:
                tempSorted = sorted(tempAvail,key=lambda tup:tup[2],reverse=True)
                k = min(len(tempSorted),offerUpLimit - len(memberOffers[member]))
                if offerUpLimit - len(memberOffers[member]) > len(tempSorted):
                    print >> sys.stderr, member + ": not enough popularity backfills"
                for i in range(k):
                   cpn = tempSorted[i][1]
                   sc = tempSorted[i][0]
                   print ' '.join([member,sc,cpn])
                   offerAvail[cpn] -= 1
                #memberBackfilled[member] = 1
            else:
                #print >> sys.stderr, member + " no_reco"
                ### already used popularity based backfilling, need to check weird results
                print >> sys.stderr, member + " no_popularity, who had no available offers"







