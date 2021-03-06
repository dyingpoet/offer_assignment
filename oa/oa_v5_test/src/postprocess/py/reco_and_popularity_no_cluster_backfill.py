import sys
from collections import defaultdict

oaFile = sys.argv[1]
fiaFile = sys.argv[2]
#memberClusterFile = sys.argv[3]
#recoClusterFile = sys.argv[4]
offerUpLimit = int(sys.argv[3])

fiaScMap = defaultdict(list)
fiaCpnMap = defaultdict(list)
recoClusterMap = defaultdict(list)
memberOffers= defaultdict(list)
memberOfferOne = defaultdict(list)
memberBackfilled = {}
#memberClusterMap = {}
#memberOffer = {}
offerAvail = {}
offerAssigned = defaultdict(int)
#coupon rank
offerRank = {}
epsilon = 1e-6

with open(fiaFile,'r') as inputObj:
    for line in inputObj:
        words = line.rstrip().split('|')
        cpn = words[17]
        item = words[5]
        inv = int(words[6])
        sc = "%02d%02d" % (int(words[1]),int(words[2]))
        offerAvail[cpn] = inv
        if cpn not in fiaScMap[sc]:
            fiaScMap[sc].append(cpn)
        if sc not in fiaScMap[cpn]:
            fiaCpnMap[cpn].append(sc)

with open(oaFile,'r') as inputObj:
    for line in inputObj:
        member, sc, cpn = line.rstrip().split(' ')
        #memberOffers[member].append(sc)
        memberOffers[member].append(fiaCpnMap[cpn])
        #print fiaCpnMap[cpn]
        #print memberOffers
        #for sc in fiaScMap[cpn]:
        #    memberOffers[member].append(sc)
        offerAssigned[cpn] += 1
        offerAvail[cpn] -= 1

offerRankList = sorted([(cpn,offerAssigned[cpn]) for cpn in offerAvail],key=lambda tup:tup[1],reverse=True)
for i in xrange(1,len(offerRankList)+1,1):
    offerRank[offerRankList[i-1][0]] = i

for member in memberOffers:
    if len(memberOffers[member]) < offerUpLimit:
        memberOfferOne[member] = memberOffers[member]

del memberOffers

'''
with open(recoClusterFile,'r') as inputObj:
    for line in inputObj:
        sc, label = line.rstrip().split('|')
        recoClusterMap[label].append(sc)

with open(memberClusterFile,'r') as inputObj:
    for line in inputObj:
        words = line.rstrip().split('\t')
        membershipNbr = words[9]
        cardholderNbr = words[10]
        membershipCreateDate = words[11]
        label = words[13]
        member = '_'.join([membershipNbr,cardholderNbr,membershipCreateDate])
        if member in memberOfferOne:
            memberOfferOne[member].extend(recoClusterMap[label])
'''

'''
#debug
for member in memberOfferOne:
    print member, memberOfferOne[member]
'''

#print len(offerRank)


'''
### streaming reco data group by member
'''


lastMember = ""
#fin = open('testRecoInput','r')
#for line in fin:
for line in sys.stdin:
    words = line.rstrip().split('\t')
    membershipNbr, cardholderNbr, membershipCreateDate,  sc, score = words
    member = '_'.join([membershipNbr,cardholderNbr,membershipCreateDate])
    score = float(score)
    if member in memberOfferOne:
        if member != lastMember:
            #output lastMember
            if lastMember != "":
            	tempAvail = filter(lambda x: x[3]>0, temp)
                toFillSize = offerUpLimit - len(memberOfferOne[lastMember])
                availFillSize = len(tempAvail)
                if len(tempAvail) > 0:
            	    tempSorted = sorted(tempAvail,key=lambda tup:tup[2],reverse=True)
                    for k in xrange(min(toFillSize,availFillSize)):
            	        #tempSorted = sorted(tempAvail,key=lambda tup:tup[2],reverse=True)
                        #if lastMember == '739932853_10_2014-07-06':
                            #print tempSorted
                        cpn = tempSorted[k][1]
                        sc = tempSorted[k][0]
                        print ' '.join([lastMember,sc,cpn])
                        offerAvail[cpn] -= 1
                    memberBackfilled[lastMember] = 1
                if availFillSize < toFillSize:
                    #print >> sys.stderr, lastMember + " no_reco"
                    ### already used popularity based backfilling, need to check weird results
                    print >> sys.stderr, lastMember + " no_reco_and_popularity"
            temp = []
        ### removed score>1e-8, and use popularity based backfilling in addition to the reco backfilling (make sure the score file contains zero-score records)
        #if sc not in memberOfferOne[member] and sc in fiaScMap and score>1e-8:
        sc = words[3]
        lastMember = member
        #if sc not in memberOfferOne[member] and sc in fiaScMap:
        if sc not in [subcat for l in memberOfferOne[member] for subcat in l] and sc in fiaScMap:
            #print member
            #print sc
            #print [subcat for l in memberOfferOne[member] for subcat in l]
            #for cpn in fiaScMap[sc]:
            #    temp.append([sc,cpn,score+epsilon/offerRank[cpn],offerAvail[cpn]])
            cpnMax = ""
            for cpn in fiaScMap[sc]:
                if cpnMax == "":
                    cpnMax = cpn
                else:
                    if offerRank[cpn] < offerRank[cpnMax]:
                        cpnMax = cpn
            cpn = cpnMax 
            #temp.append([sc,cpn,score+epsilon/offerRank[cpn],offerAvail[cpn]])
            if cpn not in [item[1] for item in temp]:
                temp.append([sc,cpn,score+epsilon/offerRank[cpn],offerAvail[cpn]])
            '''
            '''
        #if member == '739932853_10_2014-07-06':
        #    print sc
        #    print words
        #    print temp



#output lastMember
if lastMember != "":
    #print fiaCpnMap
    #print member
    #print lastMember
    #print memberOfferOne[lastMember]
    #print temp
    tempAvail = filter(lambda x: x[3]>0, temp)
    toFillSize = offerUpLimit - len(memberOfferOne[lastMember])
    availFillSize = len(tempAvail)
    if len(tempAvail) > 0:
        for k in xrange(min(toFillSize,availFillSize)):
            tempSorted = sorted(tempAvail,key=lambda tup:tup[2],reverse=True)
            cpn = tempSorted[k][1]
            sc = tempSorted[k][0]
            print ' '.join([lastMember,sc,cpn])
            offerAvail[cpn] -= 1
        memberBackfilled[lastMember] = 1
    if availFillSize < toFillSize:
        #print >> sys.stderr, lastMember + " no_reco"
        ### already used popularity based backfilling, need to check weird results
        print >> sys.stderr, lastMember + " no_reco_and_popularity"

### member with no qualified reco scores, use popularity
for member in memberOfferOne:
    if member not in memberBackfilled:
        temp = []
        for sc in fiaScMap:
            #if sc not in memberOfferOne[member]:
            if sc not in [subcat for l in memberOfferOne[member] for subcat in l]:
                for cpn in fiaScMap[sc]:
                    #temp.append([sc,cpn,epsilon/offerRank[cpn],offerAvail[cpn]])
                    if cpn not in [item[1] for item in temp]:
                        temp.append([sc,cpn,epsilon/offerRank[cpn],offerAvail[cpn]])
    	tempAvail = filter(lambda x: x[3]>0, temp)
        toFillSize = offerUpLimit - len(memberOfferOne[member])
        availFillSize = len(tempAvail)
        if len(tempAvail) > 0:
            for k in xrange(min(toFillSize,availFillSize)):
    	        tempSorted = sorted(tempAvail,key=lambda tup:tup[2],reverse=True)
                cpn = tempSorted[k][1]
                sc = tempSorted[k][0]
                print ' '.join([member,sc,cpn])
                offerAvail[cpn] -= 1
            memberBackfilled[member] = 1
        if availFillSize < toFillSize:
            #print >> sys.stderr, member + " no_reco"
            ### already used popularity based backfilling, need to check weird results
            print >> sys.stderr, member + " no_reco_and_popularity, who had no reco scores"





