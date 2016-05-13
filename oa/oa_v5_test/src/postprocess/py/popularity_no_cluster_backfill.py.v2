import sys
from collections import defaultdict

oaFile = sys.argv[1]
fiaFile = sys.argv[2]
#memberClusterFile = sys.argv[3]
#recoClusterFile = sys.argv[4]
offerUpLimit = int(sys.argv[3])

fiaScMap = defaultdict(list)
recoClusterMap = defaultdict(list)
memberOffers= defaultdict(list)
memberSCs= defaultdict(list)
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

for member in memberOffers:
    #if len(memberOffers[member]) == 1:
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
                if len(tempAvail) > 0:
            	    tempSorted = sorted(tempAvail,key=lambda tup:tup[2],reverse=True)
                    cpn = tempSorted[0][1]
                    sc = tempSorted[0][0]
                    print ' '.join([lastMember,sc,cpn])
                    offerAvail[cpn] -= 1
                    memberBackfilled[lastMember] = 1
                else:
                    #print >> sys.stderr, lastMember + " no_reco"
                    ### already used popularity based backfilling, need to check weird results
                    print >> sys.stderr, lastMember + " no_reco_and_popularity"
            lastMember = member
            temp = []
        ### removed score>1e-8, and use popularity based backfilling in addition to the reco backfilling (make sure the score file contains zero-score records)
        #if sc not in memberOfferOne[member] and sc in fiaScMap and score>1e-8:
        if sc not in memberOfferOne[member] and sc in fiaScMap:
            for cpn in fiaScMap[sc]:
                temp.append([sc,cpn,score+epsilon/offerRank[cpn],offerAvail[cpn]])



#output lastMember
if lastMember != "":
    tempAvail = filter(lambda x: x[3]>0, temp)
    if len(tempAvail) > 0:
        tempSorted = sorted(tempAvail,key=lambda tup:tup[2],reverse=True)
        cpn = tempSorted[0][1]
        sc = tempSorted[0][0]
        print ' '.join([lastMember,sc,cpn])
        offerAvail[cpn] -= 1
        memberBackfilled[lastMember] = 1
    else:
        #print >> sys.stderr, lastMember + " no_reco"
        ### already used popularity based backfilling, need to check weird results
        print >> sys.stderr, lastMember + " no_reco_and_popularity"
'''

for member in memberOfferOne:
    if member not in memberBackfilled:
        #lastMember = member
        temp = []
        for sc in fiaScMap:
            if sc not in memberSCs[member]:
                for cpn in fiaScMap[sc]:
                    temp.append([sc,cpn,epsilon/offerRank[cpn],offerAvail[cpn]])
    	tempAvail = filter(lambda x: x[3]>0, temp)
        if len(tempAvail) > 0:
    	    tempSorted = sorted(tempAvail,key=lambda tup:tup[2],reverse=True)
            k = min(len(tempSorted),offerUpLimit - len(memberOfferOne[member]))
            if offerUpLimit - len(memberOfferOne[member]) > len(tempSorted):
                print >> sys.stderr, member + ": not enough popularity backfills"
            for i in range(k):
               cpn = tempSorted[i][1]
               sc = tempSorted[i][0]
               print ' '.join([member,sc,cpn])
               offerAvail[cpn] -= 1
            memberBackfilled[member] = 1
        else:
            #print >> sys.stderr, member + " no_reco"
            ### already used popularity based backfilling, need to check weird results
            print >> sys.stderr, member + " no_popularity, who had no available offers"





