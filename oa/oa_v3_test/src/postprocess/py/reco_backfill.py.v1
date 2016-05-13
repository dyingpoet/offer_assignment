import sys
from collections import defaultdict

oaFile = sys.argv[1]
fiaFile = sys.argv[2]
memberClusterFile = sys.argv[3]
recoClusterFile = sys.argv[4]

fiaScMap = defaultdict(list)
recoClusterMap = defaultdict(list)
memberOffers= defaultdict(list)
memberOfferOne = defaultdict(list)
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

with open(oaFile,'r') as inputObj:
    for line in inputObj:
        member, sc, cpn = line.rstrip().split(' ')
        memberOffers[member].append(sc)
        offerAssigned[cpn] += 1
        offerAvail[cpn] -= 1

offerRankList = sorted([(cpn,offerAssigned[cpn]) for cpn in offerAvail],key=lambda tup:tup[1],reverse=True)
for i in xrange(1,len(offerRankList)+1,1):
    offerRank[offerRankList[i-1][0]] = i

for member in memberOffers:
    if len(memberOffers[member]) == 1:
        memberOfferOne[member] = memberOffers[member]

del memberOffers

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
#debug
for member in memberOfferOne:
    print member, memberOfferOne[member]
'''

#print len(offerRank)


'''
### streaming data group by member
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
                else:
                    print >> sys.stderr, lastMember + " no_reco"
            lastMember = member
            temp = []
        if sc not in memberOfferOne[member] and sc in fiaScMap and score>1e-8:
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
    else:
        print >> sys.stderr, lastMember + " no_reco"
