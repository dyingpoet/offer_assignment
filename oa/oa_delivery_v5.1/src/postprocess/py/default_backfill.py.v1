import sys
from collections import defaultdict

fiaFile = sys.argv[1]
memberClusterFile = sys.argv[2]
recoClusterFile = sys.argv[3]
defaultBackfillFile = sys.argv[4]
oaFile = sys.argv[5]

fiaScMap = defaultdict(list)
recoClusterMap = defaultdict(list)
defaultBackfillMap = defaultdict(list)
memberOffers= defaultdict(list)
memberOfferOne = defaultdict(list)
memberClusterMap = {}
fiaCpnMap = {}
offerAvail = {}
offerAssigned = defaultdict(int)
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
            fiaCpnMap[cpn] = sc

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

with open(defaultBackfillFile,'r') as inputObj:
    for line in inputObj:
        cpn, label = line.rstrip().split(',')
        defaultBackfillMap[label].append(cpn)

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
            memberClusterMap[member] = label


for member in memberOfferOne:
    label = memberClusterMap[member]
    n = len(defaultBackfillMap[label])
    idx = 0
    for cpn in defaultBackfillMap[label]:
        sc = fiaCpnMap[cpn]
        if sc not in memberOfferOne[member]:
            print ' '.join([member,sc,cpn])
            break
        idx += 1 
    if idx == n:
        print >> sys.stderr, member+" no_default_backfill"








