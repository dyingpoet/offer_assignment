import sys
from collections import defaultdict

oaFile = sys.argv[1]
offerFile = sys.argv[2]
pct = float(sys.argv[3])

keepRate = 1.0 * pct / (1 - pct)

offerAvail = {}
offerAssigned = defaultdict(int)

with open(offerFile,'r') as inputObj:
    for line in inputObj:
        cpn, inv = line.rstrip().split(' ')
        inv = int(inv)
        offerAvail[cpn] = inv

with open(oaFile,'r') as inputObj:
    for line in inputObj:
        member, cpn = line.rstrip().split(' ')
        offerAssigned[cpn] += 1
        offerAvail[cpn] -= 1

#for cpn in offerAvail:
for cpn in offerAssigned:
    inv = min(int(keepRate * offerAssigned[cpn]), offerAvail[cpn])
    #if offerAvail[cpn] > 0:
    #    print ' '.join([cpn, str(offerAvail[cpn])])
    if inv > 0:
        print ' '.join([cpn, str(inv)])
