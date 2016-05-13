import sys
from collections import defaultdict

oaFile = sys.argv[1]
offerFile = sys.argv[2]

offerAvail = {}
#offerAssigned = defaultdict(int)

with open(offerFile,'r') as inputObj:
    for line in inputObj:
        cpn, inv = line.rstrip().split(' ')
        inv = int(inv)
        offerAvail[cpn] = inv

with open(oaFile,'r') as inputObj:
    for line in inputObj:
        #member, cpn = line.rstrip().split(' ')
        membership, carholder, createDate, catsubcat, cpn = line.rstrip().split(' ')
        ##offerAssigned[cpn] += 1
        offerAvail[cpn] -= 1

for cpn in offerAvail:
    if offerAvail[cpn] > 0:
        print ' '.join([cpn, str(offerAvail[cpn])])
