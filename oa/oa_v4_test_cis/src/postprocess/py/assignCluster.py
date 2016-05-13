import sys
from collections import defaultdict

offerFile = sys.argv[1]
memberFile = sys.argv[2]

offerList = defaultdict(list)

with open(offerFile,'r') as inputObj:
    for line in inputObj:
        words = line.rstrip().split(' ')
        offer = words[0]
        clusterId = words[1]
        offerList[clusterId].append(offer)

with open(memberFile,'r') as inputObj:
    for line in inputObj:
        words = line.rstrip().split(' ')
        clusterId = words[-1]
        for offer in offerList[clusterId]:
            #print line.rstrip() + ' ' + offer
            print '_'.join(words[:-1]) + ' ' + offer


