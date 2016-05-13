import sys

offerFile = sys.argv[1]
memberFile = sys.argv[2]

offerList = []

with open(offerFile,'r') as inputObj:
    for line in inputObj:
        words = line.rstrip().split(' ')
        offerList.append(words[0])

with open(memberFile,'r') as inputObj:
    for line in inputObj:
        words = line.rstrip().split(' ')
        for offer in offerList:
            #print line.rstrip() + ' ' + offer
            print '_'.join(words) + ' ' + offer


