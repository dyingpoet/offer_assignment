import sys
from collections import defaultdict

offerFile = sys.argv[1]
scoreFile = sys.argv[2]
offer2scFile = sys.argv[3]

offerAvail = {}
#offerAssigned = defaultdict(int)

with open(offerFile,'r') as inputObj:
    for line in inputObj:
        cpn, inv = line.rstrip().split(' ')
        inv = int(inv)
        if inv > 0:
            offerAvail[cpn] = inv

with open(scoreFile,'r') as inputObj:
    for line in inputObj:
        cpn, member, score = line.rstrip().split(' ')
        if cpn in offerAvail:
            print line.rstrip()


with open(offer2scFile,'r') as inputObj:
    for line in inputObj:
        cpn, sc = line.rstrip().split(' ')
        if cpn in offerAvail:
            print>> sys.stderr, line.rstrip()


