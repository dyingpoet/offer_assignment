#!/bin/python
import sys

controlOfferFile=sys.argv[1]


controlOfferMap = dict()


with open(controlOfferFile,'r') as inputObj:
    for line in inputObj:
        cpn, = line.rstrip().split('\t')
        controlOfferMap[cpn] = 1

for line in sys.stdin:
    for cpn in controlOfferMap:
        print line.strip() + ' ' + cpn



