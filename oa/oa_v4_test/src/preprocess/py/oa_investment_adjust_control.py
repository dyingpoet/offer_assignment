#!/bin/python
import sys

controlOfferFile=sys.argv[1]
numControl=int(sys.argv[2])
offerAllFile=sys.argv[3]
offerFile=sys.argv[4]
scOfferCntAllFile=sys.argv[5]
scOfferCntFile=sys.argv[6]

controlOfferMap = dict()


with open(controlOfferFile,'r') as inputObj:
    for line in inputObj:
        cpn, = line.rstrip().split('\t')
        controlOfferMap[cpn] = numControl


with open(offerAllFile,'r') as inputObj:
    with open(offerFile,'w') as outputObj:
        for line in inputObj:
            cpn, cnt = line.rstrip().split(' ')
            if cpn in controlOfferMap:
                residue = int(cnt) - controlOfferMap[cpn] 
                if residue > 0:
                    outputObj.write(' '.join([cpn,str(residue)])+'\n')
                else:
                    print>> sys.stderr, "Coupon %s has investment count %s, but assigned %d to the control group, which is over the limit."  % (cpn, cnt, controlOfferMap[cpn])
            else:
                outputObj.write(line)


with open(scOfferCntAllFile,'r') as inputObj:
    with open(scOfferCntFile,'w') as outputObj:
        for line in inputObj:
            sc, cpn, cnt = line.rstrip().split(' ')
            if cpn in controlOfferMap:
                residue = int(cnt) - controlOfferMap[cpn] 
                if residue > 0:
                    outputObj.write(' '.join([sc, cpn,str(residue)])+'\n')
                else:
                    #print>> sys.stderr, "Coupon %s has investment count %s, but assigned %d to the control group, which is over the limit."  % (cpn, cnt, controlOfferMap[cpn])
                    pass
            else:
                outputObj.write(line)







