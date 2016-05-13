import sys
from collections import defaultdict

### bash ${SRC_PREPROCESS_PY_DIR}/adjInputScore.py $FINAL_DIR/offerFile_left $WORK_DIR/scoreFile_bak $WORK_DIR/offer2subcatFile_bak $FINAL_DIR/offerAssignment 1>$WORK_DIR/scoreFile 2>$WORK_DIR/offer2subcatFile

### adjustment of offerFile and scoreFile, filter out the offers assigned

offerFile = sys.argv[1]
scoreFile = sys.argv[2]
offer2scFile = sys.argv[3]
oaFile = sys.argv[4]

offerAvail = {}
#offerAssigned = defaultdict(int)
memberOfferAssigned = {}

with open(oaFile,'r') as inputObj:
    for line in inputObj:
        #member, cpn = line.rstrip().split(' ')
        membership, carholder, createDate, catsubcat, cpn = line.rstrip().split(' ')
        member = '_'.join([membership, carholder, createDate])
        ##offerAssigned[cpn] += 1
        memberOfferAssigned[(member, cpn)] = 1

with open(offerFile,'r') as inputObj:
    for line in inputObj:
        cpn, inv = line.rstrip().split(' ')
        inv = int(inv)
        if inv > 0:
            offerAvail[cpn] = inv

with open(scoreFile,'r') as inputObj:
    for line in inputObj:
        cpn, member, score = line.rstrip().split(' ')
        if cpn in offerAvail and (member, cpn) not in memberOfferAssigned:
            print line.rstrip()


with open(offer2scFile,'r') as inputObj:
    for line in inputObj:
        cpn, sc = line.rstrip().split(' ')
        if cpn in offerAvail:
            print>> sys.stderr, line.rstrip()


