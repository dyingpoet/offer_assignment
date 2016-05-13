import sys
from collections import defaultdict

### pipe in scoreFile
### python ${SRC_PREPROCESS_PY_DIR}/adjMemberFile.py $FINAL_DIR/offerAssignment $MEMBER_UB > $WORK_DIR/memberFile

oaFile = sys.argv[1]
ub = int(sys.argv[2])
ub_all = int(sys.argv[3])

memberOA = defaultdict(int)
memberScore = {}

### load oa
with open(oaFile,'r') as inputObj:
    for line in inputObj:
        member, cpn = line.rstrip().split(' ')
        memberOA[member] += 1

### pipe in scoreFile and get the member list
for line in sys.stdin:
    #cpn, member, score = line.rstrip().split(' ')
    member = line.rstrip().split(' ')[0]
    memberScore[member] = 1

for member in memberScore:
    _ub = min(ub_all - memberOA[member], ub)
    _lb = min(1, _ub)
    print ' '.join([member, str(_lb), str(_ub)])

