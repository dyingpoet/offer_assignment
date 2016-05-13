import sys
from collections import defaultdict

scCouponFile=sys.argv[1]

cpn2sc = {}
sc2cpn = defaultdict(list)

#with open('./sc_coupon_cnt','r') as inputObj:
with open(scCouponFile,'r') as inputObj:
    for line in inputObj:
        sc, cpn, cnt = line.strip().split(' ')
        cpn2sc[cpn] = sc
        sc2cpn[sc].append(cpn)

for line in sys.stdin:
    words = line.rstrip().split('\t')
    member = '_'.join(words[0:3])
    sc = words[3]
    score = words[4]
    if sc in sc2cpn:
        for cpn in sc2cpn[sc]:
            print ' '.join([cpn, member, score])



