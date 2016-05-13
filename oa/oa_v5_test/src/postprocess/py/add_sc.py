import sys

#scCouponFile=sys.argv[1]
#oaFile=sys.argv[2]

cpn2sc = {}


# #with open('./sc_coupon_cnt','r') as inputObj:
# with open(scCouponFile,'r') as inputObj:
#     for line in inputObj:
#         sc, cpn, cnt = line.strip().split(' ')
#         cpn2sc[cpn] = sc
# 
# #with open('./offerAssignment','r') as inputObj:
# with open(oaFile,'r') as inputObj:
#     for line in inputObj:
#         member, cpn = line.strip().split(' ')
#         sc = cpn2sc[cpn]
#         print ' '.join([member, sc, cpn]) 

try:
    with open(sys.argv[1],'r') as inputObj:
        for line in inputObj:
            sc, cpn, cnt = line.strip().split(' ')
            cpn2sc[cpn] = sc
except:
    with open('./sc_coupon_cnt','r') as inputObj:
        for line in inputObj:
            sc, cpn, cnt = line.strip().split(' ')
            cpn2sc[cpn] = sc


try:
    with open(sys.argv[2],'r') as inputObj:
        for line in inputObj:
            member, cpn = line.strip().split(' ')
            sc = cpn2sc[cpn]
            print ' '.join([member, sc, cpn]) 
except:
    with open('./offerAssignment','r') as inputObj:
        for line in inputObj:
            member, cpn = line.strip().split(' ')
            sc = cpn2sc[cpn]
            print ' '.join([member, sc, cpn]) 



