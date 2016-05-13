import sys

cpn2sc = {}
with open('./sc_coupon_cnt','r') as inputObj:
    for line in inputObj:
        sc, cpn, cnt = line.strip().split(' ')
        cpn2sc[cpn] = sc

with open('./offerAssignment','r') as inputObj:
    for line in inputObj:
        member, cpn = line.strip().split(' ')
        sc = cpn2sc[cpn]
        print ' '.join([member, sc, cpn]) 


