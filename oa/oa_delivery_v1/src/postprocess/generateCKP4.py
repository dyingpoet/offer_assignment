import sys
from collections import defaultdict

#b.MDS_FAM_ID as item_nbr, b.COUPON_ITEM_NBR as is_item_nbr
campaignNbr = sys.argv[1]

fiaFile = sys.argv[2]

map_coupon_item_nbr = {}

#one coupon can be linked to multiple items
#map_system_item_nbr = {}
map_system_item_nbr = defaultdict(list)

counts = {}

def argmin(x, items):
    if len(items) == 1:
        return items[0]
    y = [x[item] for item in items]
    return items[y.index(min(y))]
     

with open(fiaFile,'r') as inputObj:
#with open('/home/jli21/sams/auction/2014jan/v4/offers.txt','r') as inputObj:
    for line in inputObj:
        CAMPAIGN_NBR,SUBCLASS_NBR,DEPT_NBR,VAL_OFFER_TYPE_CODE,VAL_OFFER_TYPE_DESC,MDS_FAM_ID,INVESTMENT_CNT,VALUE_AMT,VALUE_PCT,MIN_ITEM_PURCH_QTY,MAX_REDEMPTION_CNT,PACKAGE_CODE,PACKAGE_DESC,VENDOR_FUNDED_IND,COUPON_ITEM_NBR,VAL_ITEM_TYPE_CODE,VAL_ITEM_TYPE_DESC,VALUE_COUPON_NBR,PRVDR_COUPON_NBR,CLUB_AVAIL_IND,FILLER_DATA, INV= line.strip().split('|')
        map_system_item_nbr[VALUE_COUPON_NBR].append(MDS_FAM_ID)
        map_coupon_item_nbr[VALUE_COUPON_NBR] = COUPON_ITEM_NBR
        counts[MDS_FAM_ID] = 0


for line in sys.stdin:
    words = line.strip().split('\t')
    member = "%09d" % (int(words[3]),)
    words[3] =  member + '|' + member[7:9] + member[3:7] + member[0:3]
    valueCouponNbr = words[6]
    #words[11] = map_system_item_nbr[valueCouponNbr]
    items = map_system_item_nbr[valueCouponNbr]
    item = argmin(counts, items)
    words[11] = item
    counts[item] += 1
    words[12] = map_coupon_item_nbr[valueCouponNbr]
    print  campaignNbr +"|||" + '|'.join(words)
    
