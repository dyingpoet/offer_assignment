import sys
import datetime
from collections import defaultdict


nonofferFile=sys.argv[1]
memberClusterFile=sys.argv[2] 
campaignFile=sys.argv[3]
memberProfileFile=sys.argv[4]


nonoffer = defaultdict(dict)
nonofferCluster = {}

#null|CPM:val_assign_type_cd|null: or nonoffer|CPM:val_assign_type_desc|CPM:package_desc|Manual:system_item_nbr|null|CPM:start_date|manual:end_date(null)|GeC...  |0|manual:item_nbr_backfill

with open(nonofferFile,'r') as inputObj:
    for line in inputObj:
        words = line.rstrip().split('\t')
        label = words[2]
        nonoffer[label][words[0]] = words[1]

with open(memberClusterFile,'r') as inputObj:
    for line in inputObj:
        words = line.rstrip().split()
        membershipNbr = words[0]
        cardholderNbr = words[1]
        membershipCreateDate = words[2]
        label = words[3]
        member = '_'.join([membershipNbr,cardholderNbr,membershipCreateDate])
        nonofferCluster[member] = label

with open(campaignFile,'r') as inputObj:
    for line in inputObj:
        words = line.rstrip().split('\t')
        assignmentTypeCd = words[3]
        assignmentTypeDesc = words[4]
        packageDesc = words[7]
        startDate = words[13]
        duration = int(words[8]) - 1

#import pandas as pd
#startdate = "10/10/2011"
#enddate = pd.to_datetime(startdate) + pd.DateOffset(days=5)

startDateDt = datetime.datetime.strptime(startDate, "%Y-%m-%d")
endDate = (startDateDt + datetime.timedelta(days=duration)).strftime("%Y-%m-%d")

#for item in nonoffer:
#    print "|%s||%s|%s|%s||%s|%s|%s|0|%s|0|%s" % (assignmentTypeCd,assignmentTypeDesc,packageDesc,item,startDate,endDate,'GeC','rew',nonoffer[item])

# cache in member+scores: no reward score for nonoffer items
#with open(scoreFile,'r') as inputObj:
#    for line in inputObj:
#        #cpn, member, score = line.rstrip().split(' ')
#        cpn, member, score = line.rstrip().split('\t')
#        memberScore[(member,cpn)] = score


# stream members: memberProfile
with open(memberProfileFile,'r') as inputObj:
    for line in inputObj:
        words = line.rstrip().split('|')
        member = '_'.join([str(int(words[6])),words[8],words[9]])
        label = nonofferCluster[member]
        for item in nonoffer[label]:
            print "%s||%s||%s|%s|%s||%s|%s|%s|0.000000|%s|0|%s" % ('|'.join(words),assignmentTypeCd,assignmentTypeDesc,packageDesc,item,startDate,endDate,'GeC','rew',nonoffer[label][item])






