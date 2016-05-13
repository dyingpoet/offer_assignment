import sys
import datetime


nonofferFile=sys.argv[1]
campaignFile=sys.argv[2]
memberProfileFile=sys.argv[3]


nonoffer = {}

#null|CPM:val_assign_type_cd|null: or nonoffer|CPM:val_assign_type_desc|CPM:package_desc|Manual:system_item_nbr|null|CPM:start_date|manual:end_date(null)|GeC...  |0|manual:item_nbr_backfill

with open(nonofferFile,'r') as inputObj:
    for line in inputObj:
        words = line.rstrip().split('\t')
        nonoffer[words[0]] = words[1]

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
        member = '_'.join([words[6],words[8],words[9]])
        for item in nonoffer:
            print "%s||%s||%s|%s|%s||%s|%s|%s|0.000000|%s|0|%s" % ('|'.join(words),assignmentTypeCd,assignmentTypeDesc,packageDesc,item,startDate,endDate,'GeC','rew',nonoffer[item])






