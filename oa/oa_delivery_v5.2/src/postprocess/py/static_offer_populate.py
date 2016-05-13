import datetime
import sys

###plain version FIA
fiaFile = sys.argv[1]
campaignFile = sys.argv[2]

staticOffer = sys.argv[3]
team = sys.argv[4]
scoreType = sys.argv[5]

staticOfferMap = {}
outputMemberMap = {}
offerAvail = {}


### read the info of the static offer from the FIA
with open(fiaFile,'r') as inputObj:
    for line in inputObj:
        words = line.rstrip().split('|')
        cpn = words[17]
        offerTypeDesc = words[4]
        item = words[5]
        inv = int(words[6])
        packageDesc = words[12]
        IS = words[14]
        sc = "%02d%02d" % (int(words[1]),int(words[2]))
        #offerAvail[cpn] = inv
        if cpn == staticOffer:
            staticOfferMap['VC'] = cpn
            staticOfferMap['systemItemNbr'] = item
            staticOfferMap['IS'] = IS
            staticOfferMap['packageCode'] = packageDesc
            staticOfferMap['offerTypeDesc'] = offerTypeDesc

### read the campaign file
with open(campaignFile,'r') as inputObj:
    for line in inputObj:
        words = line.rstrip().split('\t')
        assignmentTypeCd = words[3]
        assignmentTypeDesc = words[4]
        packageDesc = words[7]
        startDate = words[13]
        duration = int(words[8]) - 1

startDateDt = datetime.datetime.strptime(startDate, "%Y-%m-%d")
endDate = (startDateDt + datetime.timedelta(days=duration)).strftime("%Y-%m-%d")

staticOfferMap['startDate'] = startDate
staticOfferMap['endDate'] = endDate
staticOfferMap['treatmentTypeCD'] = assignmentTypeCd
staticOfferMap['assignmentTypeDesc'] = assignmentTypeDesc


#{printf "%s|54338|1|OR $ off|Analytic|REWARD|31459826|415692|2015-03-10|2015-03-31|GeC|0|rec|1|\n",$0}' memberProfile > ckp_static
#467|94|10|1|OR $ off|31459826|75000|3.0|0.0|1|1|31|REWARD|N|415692|1|Core|54338|1014425|Y|FILLER
#3228	467-US-MEP_Mar7and9	467	1	Analytic	Y	31	REWARD	22	22	12	22	1	2015-03-10	2015-03-31	NOT USED	NOT USED

### stream and cache the member info from the OA output (columns 1-10) without the static offer
for line in sys.stdin:
    words = line.rstrip().split('|')
    member = '_'.join([words[6],words[8],words[9]])
    if member not in outputMemberMap:
        print '|'.join(words[:10]) + '|' + '|'.join([staticOfferMap['VC'],staticOfferMap['treatmentTypeCD'],staticOfferMap['offerTypeDesc'],staticOfferMap['assignmentTypeDesc'],staticOfferMap['packageCode'],staticOfferMap['systemItemNbr'],staticOfferMap['IS'],staticOfferMap['startDate'],staticOfferMap['endDate'],team,'0.000000',scoreType,'1',''])
        outputMemberMap[member] = 1



