import sys

ckpInput=sys.argv[1]
scoreFile=sys.argv[2]
scoreType=sys.argv[3]

scoreDict = {}



#load score file
#9421 215939_12_0001-01-01 0.07670108
with open(scoreFile,'r') as inputObj:
    for line in inputObj:
        words = line.strip().split(' ')
        member = words[0]
        score = words[1]
        scoreDict[member] = "%8f" % float(score)

#iterate the ckp output
#427|||625561776|1435318358|US|722891595|958915722|10|2014-02-23|52721|1|OR $ off|Analytic|REWARD|2911535|224830|2014-12-03|2014-12-24|GeC|0.331122|rew
with open(ckpInput,'r') as inputObj:
    for line in inputObj:
        words = line.strip().split('|')
        membership_nbr = int(words[6])
        cardholder_nbr = int(words[8])
        membership_create_date = words[9]
        member = '_'.join([str(membership_nbr),str(cardholder_nbr),membership_create_date])
        cpn = words[10]
        #sc = coupon2sc[coupon]
        #try:
        #    score = "%8f" % scoreDict[(cpn,member)] 
        #    print line.strip() + '|' + p_score_source + '|' + score + '|' + p_score_type_cd
        #except:
        #    pass
        if member in scoreDict:
            words[21] = scoreType
            words[20] = scoreDict[member]
        print '|'.join(words)





