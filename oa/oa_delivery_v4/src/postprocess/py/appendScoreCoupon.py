import sys

ckpInput=sys.argv[1]
fiaFile=sys.argv[2]
scoreFile=sys.argv[3]
p_score_source=sys.argv[4]
p_score_type_cd=sys.argv[5]

coupon2sc = {}
scoreDict = {}

#load fia
with open(fiaFile,'r') as inputObj:
    for line in inputObj:
        words = line.strip().split('|')
        #sc = words[0]
        #coupon = words[18]
        sc = "%02d%02d" % (int(2),int(1))
        coupon = words[17]
        coupon2sc[coupon] = sc

with open(ckpInput,'r') as inputObj:
    for line in inputObj:
        words = line.strip().split('|')
        membership_nbr = int(words[6])
        cardholder_nbr = int(words[8])
        membership_create_date = words[9]
        member = '_'.join([str(membership_nbr),str(cardholder_nbr),membership_create_date])
        cpn = words[10]
        scoreDict[(cpn,member)] = 0 

#load score file
#9421 215939_12_0001-01-01 0.07670108
with open(scoreFile,'r') as inputObj:
    for line in inputObj:
        words = line.strip().split(' ')
        cpn = words[0]
        member = words[1]
        score = float(words[2])
        if (cpn,member) in scoreDict:
            scoreDict[(cpn,member)] = score

#iterate the ckp output
with open(ckpInput,'r') as inputObj:
    for line in inputObj:
        words = line.strip().split('|')
        membership_nbr = int(words[6])
        cardholder_nbr = int(words[8])
        membership_create_date = words[9]
        member = '_'.join([str(membership_nbr),str(cardholder_nbr),membership_create_date])
        cpn = words[10]
        sc = coupon2sc[coupon]
        try:
            score = "%8f" % scoreDict[(cpn,member)] 
            print line.strip() + '|' + p_score_source + '|' + score + '|' + p_score_type_cd
        except:
            pass



