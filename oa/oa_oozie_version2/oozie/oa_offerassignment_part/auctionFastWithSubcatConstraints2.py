#!/usr/bin/python
#-----------------------------------------------------------------------------------------
#--  Process Name : OA Automation - Core process
#--  Script Name  : auctionFastWithSubcatConstraints2.pyg
#--  Description  : This python script will generate offer assignment output file.
#--  Input Files  : 1) scoreFile in line format "offer member score"; 2) offerFile "offer limit"; 
#--					3) memberFile "member lowerBound upperBound"; 4) offer2subcatFile "offer subcat".
#--  Output Files : Offerassignment
#--  Modification Log: 
#-----------------------------------------------------------------------------------------
#--  Author           Version              Date                     Comments
#-----------------------------------------------------------------------------------------
#--  cchan1            1.0              09/25/2014          Centralization of OA 
#--                                                                                     
#-----------------------------------------------------------------------------------------

#!/usr/bin/env /usr/bin/python
# scores on item (offer) level

import sys
import heapq
from datetime import datetime
import gc
from itertools import izip
from sets import Set
# Included to read/write file from HDFS
import subprocess
from subprocess import Popen, PIPE
# Model Ops Reporting

from email.mime.text import MIMEText
import json
import smtplib
import time

def mops_report(message):

    """ Send a mail message for notification """

    server = 'smtp.wal-mart.com'
    port = 25
    sender = 'mark.conway0@walmart.com'
    receiver = sender

    model = 'Offer Assignment'
    now = time.strftime("%c")

    mimemsg = MIMEText(message)
    mimemsg['From'] = sender
    mimemsg['To'] = receiver
    mimemsg['Subject'] = ' run on '.join([model, now])

    try:
        smtp_server = smtplib.SMTP(server, port)
        smtp_server.sendmail(sender, receiver, mimemsg.as_string())
        print "Model Ops Report: Successfully sent Email"
    except Exception:
        print "Model Ops Report: Unable to send Email"
# commented out due to non availability of numpy packages in all data-nodes for python 2.6
#import numpy

# Start of Code
'''
Inputs: 1) scoreFile in line format "offer member score"; 2) offerFile "offer limit"; 3) memberFile "member lowerBound upperBound"; 4) offer2subcatFile "offer subcat".
'''
offer2subcat = dict() #key is hashed offer, value is subcat

def run(scoreFile, offerFile, memberFile, offer2subcatFile):
    print '\nProgram started at', str(datetime.now())
    convert(scoreFile, offerFile, memberFile, offer2subcatFile, 1000000)
    print 'Step 1 preprocess completed at', str(datetime.now()), '\n'
    auctionSparse(offerFile+'.converted', memberFile+'.converted', scoreFile+'.converted')
    print 'Step 2 auction algorithm completed at', str(datetime.now()), '\n'
    convertFinalResults(memberFile+'.table', offerFile+'.table', scoreFile+'.converted_offerAssignment.output')
    print 'Last step completed at', str(datetime.now()), '\n'

#step 1: preprocess
def constructDict(theFile, kind):
    theDict = dict() #another option is dict(zip(theFile.readlines(), range(numEntities))), but need to linux cut theFile first
    count = 0
    cat1 = subprocess.Popen(["hadoop","fs","-cat",sys.argv[7]+theFile+"/campaign_iter="+sys.argv[9]+"/*"],stdout=subprocess.PIPE)
    
    with open(theFile+'.converted', 'w') as outputObj: 
        with open(theFile+'.table', 'w') as table:
            for line in cat1.stdout:
                    entity, limit = line.split(' ', 1)
                    if kind == 'offer':
                        theDict[entity] = str(count)
                    else:
                        theDict[entity] = [str(count), 0] # the second element will record # eligible offers
                    count += 1
                    table.write(entity+'\n')
                    outputObj.write(limit)

    cat1.stdout.close()
    return theDict

def convert(scoreFile, offerFile, memberFile, offer2subcatFile, multiple):
    offerDict = constructDict(offerFile, 'offer')
    memberDict = constructDict(memberFile, 'member')
    
    #subcat2offerListDict = dict() # key is subcat, value is a list of hashed offer
    #with open(offer2subcatFile, 'r') as inputObj:
    cat_o2scf = subprocess.Popen(["hadoop","fs","-cat",sys.argv[5]],stdout=subprocess.PIPE)
    for line in cat_o2scf.stdout:
            offer, subcat = line.split()
            # if subcat not in subcatSet:
            #     subcatSet.add(subcat)
            #     subcat2offerListDict[subcat] = [offerDict[offer]]
            # else:
            #     subcat2offerListDict[subcat].append(offerDict[offer])
            offer2subcat[int(offerDict[offer])] = subcat

    # if score on subcat level
    # with open(scoreFile, 'r') as inputObj:
    #     with open(scoreFile+'.converted', 'w') as outputObj:
    #         for line in inputObj:
    #             offer, member, score = line.split()
    #             if offer in subcatSet and member in memberDict:
    #                 memberDict[member][1] += len(subcat2offerListDict[offer])
    #                 for c in subcat2offerListDict[offer]:
    #                     outputObj.write(c+' '+memberDict[member][0]+' '+str(int(float(score)*multiple))+'\n')

    # if score on offer level
    #with open(scoreFile, 'r') as inputObj:
    cat_sf = subprocess.Popen(["hadoop","fs","-cat",sys.argv[6]],stdout=subprocess.PIPE)
    with open(scoreFile+'.converted', 'w') as outputObj:
            for line in cat_sf.stdout:
                offer, member, score = line.split()
                if offer in offerDict and member in memberDict:
                    memberDict[member][1] += 1
                    outputObj.write(offerDict[offer]+' '+memberDict[member][0]+' '+str(int(float(score)*multiple))+'\n')

    with open(memberFile+'.converted_numEligibleOffers.output', 'w') as outputObj:
        for newId, limit in memberDict.values():
            outputObj.write(newId+' '+str(limit)+'\n')

    with open(scoreFile+'.converted_numOffers_numMembers.output', 'w') as outputObj:
        outputObj.write(str(len(offerDict))+' '+str(len(memberDict))+'\n')
    
    cat_o2scf.stdout.close()
    cat_o2scf.wait()
    cat_sf.stdout.close()
    cat_sf.wait()

# Step 2: auction algorithm    
def computeGain(baskets):
    totalGain = 0
    for basket in baskets:
          for [offer, score] in basket:
            totalGain += score
    return totalGain/1000000.0

def isFeasible(numMembers, baskets, lowerBounds, upperBounds):
    for m in xrange(numMembers):
        if len(baskets[m]) < lowerBounds[m] or len(baskets[m]) > upperBounds[m]:
            return False
    return True

def computeBackup(currentValues, lowerBound, upperBound, me, maxNumAvailableOffers):
    #compute backupValue
   
    if maxNumAvailableOffers < lowerBound: # or currentValues[lowerBound-1][1] == -numpy.inf:
        print currentValues
        mops_message = 'No feasible solutions: Not enough options for customer '+str(me)+'. The lowerBound is '+str(lowerBound)+', while the maximum number of eligible offers is '+str(maxNumAvailableOffers)+'.'
        mops_report(mops_message)
        raise Exception(mops_message)
        #raise Exception('No feasible solutions')

    elif maxNumAvailableOffers == lowerBound:
        #backupValue = -numpy.inf
		#assign -max integer value instead of -infinity
        backupValue = -sys.maxint

    else:
        # find the index for the first negative current value
        if currentValues[-1][1] > 0: # if no negative
            firstNegative = -1
        else:
            firstNegative = next(i for i, v in enumerate(currentValues) if v[1] <=0)
            #firstNegative += 1

        backupIndex = -1    
        backupIndexLower = -1
        scSoFar = Set()    
        for i, v in enumerate(currentValues):
            scSoFar.add(v[3])
            if len(scSoFar) == lowerBound+1 and backupIndexLower == -1:
                backupIndexLower = i
            if len(scSoFar) == upperBound+1:
                backupIndex = i
                break

        #print me, firstNegative, backupIndex, backupIndexLower
        if firstNegative == -1:
            if maxNumAvailableOffers > upperBound:
                backupValue = currentValues[backupIndex][1]
            else:
                backupValue = 0
        else:
            numOffersBeforeNegative = len(Set([sc for [x,y,z,sc] in currentValues[0:firstNegative] ]))
            if numOffersBeforeNegative <= lowerBound:
                backupValue = currentValues[backupIndexLower][1]
            elif numOffersBeforeNegative > lowerBound and numOffersBeforeNegative <= upperBound:
                backupValue = 0
                #backupValue = currentValues[firstNegative][1]
            else:
                backupValue = currentValues[backupIndex][1]
    return backupValue

def bid(numMembers, lowerBounds, upperBounds, baskets, subcatBaskets, offerIDs, values, correspondingSubcats, offerHeaps, prices, indices, maxNumAvailableOffers, numIter=3000, epsilon=1):
    gainList = [0 for x in xrange(30)]
    currentGain = 0
    for i in xrange(numIter):
        print 'Round', i, 'started. Current objective value:', currentGain, ' Current time', str(datetime.now())
        sys.stdout.flush()
        bidFlag = 0
        for me in xrange(numMembers):
            if me % 100000 == 0:
                #print me, 'members finished bidding at', str(datetime.now())
                sys.stdout.flush()
            lowerBound, upperBound = lowerBounds[me], upperBounds[me]
            #print me, baskets[me], subcatBaskets[me]
            if len(baskets[me]) == upperBound: #if my basket is full
                continue

            rowStart, rowEnd = indices[me], indices[me+1]
            currentValues = [ [o, s - prices[o], s, sc] for [o, s, sc] in izip(offerIDs[rowStart:rowEnd], values[rowStart:rowEnd], correspondingSubcats[rowStart:rowEnd]) ] # or use a sortedInsert or heap, but then need dynamic malloc
            currentValues.sort(key=lambda tup:tup[1], reverse=True)
            backupValue = computeBackup(currentValues, lowerBound, upperBound, me, maxNumAvailableOffers[me])

            currentItems = [x[0] for x in baskets[me]]
            for [target, value, score, subcat] in currentValues: # do not use indices[:limit] to decide if the basket is full, since values may have same numbers.
                if (value<= 0 and len(baskets[me])>=lowerBound) or len(baskets[me])==upperBound:
                    break

                
                #print offer2subcat
                if target not in currentItems and subcat not in subcatBaskets[me]:
                    #print me, baskets[me], subcatBaskets[me], target, currentValues, prices
                    bidFlag = 1
                    bid = value - backupValue + epsilon
                    price = bid + offerHeaps[target][0][0]

                    kickedOut = heapq.heapreplace(offerHeaps[target], (price, me))
                    #if kickedOut[0] == numpy.inf:
		    #assigned max integer value instead of infinity
		    if kickedOut[0] == sys.maxint:
                        mops_message = 'No feasible solutions in bid routine (sys.maxint)'
                        mops_report(mops_message)						
                        raise Exception(mops_message)

                    if kickedOut[1] != -1:
                        subcatBaskets[kickedOut[1]].remove(subcat)
                        theBasket = baskets[kickedOut[1]]

                        for x in xrange(len(theBasket) - 1, -1, -1):
                            if theBasket[x][0] == target:
                                del theBasket[x]
                                break # removable_ip is allegedly unique

                    baskets[me].append([target, score])
                    subcatBaskets[me].append(subcat)
                    prices[target] = offerHeaps[target][0][0]

        previousGain = currentGain
        currentGain = computeGain(baskets)
        gc.collect()

        if bidFlag == 0:
            if isFeasible(numMembers, baskets, lowerBounds, upperBounds):
                print 'Algorithm converged after '+str(i)+' iteration(s): objectiveValue = '+str(currentGain)
                break
            else:
                mops_message = 'No feasible solutions(s) in bid routine (bidFlag ==0)'
		mops_report(mops_message)
                print 'No feasible solution(s)'
                break

    if i == numIter - 1:
        mops_message = 'If there is no "algorithm converged..." message right before this message, algorithm did not converge. \
Lack of feasible solutions prevents convergence. Take a look at the total gain in each iteration: \
If they were already "periodic", the problem might not have feasible solutions. Otherwise, setting a bigger numIter will help.'
        mops_report(mops_message)
        print(mops_message)

def auctionSparse(offerFile, memberFile, scoreFile):

    with open(scoreFile+'_numOffers_numMembers.output', 'r') as inputObj:
        numOffers, numMembers = [int(x) for x in inputObj.readline().split()]

    # offer side    
    with open(offerFile, 'r') as offers:
        offerHeaps = [ [(0, -1) for i in xrange(int(offer))] for offer in offers.readlines()] # # offers is small, OK to read in all
    for h in offerHeaps:
        heapq.heapify(h)
    prices = [0]*numOffers
    print 'Offer data loaded at', str(datetime.now())

    # member side: perhaps process the memberFile here
    lowerBounds = [0]*numMembers
    upperBounds = [10]*numMembers
    memberCount = 0
    with open(memberFile) as inputObj:
        for line in inputObj:
            lowerBounds[memberCount], upperBounds[memberCount]  = [int(x) for x in line.split()]
            memberCount += 1

    #memberConstraints = [[2,20] for m in xrange(numMembers)]
    baskets = [[] for i in xrange(numMembers)] #store the offers the member got so for, the size is small, can append/remove
    subcatBaskets = [[] for i in xrange(numMembers)] #store the subcats the member got so for, ensuring each member can at most one offer in each subcat
    print 'Member data loaded at', str(datetime.now())

    # load scores 
    eligibleOffers = [0]*numMembers
    with open(memberFile+'_numEligibleOffers.output', 'r') as inputObj:
        for line in inputObj.readlines():
            member, num = [int(x) for x in line.split()]
            eligibleOffers[member] = num 

    indices = [0]*(numMembers+1) #the start index of each row
    cumulativeSum = 0        
    for i in xrange(numMembers):        
        indices[i] = cumulativeSum
        cumulativeSum += eligibleOffers[i]
    indices[-1] = cumulativeSum # this is also # scores
    numScores = cumulativeSum
    #print indices
    
    offerIDs = [0]*numScores
    values = [0]*numScores 
    correspondingSubcats = [0]*numScores
    numOffersSoFar = [0]*numMembers
    with open(scoreFile, 'r') as inputObj:
        for line in inputObj:
            offer, member, score = [int(x) for x in line.split()]
            myIndex = indices[member]+numOffersSoFar[member]
            offerIDs[myIndex], values[myIndex], correspondingSubcats[myIndex] = offer, score, offer2subcat[offer]
            numOffersSoFar[member] += 1                
    del numOffersSoFar

    maxNumAvailableOffers = [0]*numMembers
    for me in xrange(numMembers):
        maxNumAvailableOffers[me] = len(Set(correspondingSubcats[indices[me]:indices[me+1]]))

    print 'Score data loaded at', str(datetime.now())

    bid (numMembers, lowerBounds, upperBounds, baskets, subcatBaskets, offerIDs, values, correspondingSubcats, offerHeaps, prices, indices, maxNumAvailableOffers)
    
    with open(scoreFile+'_offerAssignment.output', 'w') as outputObj:
        for m in xrange(numMembers):
            for [item, score] in baskets[m]:
                outputObj.write(str(m)+' '+str(item)+'\n')
    
# Step 3: output
def convertFinalResults(memberTable, offerTable, rawAssignment):
    with open(memberTable, 'r') as inputObj:
        memberHashArray = inputObj.readlines()

    with open(offerTable, 'r') as inputObj:
        offerHashArray = inputObj.readlines()

    with open(rawAssignment, 'r') as inputObj:
        #with open(rawAssignment+'Final', 'w') as outputObj:
        put = subprocess.Popen(["hadoop","fs","-put","-",sys.argv[8]],stdin=PIPE)
        for line in inputObj:
             member, offer = line.split()
             #outputObj.write(memberHashArray[int(member)][:-1]+' '+offerHashArray[int(offer)])
             put.stdin.write(memberHashArray[int(member)][:-1]+' '+offerHashArray[int(offer)])
          
        put.stdin.close()
        put.wait()

if __name__ == '__main__':
    #python auctionFastWithSubcatConstraints2.py scoreFile offerFile memberFile offer2subcatFile
    scoreFile = sys.argv[1] 
    offerFile = sys.argv[2] 
    memberFile = sys.argv[3] 
    offer2subcatFile = sys.argv[4] 
    run(scoreFile, offerFile, memberFile, offer2subcatFile)    
    #run('testScore1.txt', 'testOffer1.txt', 'testMember1.txt')
    #run('scoreFile', 'offerFile_10', 'memberFile', 'coupon_sc.map')    
    #run('campaign26.score.sorted_member_item.score','campaign26.score.sorted_OfferConstraints','campaign26.score.sorted_members')
    #run('campaign10.scores', 'campaign10.offerConstraints', 'campaign10.memberConstraints')
