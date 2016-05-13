# scores on item (offer) level

import heapq
from datetime import datetime
import sys
import gc
import numpy
from itertools import izip
from sets import Set

'''
Inputs: 1) scoreFile in line format "offer member score"; 2) offerFile "offer limit"; 3) memberFile "member lowerBound upperBound"; 4) offer2subcatFile "offer subcat".
'''

def run(scoreFile, offerFile, memberFile, offer2subcatFile):
    print '\nProgram started at', str(datetime.now())
    offer2subcat = convert(scoreFile, offerFile, memberFile, offer2subcatFile, 1000000)
    print 'Step 1 preprocess completed at', str(datetime.now()), '\n'
    auctionSparse(offerFile+'.converted', memberFile+'.converted', scoreFile+'.converted', offer2subcat)
    print 'Step 2 auction algorithm completed at', str(datetime.now()), '\n'
    convertFinalResults(memberFile+'.table', offerFile+'.table', scoreFile+'.converted_offerAssignment.output')
    print 'Last step completed at', str(datetime.now()), '\n'

#step 1: preprocess
def constructDict(theFile, kind):
    theDict = dict() #another option is dict(zip(theFile.readlines(), range(numEntities))), but need to linux cut theFile first
    count = 0
    with open(theFile, 'r') as inputObj: #the Python version at the server does not support multiple with-open's in one line
        with open(theFile+'.converted', 'w') as outputObj: 
            with open(theFile+'.table', 'w') as table:
                for line in inputObj:
                    entity, limit = line.split(' ', 1)
                    if kind == 'offer':
                        theDict[entity] = str(count)
                    else:
                        theDict[entity] = [str(count), 0] # the second element will record # eligible offers
                    count += 1
                    table.write(entity+'\n')
                    outputObj.write(limit)
    return theDict

def convert(scoreFile, offerFile, memberFile, offer2subcatFile, multiple):
    offerDict = constructDict(offerFile, 'offer')
    memberDict = constructDict(memberFile, 'member')

    offer2subcat = dict() #key is hashed offer, value is subcat
    with open(offer2subcatFile, 'r') as inputObj:
        for line in inputObj:
            offer, subcat = line.split()
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
    with open(scoreFile, 'r') as inputObj:
        with open(scoreFile+'.converted', 'w') as outputObj:
            for line in inputObj:
                offer, member, score = line.split()
                if offer in offerDict and member in memberDict:
                    memberDict[member][1] += 1
                    outputObj.write(offerDict[offer]+' '+memberDict[member][0]+' '+str(int(float(score)*multiple))+'\n')

    with open(memberFile+'.converted_numEligibleOffers.output', 'w') as outputObj:
        for newId, limit in memberDict.values():
            outputObj.write(newId+' '+str(limit)+'\n')

    with open(scoreFile+'.converted_numOffers_numMembers.output', 'w') as outputObj:
        outputObj.write(str(len(offerDict))+' '+str(len(memberDict))+'\n')
    
    return offer2subcat

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
    
    if maxNumAvailableOffers < lowerBound: # or currentValues[lowerBound-1][1] == -numpy.inf:
        raise Exception('No feasible solutions: Not enough options for customer '+str(me)+'. Her lowerBound is '+str(lowerBound)+', while her maximum number of eligible offers is '+str(maxNumAvailableOffers)+'.')
        
    elif maxNumAvailableOffers == lowerBound:
        return -numpy.inf

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

def bid(numMembers, lowerBounds, upperBounds, baskets, subcatBaskets, offerIDs, values, correspondingSubcats, offerHeaps, prices, indices, maxNumAvailableOffers, maxBiddingTime=24, numIter=3000, epsilon=1):
    currentGain = 0
    startTime = datetime.now()
    for i in xrange(numIter):
        print 'Round', i, 'started. Current objective value:', currentGain, ' Current time', str(datetime.now())
        sys.stdout.flush()
        bidFlag = 0
        for me in xrange(numMembers):
            if me % 100000 == 0:
                #print me, 'members finished bidding at', str(datetime.now())
                sys.stdout.flush()
            lowerBound, upperBound = lowerBounds[me], upperBounds[me]
            
            if len(baskets[me]) == upperBound: #if my basket is full
                continue

            rowStart, rowEnd = indices[me], indices[me+1]
            currentValues = [ [o, s - prices[o], s, sc] for [o, s, sc] in izip(offerIDs[rowStart:rowEnd], values[rowStart:rowEnd], correspondingSubcats[rowStart:rowEnd]) ] # or use a sortedInsert or heap, but then need dynamic malloc
            currentValues.sort(key=lambda tup:tup[1], reverse=True)
            #print i, me, currentValues
            backupValue = computeBackup(currentValues, lowerBound, upperBound, me, maxNumAvailableOffers[me])

            currentItems = [x[0] for x in baskets[me]]
            currentIdx = 0
            for [target, value, score, subcat] in currentValues: # do not use indices[:limit] to decide if the basket is full, since values may have same numbers.
                currentIdx += 1
                if (value<= 0 and len(baskets[me])>=lowerBound) or len(baskets[me])==upperBound:
                    break

                
                if target not in currentItems and subcat not in subcatBaskets[me]:
                    bidFlag = 1

                    # the backup is the max of the second best of the same subcat and the old backup
                    flag = 0
                    resetBackup = 0
                    for [thisTarget, thisValue, thisScore, thisSubcat] in currentValues[currentIdx:]:
                        if subcat == thisSubcat:
                            flag = 1
                        if flag == 1:
                            bid = value - max(backupValue, thisValue) + epsilon
                            resetBackup = 1
                            break
                    if resetBackup == 0:
                        bid = value - backupValue + epsilon
                                
                    price = bid + offerHeaps[target][0][0]

                    kickedOut = heapq.heapreplace(offerHeaps[target], (price, me))
                    if kickedOut[0] == numpy.inf:
                        raise Exception('No feasible solutions')

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
                print '\n**********************************************************************'
                print 'Algorithm converged after '+str(i)+' iteration(s): objectiveValue = '+str(currentGain)
                print '**********************************************************************\n'
                break
            else:
                print 'No feasible solution(s)'
                break

        if (datetime.now() - startTime).seconds/3600.0 > maxBiddingTime:
            print '\n***********************************************************************'
            print 'Max running time hit. Algorithm did NOT converge. Lack of feasible solutions prevents convergence. Take a look at the total gain in each iteration: \
If they were already "periodic", the problem might not be feasible. Otherwise, 1) setting a bigger numIter and maxBiddingTime will help; \
2) setting a bigger epsilon will accelerate convergence but may sacrifice optimality.'
            print '***********************************************************************\n'
            break

    if i == numIter - 1:
        print '\n********************************************************************************************'
        print 'If there is no "algorithm converged..." message right before this message, algorithm did not converge. \
Lack of feasible solutions prevents convergence. Take a look at the total gain in each iteration: \
If they were already "periodic", the problem might not be feasible. Otherwise, 1) setting a bigger numIter and maxBiddingTime will help; \
2) setting a bigger epsilon will accelerate convergence but may sacrifice optimality.'
        print '********************************************************************************************\n'

def auctionSparse(offerFile, memberFile, scoreFile, offer2subcat):

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
        with open(rawAssignment+'Final', 'w') as outputObj:
            for line in inputObj:
                member, offer = line.split()
                outputObj.write(memberHashArray[int(member)][:-1]+' '+offerHashArray[int(offer)])


if __name__ == '__main__':
    #run('testScore1.txt', 'testOffer1.txt', 'testMember1.txt')
    #run('testScore.txt', 'testOffer.txt', 'testMember.txt', 'testCoupon2Subcat.txt')    
    #run('scoreFile', 'offerFile_10', 'memberFile', 'offer2subcatFile')    
    #run('campaign10.scores', 'campaign10.offerConstraints', 'campaign10.memberConstraints')
    run('scoreFile', 'offerFile', 'memberFile', 'offer2subcatFile')   
