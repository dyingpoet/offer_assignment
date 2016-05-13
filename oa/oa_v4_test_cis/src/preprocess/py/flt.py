import sys

removeFile = sys.argv[1]
idx = int(sys.argv[2])
sep = sys.argv[3]

removeDict = {}

with open(removeFile,'r') as inputObj:
    for line in inputObj:
        words = line.strip().split('\t')
        removeDict[words[0]] = 1

for line in sys.stdin:
    words = line.strip().split(sep)
    if words[idx] not in removeDict:
        print line.strip()
    else:
        print >> sys.stderr, line.strip()


