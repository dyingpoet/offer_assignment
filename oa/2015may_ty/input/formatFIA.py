#!/usr/bin/python
import sys 

for line in sys.stdin:
    words = line.rstrip().split('\t')
    catsubcat = "%02d%02d" % (int(words[1]),int(words[2]))
    del words[20]
    print catsubcat + '|' + '|'.join(words)

