import sys

for line in sys.stdin:
    words = line.strip().split('\t')
    member = "%09d" % (int(words[3]),)
    words[3] =  member + '|' + member[7:9] + member[3:7] + member[0:3]
    print "281|||" + '|'.join(words)
    
