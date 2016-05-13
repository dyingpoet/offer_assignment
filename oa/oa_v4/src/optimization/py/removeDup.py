import sys

idx = int(sys.argv[1])
sep = sys.argv[2]

map = {}


for line in sys.stdin:
    words = line.rstrip().split(sep)
    if words[idx] not in map:
        map[words[idx]] = 1
        print sep.join(words)



