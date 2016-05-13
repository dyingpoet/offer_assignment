import sys


### 0103|601|1|3|1|OR $ off|5429119|300000|1.4|0.0|1|1|31|ANALYTIC|Y|618125|1|Core|58679|1018736|Y|FILLER

cntMap = {"Yes":100000000,"Online Only":250000}


for line in sys.stdin:
    words = line.rstrip().split(',')
    cnt = str(cntMap[words[0]])
    for item in words[1:]:
        print ' '.join([item,cnt])

