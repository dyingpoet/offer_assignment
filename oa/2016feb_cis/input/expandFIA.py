import sys


### 0103|601|1|3|1|OR $ off|5429119|300000|1.4|0.0|1|1|31|ANALYTIC|Y|618125|1|Core|58679|1018736|Y|FILLER


campaign_id = sys.argv[1]

### input: (catsubcat, system_item_nbr, investment_cnt, pseudo_coupon)
for line in sys.stdin:
    catsubcat, item, cnt, vc = line.rstrip().split('\t')
    cat = str(int(catsubcat[0:2]))
    subcat = str(int(catsubcat[2:4]))
    print '|'.join([catsubcat, campaign_id, cat, subcat, '', '', item, cnt] + [''] * 10 + [vc] + [''] * 3)

