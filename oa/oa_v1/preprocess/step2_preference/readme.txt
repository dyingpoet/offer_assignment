
#! /bin/bash

#TransData=/user/pythia/Workspaces/SamsMEP/customer_club_day_item_sales_with_sub_category_v2
#StudyCats=/user/pythia/Workspaces/SamsMEP/campaign_sub_cat_summary_570
#OUT=/user/pythia/Workspaces/SamsMEP/TTEScoring/plus/adhoc/2013-11-08/member_item_preference

#pig -p MemList=${MemList} -p TransData=${TransData} -p StudyCats=${StudyCats} -p OUT=${OUT} item_preference.pig

MemList=/user/pythia/Workspaces/SamsMEP/TTEScoring/plus/init/2014-03-10/mem_with_compcards
TransData=/user/pythia/Workspaces/SamsMEP/customer_club_day_item_sales_with_sub_category_v2
#StudyCats=/user/pythia/Workspaces/SamsMEP/campaign_sub_cat_summary_2896
OUT=/user/pythia/Workspaces/SamsMEP/TTEScoring/plus/adhoc/2014-03-10/member_item_preference_for_offer_assignment
DateLB="2012-09-19"
DateUB="2014-03-18"
#SPLB="2012-04-01"
#SPUB="2012-04-30"
#SplitDate="2013-03-19"

pig -p MemList=${MemList} \
    -p TransData=${TransData} \
    -p OUT=${OUT} \
    -p DateLB=${DateLB} \
    -p DateUB=${DateUB} \
#    -p SPLB=${SPLB} \
#    -p SPUB=${SPUB} \
#    -p SplitDate=${SplitDate} \
    -f item_preference_for_offer_assignment.pig




