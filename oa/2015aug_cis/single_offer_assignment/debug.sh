#setopt noglob
set -o noglob

#CAMPAIGN_MONTH=Jul2015-532
CAMPAIGN_MONTH=Aug2015-555

cmd="select * from pythia.pis_member_item_subcat_score where (campaign_month='${CAMPAIGN_MONTH}-rew') limit 10;"
echo $cmd
hive -e "$cmd"


cmd="select * from pythia.fia_subcat_item_club where (campaign_month='${CAMPAIGN_MONTH}') limit 10;"
echo -e $cmd
hive -e "$cmd" 


Partition=${CAMPAIGN_MONTH}
cmd="select * from pythia.offer_assigned_member_base_w_club_preference where (campaign_month='${CAMPAIGN_MONTH}') limit 10;"
echo $cmd
hive -e "$cmd"

Partition=${CAMPAIGN_MONTH}-rew
cmd="select * from pythia.pis_member_subcat_item_coupon_score where (campaign_month='$Partition') limit 10;"
echo $cmd
hive -e "$cmd" 



