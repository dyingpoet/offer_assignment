#setopt noglob
set -o noglob


Campaign_Month=Oct2015-601
Partition=Oct2015-601-rew
cmd="select * from pythia.pis_member_item_subcat_score where (campaign_month='$Partition') limit 10;"
echo $cmd
hive -e "$cmd"


cmd="select * from pythia.fia_subcat_item_club where (campaign_month='$Campaign_Month') limit 10;"
echo -e $cmd
hive -e "$cmd" 


cmd="select * from pythia.offer_assigned_member_base_w_club_preference where (campaign_month='$Campaign_Month') limit 10;"
#echo $cmd
#hive -e "$cmd"

cmd="select * from pythia.pis_member_subcat_item_coupon_score where (campaign_month='$Partition') limit 10;"
echo $cmd
#hive -e "$cmd" 



