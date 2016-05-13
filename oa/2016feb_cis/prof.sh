#setopt noglob
set -o noglob

source reward_offer_assignment_param.cfg

testfunc () { echo "$# parameters"; echo "$@"; }

getTop () {
    local DB=$1
    local Partition=$2
    local Key=$3
    Cmd="select * from $DB where ($Key='$Partition') limit 10;"
    echo $Cmd;
    hive -e "$Cmd"
}

#getTop pythia.pis_member_item_subcat_score Jun2015-510-rew campaign_month
getTop ${DATABASE}.${SCORE_TBL} ${CAMPAIGN_MONTH_TYPE} campaign_month
getTop ${DATABASE}.${SUBCAT_ITEM_CLUB_TBL} ${CAMPAIGN_MONTH} campaign_month
getTop ${DATABASE}.${MEMBER_CLUB_PREF_TBL} ${CAMPAIGN_MONTH} campaign_month
getTop ${DATABASE}.${ITEM_COUPON_SCORE_TBL} ${CAMPAIGN_MONTH_TYPE} campaign_month

#testfunc a b c
#cmd="select * from pythia.fia_subcat_item_club where (campaign_month='Jun2015-510') limit 10;"
#cmd="select * from pythia.offer_assigned_member_base_w_club_preference where (campaign_month='Jun2015-510') limit 10;"
#Partition=Jun2015-510-rew
#cmd="select * from pythia.pis_member_subcat_item_coupon_score where (campaign_month='$Partition') limit 10;"



