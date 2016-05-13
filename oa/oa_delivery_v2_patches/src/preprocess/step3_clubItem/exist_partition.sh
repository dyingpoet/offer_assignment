
containsElement () {
  local e
  for e in "${@:2}"; do [[ "$e" == "$1" ]] && return 0; done
  return 1
}

#tmp=`hive -e "use pythia; show partitions pis_member_reco_score_anchor" | grep Apr2014-gec | cut -d = -f2`;
#IFS=' ' read -a array <<< "$tmp"
#array=(${tmp//:/ })
tmp=`hive -e "use pythia; show partitions pis_member_reco_score_anchor" | grep Apr2014 | cut -d = -f2`;
array=($tmp)
containsElement $CampaignMonthType "${array[@]}"
#echo $?
#echo $array
#echo ${array[1]}



