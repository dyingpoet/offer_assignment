#!/bin/bash

set -e

source ./split_bucketing.cfg

#############################################################################################################################################
### use test members and convert item or sub-cat level scores into the general form
#############################################################################################################################################

#if [[ "$RewardLevel" == "item" ]]; 
#then 
#    pig -p scoreAll=$RewardScore -p memberSplit=$MemberTest -p scoreTest=$RewardScoreTest -f test_score_item_flt.pig 1>test_reward_score_item_flt.log 2>&1
#else
#    pig -p scoreAll=$RewardScore -p memberSplit=$MemberTest -p scoreTest=$RewardScoreTest -f test_score_sc_flt.pig 1>test_reward_score_sc_flt.log 2>&1
#fi

#if [[ "$RecoLevel" == "item" ]]; 
#then 
#    pig -p scoreAll=$RecoScore -p memberSplit=$MemberTest -p scoreTest=$RecoScoreTest -f test_score_item_flt.pig 1>test_reco_score_item_flt.log 2>&1
#else
#    pig -p scoreAll=$RecoScore -p memberSplit=$MemberTest -p scoreTest=$RecoScoreTest -f test_score_sc_flt.pig 1>test_reco_score_sc_flt.log 2>&1  
#fi


#############################################################################################################################################
### use attrition deciles to filter reward and reco scores
#############################################################################################################################################

#pig -p scoreAll=$RecoScoreTest -p decile=$MemberDecile -p scoreOut=$RecoScoreTestDecile -p cond="$RecoCond" -f decile_split_member_flex.pig 1> deciles_reco.log 2>&1
#pig -p scoreAll=$RewardScoreTest -p decile=$MemberDecile -p scoreOut=$RewardScoreTestDecile -p cond="$RewardCond" -f decile_split_member_flex.pig 1> deciles_reward.log 2>&1


#############################################################################################################################################
### get the right test members based on the attrition criterion
#############################################################################################################################################

#pig -p memberTestAttitionInclude=$MemberTestAttitionInclude -p decile=$MemberDecile -p cond="$AttritionCond" -p memberTest=$MemberTest -f memberTestAttitionInclude.pig 1 > memberTestAttitionInclude.log 2>&1
#hadoop fs -cat $MemberTestAttitionInclude/p* > ../config/memberTest


#############################################################################################################################################
### lapsed tuning on tenure reward scores
#############################################################################################################################################

if [[ "$LapsedTuning" == "Y" ]]; 
then
    #pig -p lapsedMemberCat=$LapsedMemberCat -p scoreAll=${RewardScoreTestDecile} -p decile=${MemberDecile} -p scoreOut=${RewardScoreTestDecileLapsedTuned} -p "cond=${LapsedCond}" -f scoreLapsedTuning.pig 1> scoreLapsedTuning.log 2>&1
    pig -p lapsedMemberCat=$LapsedMemberCat -p scoreAll=${RewardScoreTest} -p scoreOut=${RewardScoreTestDecileLapsedTuned}  -f scoreLapsedTuning_no_decile.pig 1> scoreLapsedTuning_no_decile.log 2>&1
fi
