#!/bin/bash

### input variable
# input

MemCombined=/user/pythia/Workspaces/SamsMEP/MNScoring/May201579_fy79_489/init/2015-03-12/mem_with_compcards_with_control 
#Attrition bucketing 
MemberDecile=/user/pythia/Workspaces/SamsMEP/Attrition/May2015FY79_489/pred/2015-03-12/attrition_scores_union
MemberTest=/user/pythia/Workspaces/SamsMEP/offer_assignmemt_member_test/campaign_month=May2015-489
 
#score input location list 
RewardScore=/user/pythia/Workspaces/SamsMEP/MNScoring/May201579_fy79_489/est/2015-03-12/score_items_tenure_season
RecoScore=/user/jli21/pythia/Workspaces/SamsMEP/Recommend/Scoring/79/2015-03-12/Decay/recommend_score_cobought_no_smoothing_max_cosine_cobought_transaction_debug_adj_study_scaled
RewardScoreTest=/user/jli21/pythia/Workspaces/SamsMEP/MNScoring/May201579_fy79_489/est/2015-03-12/score_items_tenure_season_test
RecoScoreTest=${RecoScore}_test

#score type list (item or subcat) 
RewardLevel=item
RecoLevel=subcat
 
RewardCond=">4"
RecoCond="<5"

RewardScoreTestDecile=${RewardScoreTest}_5_plus
RecoScoreTestDecile=${RecoScoreTest}_4_minus

echo $RewardScoreTestDecile
echo $RecoScoreTestDecile


#hadoop fs -mkdir $MemberTest
#hadoop fs -put ../input/memberTest $MemberTest
 

# steps
### 0) generate the test members
##pig -param_file memberTestGeneration.param -f memberTestGeneration.pig 1>memberTestGeneration.log 2>&1
##hadoop fs -cat /user/pythia/Workspaces/SamsMEP/offer_assignmemt_member_test/campaign_month=Apr2015-79/* | tr '\t' ' ' > ../config/member_base_2015apr_477
##hadoop fs -cat /user/pythia/Workspaces/SamsMEP/offer_assignmemt_member_test/campaign_month=Apr2015-79/* | tr '\t' '_' > ../config/memberTest

### 1) test control split
### run control_score_item_flt.pig on reward scores
### run control_score_sc_flt.pig on reco scores

### use member_split table
##pig -param_file control_reco_score_sc_flt.param -f control_score_sc_flt.pig 1>control_reco_score_sc_flt.log 2>&1
##pig -param_file control_reward_score_item_flt.param -f control_score_item_flt.pig 1>control_reco_score_sc_flt.log 2>&1

### use test members

if [[ "$RewardLevel" == "item" ]]; 
then 
    pig -p scoreAll=$RewardScore -p memberSplit=$MemberTest -p scoreTest=$RewardScoreTest -f test_score_item_flt.pig 1>test_reward_score_item_flt.log 2>&1
else
    pig -p scoreAll=$RewardScore -p memberSplit=$MemberTest -p scoreTest=$RewardScoreTest -f test_score_sc_flt.pig 1>test_reward_score_sc_flt.log 2>&1
fi

if [[ "$RecoLevel" == "item" ]]; 
then 
    pig -p scoreAll=$RecoScore -p memberSplit=$MemberTest -p scoreTest=$RecoScoreTest -f test_score_item_flt.pig 1>test_reco_score_item_flt.log 2>&1
else
    pig -p scoreAll=$RecoScore -p memberSplit=$MemberTest -p scoreTest=$RecoScoreTest -f test_score_sc_flt.pig 1>test_reco_score_sc_flt.log 2>&1  
fi

### 2) use attrition deciles to filter out reward for bucket 1
### decile_split_member_flex.pig on (a) reward scores deciles >4 (b) reco <=4

pig -p scoreAll=$RecoScoreTest -p decile=$MemberDecile -p scoreOut=$RecoScoreTestDecile -p cond=$RecoCond -f decile_split_member_flex.pig 1> deciles_reco.log 2>&1
pig -p scoreAll=$RewardScoreTest -p decile=$MemberDecile -p scoreOut=$RewardScoreTestDecile -p cond=$RewardCond -f decile_split_member_flex.pig 1> deciles_reward.log 2>&1


### 3) get the sample scores for the static offer; generate reward.sample.input and reco.sample.input
### getItemScore.pig
### getSubcatScore.pig
### 
### pig -param_file getSubcatRecoScore.param -f getSubcatScore.pig 1>getSubcatRecoScore.log 2>&1
### pig -param_file getItemRewardScore.param -f getItemScore.pig 1>getItemRewardScore.log 2>&1
### 
### SAMPLE_RECO=/user/jli21/pythia/Workspaces/SamsMEP/Recommend/Scoring/79/2014-02-24/Decay/recommend_score_cobought_no_smoothing_max_cosine_cobought_transaction_debug_adj_study_scaled_sample
### SAMPLE_REWARD=/user/jli21/pythia/Workspaces/SamsMEP/MNScoring/Apr2015FY79_477/est/2015-02-24/score_items_tenure_season_sample
### 
### hadoop fs -cat $SAMPLE_RECO/p*   | cut -f1-3,5 | sed 's/\t/_/' | sed 's/\t/_/' | sed 's/\t/ /' > reco.sample.input
### hadoop fs -cat $SAMPLE_REWARD/p* | cut -f1-3,6 | sed 's/\t/_/' | sed 's/\t/_/' | sed 's/\t/ /' > reward.sample.input

