
#MemCombined=/user/pythia/Workspaces/SamsMEP/MNScoring/Apr2015FY79_477/init/2015-02-09/mem_with_compcards
#MemberTest=/user/pythia/Workspaces/SamsMEP/offer_assignmemt_member_test/campaign_month=Apr2015-79

CAMPAIGN_MONTH=Apr2015-478
MemberTest=/user/pythia/Workspaces/SamsMEP/offer_assignmemt_member_test/campaign_month=${CAMPAIGN_MONTH}

#hadoop fs -put memberTest ${MemberTest}

###../memberSplit/test_score_item_flt.pig  ../memberSplit/test_score_sc_flt.pig

ItemRewardScore=/user/pythia/Workspaces/SamsMEP/MNScoring/Apr2015TY_478/est/2015-03-03/score_items_tenure_season
ItemRewardScoreTest=/user/jli21/pythia/Workspaces/SamsMEP/MNScoring/Apr2015TY_478/est/2015-03-03/score_items_tenure_season_test

SubcatRecoScore=/user/jli21/pythia/Workspaces/SamsMEP/Recommend/Scoring/ThankYou/2014-03-03/Decay/recommend_score_cobought_no_smoothing_max_cosine_cobought_transaction_debug_adj_study_scaled
SubcatRecoScoreTest=/user/jli21/pythia/Workspaces/SamsMEP/Recommend/Scoring/ThankYou/2014-03-03/Decay/recommend_score_cobought_no_smoothing_max_cosine_cobought_transaction_debug_adj_study_scaled_test

pig -p memberSplit=${MemberTest} -p scoreAll=${ItemRewardScore} -p scoreTest=${ItemRewardScoreTest} -f ../memberSplit/test_score_item_flt.pig
pig -p memberSplit=${MemberTest} -p scoreAll=${SubcatRecoScore} -p scoreTest=${SubcatRecoScoreTest} -f ../memberSplit/test_score_sc_flt.pig



