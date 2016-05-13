
export CAMPAIGN_MONTH=Feb2016-684
export MemberTest=/user/pythia/Workspaces/SamsMEP/offer_assignmemt_member_test/campaign_month=${CAMPAIGN_MONTH}
export RecoScore=/user/jli21/pythia/Workspaces/SamsMEP/Recommend/Scoring/ClubInsider/2016-01-23/Decay/recommend_score_cobought_no_smoothing_max_cosine_cobought_transaction_debug_adj_study_scaled
export RecoScoreTest=${RecoScore}_test
#export LapsedScore=/user/pythia/Workspaces/SamsMEP/LapsedScoring/ProdData/lapsed_scores_subcat/segment=Oct2015_CIS_2015-09-15
#export LapsedScoreTest=/user/jli21/pythia/Workspaces/SamsMEP/LapsedScoring/ProdData/lapsed_scores_subcat/segment=Oct2015_CIS_2015-09-15_test

#bash getLasped.sh

if ! hadoop fs -test -d ${MemberTest}; then
    hadoop fs -mkdir $MemberTest
    hadoop fs -put ../input/memberTest $MemberTest
fi



#    pig -p scoreAll=$RecoScore -p memberSplit=$MemberTest -p scoreTest=$RecoScoreTest -f test_score_sc_flt.pig 1>test_reco_score_sc_flt.log 2>&1  
    pig -p scoreAll=$LapsedScore -p memberSplit=$MemberTest -p scoreTest=$LapsedScoreTest -f test_score_sc_flt.pig 1>test_lapsed_score_sc_flt.log 2>&1  

