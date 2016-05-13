RewardScoreTest=/user/jli21/pythia/Workspaces/SamsMEP/MNScoring/Jun510FY79/est/2015-04-23/score_items_tenure_season
MemberDecile=/user/pythia/Workspaces/SamsMEP/Attrition/Jun2015FY79/pred/2015-04-29/attrition_scores_union
RewardCond='(decile_nbr>=3\ and\ decile_nbr<=8)'
#RewardCond="(decile_nbr>=3)"
RewardScoreTestDecile=${RewardScoreTest}_3_plus


RewardScoreTestDecileLapsedTuned=${RewardScoreTestDecile}_lapsed_tuned
LapsedCond='(decile_nbr>=3\ and\ decile_nbr<=4)'

LapsedMemberCat=/user/jli21/sams/lapsed_member_subcat

ScoreMember=/user/jli21/pythia/Workspaces/SamsMEP/lapsed/2015jun79/2015-05-04/scoreMember
LapsedMember=/user/jli21/pythia/Workspaces/SamsMEP/lapsed/2015jun79/2015-05-04/lapsedMember
JoinMember=/user/jli21/pythia/Workspaces/SamsMEP/lapsed/2015jun79/2015-05-04/joinMember

LapsedMemberCatSubset=/user/jli21/pythia/Workspaces/SamsMEP/lapsed/2015jun79/2015-05-04/lapsedMemberCatSubset
ScoreJoin=/user/jli21/pythia/Workspaces/SamsMEP/lapsed/2015jun79/2015-05-04/scoreJoin

#pig -p scoreAll=${RewardScoreTest} -p decile=${MemberDecile} -p scoreOut=${RewardScoreTestDecile} -p "cond=${RewardCond}" -f decile_split_member_flex.pig 1> deciles_reward.log 2>&1

pig -p lapsedMemberCat=$LapsedMemberCat -p scoreAll=${RewardScoreTestDecile} -p decile=${MemberDecile} -p scoreOut=${RewardScoreTestDecileLapsedTuned} -p "cond=${LapsedCond}" -f scoreLapsedTuning.pig 1> scoreLapsedTuning.log 2>&1
#pig -p lapsedMemberCatSubset=$LapsedMemberCatSubset -p scoreJoin=$ScoreJoin -p lapsedMemberCat=$LapsedMemberCat -p scoreAll=${RewardScoreTestDecile} -p decile=${MemberDecile} -p scoreOut=${RewardScoreTestDecileLapsedTuned} -p "cond=${LapsedCond}" -f scoreLapsedTuning.pig 1> scoreLapsedTuning.log 2>&1

#pig -p lapsedMemberCat=$LapsedMemberCat -p scoreAll=${RewardScoreTestDecile} -p decile=${MemberDecile} -p scoreOut=${RewardScoreTestDecileLapsedTuned} -p "cond=${LapsedCond}" -p lapsedMember=$LapsedMember -p scoreMember=$ScoreMember -p joinMember=$JoinMember -f debug.pig 1> debug.log 2>&1

