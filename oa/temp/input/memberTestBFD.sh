

TAG=Apr2015-485

MemCombined=/user/pythia/Workspaces/SamsMEP/MNScoring/OTH485/init/2015-03-26/mem_with_compcards_test_for_oa
MemberTest=/user/pythia/Workspaces/SamsMEP/offer_assignmemt_member_test/campaign_month=${TAG}

#pig -param_file memberTestGeneration.param -f memberTestGeneration.pig 1>memberTestGeneration.log 2>&1
pig -p MemCombined=$MemCombined -p MemberTest=$MemberTest -f memberTestGeneration.pig 1>memberTestGeneration.log 2>&1

hadoop fs -cat /user/pythia/Workspaces/SamsMEP/offer_assignmemt_member_test/campaign_month=${TAG}/* | tr '\t' ' ' > ../config/memberAll
hadoop fs -cat /user/pythia/Workspaces/SamsMEP/offer_assignmemt_member_test/campaign_month=${TAG}/* | tr '\t' '_' > ../config/memberTest


