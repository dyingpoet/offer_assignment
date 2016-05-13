set default_parallel 800

%default lapsed '/user/jli21/sams/lapsed_member_subcat';
%default cpnScore '/user/pythia/Workspaces/SamsMEP/pis_offer_assignment_score_file/campaign_month=Feb2016-684-rew';
%default subcatCpn '/user/pythia/Workspaces/SamsMEP/offerAssignment/2016feb684/subcatCpn';
%default scoreOut '/user/pythia/Workspaces/SamsMEP/offerAssignment/2016feb684/cpn_reward_score_lapsedInd';

/*
1) lapsed info (membership, subcat)
/user/jli21/sams/lapsed_member_subcat/
2) fia (cat_subcat_nbr, cpn)
/user/pythia/Workspaces/SamsMEP/offerAssignment/2016feb684/subcatCpn 
3) score (member, vc, score) 
/user/pythia/Workspaces/SamsMEP/pis_offer_assignment_score_file/campaign_month=Oct2015-601-rew
58635 100003698_12_0001-01-01 0.5068666
*/

lapsed = LOAD '$lapsed' Using PigStorage('\u0001') AS (membership_nbr:chararray, cat_subcat_nbr:chararray);

cpnScore = LOAD '$cpnScore' Using PigStorage(' ') AS (cpn:int, member:chararray, score:float);

cpnSubcat = LOAD '$subcatCpn' Using PigStorage('|') AS (cat_subcat_nbr:chararray, cpn:int);


lapsedJoinCpn = JOIN lapsed BY cat_subcat_nbr, cpnSubcat BY cat_subcat_nbr;

lapsedCpn = FOREACH lapsedJoinCpn GENERATE lapsed::membership_nbr AS membership_nbr, cpnSubcat::cpn AS cpn;

lapsedCpnUniq = DISTINCT lapsedCpn;

--cpnScoreMembershipNbr = FOREACH cpnScore GENERATE *, (chararray) STRSPLIT(member, '_').$0 AS membership_nbr;
cpnScoreMembershipNbr = FOREACH cpnScore GENERATE *, FLATTEN(STRSPLIT(member, '_')) AS (membership_nbr:chararray, carholder_nbr:chararray, membership_create_date:chararray);

scoreJoinLapsed = JOIN cpnScoreMembershipNbr BY (membership_nbr,cpn) LEFT, lapsedCpnUniq BY (membership_nbr,cpn);

scoreOut = FOREACH scoreJoinLapsed GENERATE 
    cpnScoreMembershipNbr::cpn AS cpn
    , cpnScoreMembershipNbr::member AS member
    , cpnScoreMembershipNbr::score AS score
    , (lapsedCpnUniq::membership_nbr IS NULL ? 0 : 1) AS lapsedInd
;

STORE scoreOut INTO '$scoreOut' USING PigStorage('\t');

