set default_parallel 800

lapsedMemberCat = LOAD '$lapsedMemberCat' USING PigStorage('\u0001') AS (
    membership_nbr: int
    , cat_subcat_nbr: chararray
);

score = LOAD '$scoreAll' USING PigStorage('\t') AS (
    membership_nbr: int
    , cardholder_nbr: int
    , membership_create_date: chararray
    , cat_subcat_nbr: chararray
    , system_item_nbr: chararray
    --, value_coupon_nbr: int
    , score: float
);


lapsedMember = DISTINCT (FOREACH lapsedMemberCat GENERATE membership_nbr);

scoreMember =  DISTINCT (FOREACH score GENERATE membership_nbr);

memberJoin = JOIN lapsedMember BY membership_nbr, scoreMember BY membership_nbr; 

joinMember = FOREACH memberJoin GENERATE lapsedMember::membership_nbr AS membership_nbr;

STORE lapsedMember INTO '$lapsedMember' USING PigStorage('\t');
STORE scoreMember INTO '$scoreMember' USING PigStorage('\t');
STORE joinMember INTO '$joinMember' USING PigStorage('\t');


