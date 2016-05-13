set default_parallel 80


memberSplitTest = LOAD '$memberTest' USING PigStorage('\t') AS (
    membership_nbr: int
    , cardholder_nbr: chararray
    , membership_create_date: chararray
);

/* table deciles from Wei Prod 6 */
decile = LOAD '$decile' USING PigStorage('\t') AS (
    membership_nbr: int
    , membership_create_date: chararray
    , next_renew_date: chararray
    --, cardholder_nbr: int
    , score: float
    , decile_nbr: int
);


decile = FILTER decile BY $cond;

memberJoin = JOIN decile BY (membership_nbr,membership_create_date), memberSplitTest BY (membership_nbr,membership_create_date);

memberTestAttitionInclude = FOREACH memberJoin GENERATE
    memberSplitTest::membership_nbr AS membership_nbr
    , memberSplitTest::cardholder_nbr AS cardholder_nbr
    , memberSplitTest::membership_create_date AS membership_create_date
;

STORE memberTestAttitionInclude INTO '$memberTestAttitionInclude' USING PigStorage('_');
