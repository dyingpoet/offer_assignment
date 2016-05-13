set default_parallel 800


score = LOAD '$scoreAll' USING PigStorage('\t') AS (
    membership_nbr: chararray
    , cardholder_nbr: chararray
    , membership_create_date: chararray
    , system_item_nbr: chararray
    , cat_subcat_nbr: chararray
    --, value_coupon_nbr: int
    , score: float
);

memberSplit = LOAD '$memberSplit' USING PigStorage('\t') AS (
    membership_nbr: chararray
    , cardholder_nbr: chararray
    , membership_create_date: chararray
    , issuing_country_code: chararray
    , team_id: chararray
    , team_desc: chararray
    , control_ind: int
);

memberSplitTest = FILTER memberSplit BY (control_ind == 0);

scoreJoin = JOIN score BY (membership_nbr), memberSplitTest BY (membership_nbr);

scoreTest = FOREACH scoreJoin GENERATE
    score::membership_nbr AS membership_nbr
    --, score::cardholder_nbr AS cardholder_nbr
    , memberSplitTest::cardholder_nbr AS cardholder_nbr
    --, score::membership_create_date AS membership_create_date
    , memberSplitTest::membership_create_date AS membership_create_date
    , score::system_item_nbr AS system_item_nbr
    , score::cat_subcat_nbr AS cat_subcat_nbr
    , score::score AS score
;


STORE scoreTest INTO '$scoreTest' USING PigStorage('\t');
