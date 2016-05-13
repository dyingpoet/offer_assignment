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

scoreTest = FILTER score BY (system_item_nbr=='$systemItemNbr');

STORE scoreTest INTO '$scoreTest' USING PigStorage('\t');
