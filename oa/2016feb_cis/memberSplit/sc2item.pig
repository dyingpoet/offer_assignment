set default_parallel 800


score = LOAD '$scoreSC' USING PigStorage('\t') AS (
    membership_nbr: chararray
    , cardholder_nbr: chararray
    , membership_create_date: chararray
    , cat_subcat_nbr: chararray
    --, system_item_nbr: chararray
    --, value_coupon_nbr: int
    , score: float
);


scoreItem = FOREACH score GENERATE
    membership_nbr AS membership_nbr
    , cardholder_nbr AS cardholder_nbr
    , membership_create_date AS membership_create_date
    , NULL AS system_item_nbr
    , cat_subcat_nbr AS cat_subcat_nbr
    , score AS score
;


STORE scoreItem INTO '$scoreItem' USING PigStorage('\t');
