set default_parallel 800

-- load 

score = LOAD '$scoreInput' USING PigStorage('|') AS (
    membership_nbr: chararray
    , cardholder_nbr: chararray
    , membership_create_date: chararray
    , cat_subcat_nbr: chararray
    , system_item_nbr: chararray
    , value_coupon_nbr: int
    , score: float
);


STORE score INTO '$scoreOutput' USING PigStorage('\u0001');


