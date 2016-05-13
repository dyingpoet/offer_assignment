set default_parallel 800

-- load 

score = LOAD '$scoreFlt1' USING PigStorage('\u0001') AS (
    membership_nbr: chararray
    , cardholder_nbr: chararray
    , membership_create_date: chararray
    , cat_subcat_nbr: chararray
    , system_item_nbr: chararray
    , value_coupon_nbr: int
    , score: float
);

-- Need the system_item_nbr in the last reward offer assignment
lastOfferAssignment = LOAD '$lastOfferAssignment' USING PigStorage('\u0001') AS (
    membership_nbr: chararray
    , cardholder_nbr: chararray
    , membership_create_date: chararray
    , cat_subcat_nbr: chararray
    , value_coupon_nbr: int
    , system_item_nbr: chararray
);



-- filter

scoreJoin = JOIN score BY (membership_nbr,cardholder_nbr,membership_create_date,cat_subcat_nbr,system_item_nbr) LEFT, lastOfferAssignment BY (membership_nbr,cardholder_nbr,membership_create_date,cat_subcat_nbr,system_item_nbr);

scoreFlt1 = FILTER scoreJoin BY lastOfferAssignment::system_item_nbr IS NULL;

scoreFlt2 = FOREACH scoreFlt1 GENERATE
    score::membership_nbr
    , score::cardholder_nbr
    , score::membership_create_date
    , score::cat_subcat_nbr
    , score::system_item_nbr
    , score::value_coupon_nbr
    , score::score
;

STORE scoreFlt2 INTO '$scoreFlt2' USING PigStorage('\u0001');


