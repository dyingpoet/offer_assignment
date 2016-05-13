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




scoreJoin = JOIN score BY (membership_nbr,cardholder_nbr,membership_create_date,system_item_nbr) LEFT, lastOfferAssignment BY (membership_nbr,cardholder_nbr,membership_create_date,system_item_nbr);

-- find out the coupon numbers in the FIA whose items were assigned last time
scoreFlt1 = FILTER scoreJoin BY lastOfferAssignment::system_item_nbr IS NOT NULL;

couponLast = FOREACH scoreFlt1 GENERATE score::membership_nbr AS membership_nbr, score::cardholder_nbr AS cardholder_nbr, score::membership_create_date AS membership_create_date, score::value_coupon_nbr AS value_coupon_nbr;

couponLastUniq = DISTINCT couponLast;

scoreJoin2 = JOIN score BY (membership_nbr,cardholder_nbr,membership_create_date,value_coupon_nbr) LEFT, couponLastUniq BY (membership_nbr,cardholder_nbr,membership_create_date,value_coupon_nbr);

-- filter
scoreFlt2 = FILTER scoreJoin2 BY couponLastUniq::value_coupon_nbr IS NULL;

scoreOut = FOREACH scoreFlt2 GENERATE
    score::membership_nbr
    , score::cardholder_nbr
    , score::membership_create_date
    , score::cat_subcat_nbr
    , score::system_item_nbr
    , score::value_coupon_nbr
    , score::score
;

STORE scoreOut INTO '$scoreFlt2' USING PigStorage('\u0001');


