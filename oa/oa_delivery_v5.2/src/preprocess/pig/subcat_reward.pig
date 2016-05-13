set default_parallel 800

-- load 

score = LOAD '$scoreFltFinal' USING PigStorage('\u0001') AS (
    membership_nbr: chararray
    , cardholder_nbr: chararray
    , membership_create_date: chararray
    , cat_subcat_nbr: chararray
    , system_item_nbr: chararray
    , value_coupon_nbr: int
    , score: float
);


itemMemberPopularity = LOAD '$itemMemberPopularity' USING PigStorage('\u0001') AS (
    membership_nbr: chararray
    , cardholder_nbr: chararray
    , membership_create_date: chararray
    , cat_subcat_nbr: chararray
    , system_item_nbr: chararray
    , rank: float
);



-- filter

score = FILTER score BY score>0;

scoreJoin = JOIN score BY (membership_nbr,cardholder_nbr,membership_create_date,cat_subcat_nbr,system_item_nbr) LEFT, itemMemberPopularity BY (membership_nbr,cardholder_nbr,membership_create_date,cat_subcat_nbr,system_item_nbr) PARALLEL 500;

scoreFlt = FOREACH scoreJoin GENERATE 
    score::membership_nbr AS membership_nbr
    , score::cardholder_nbr AS cardholder_nbr
    , score::membership_create_date AS membership_create_date
    , score::cat_subcat_nbr AS cat_subcat_nbr
    , score::system_item_nbr AS system_item_nbr
    , score::value_coupon_nbr AS value_coupon_nbr
    , (itemMemberPopularity::rank IS NULL ? score::score : score::score + 1.0 / itemMemberPopularity::rank * (float) $delta) AS score
;

---- at the coupon level
scoreGrp = GROUP scoreFlt BY (membership_nbr,cardholder_nbr,membership_create_date, value_coupon_nbr);

scoreCoupon = FOREACH scoreGrp GENERATE FLATTEN(group) AS (membership_nbr,cardholder_nbr,membership_create_date, value_coupon_nbr), MAX(scoreFlt.score) AS score; 


--scoreCouponGrp = GROUP scoreCoupon BY (membership_nbr,cardholder_nbr,membership_create_date);


--scoreFinal = FOREACH scoreCouponGrp {
--    scoreOrder = ORDER scoreCoupon BY score DESC;
--    GENERATE FLATTEN(scoreOrder);
--    --GENERATE FLATTEN(scoreOrder.(membership_nbr, cardholder_nbr, membership_create_date, value_coupon_nbr, score));
--}

scoreFinal = ORDER scoreCoupon BY membership_nbr,cardholder_nbr,membership_create_date ASC, score DESC parallel 1;

--STORE scoreFinal INTO '$scoreRankDelta' USING PigStorage('\u0001');

scoreFinal = FOREACH scoreFinal GENERATE value_coupon_nbr, CONCAT(CONCAT(CONCAT(membership_nbr,'_'),CONCAT(cardholder_nbr,'_')),membership_create_date) AS member, score;

STORE scoreFinal INTO '$scoreRankDelta' USING PigStorage(' ');


members = FOREACH scoreFlt GENERATE CONCAT(CONCAT(CONCAT(membership_nbr,'_'),CONCAT(cardholder_nbr,'_')),membership_create_date) AS member;

member = DISTINCT members;

memberOrder = ORDER member BY member PARALLEL 1;

STORE memberOrder INTO '$memberHDFS' USING PigStorage('\u0001');


