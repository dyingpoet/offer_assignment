-----------------------------------------------------------------------------------------
--  Process Name : Offer_Assignment-Reward filtering
--  Script Name  : subcat_reward.pig
--  Description  : This will generate subcat score and member file
--  Parameters   : scoreFltFinal, itemMemberPopularity, scoreRankDelta,memberHDFS
--  Input Files  : scoreFltFinal, itemMemberPopularity
--  Output Files : scoreRankDelta,memberHDFS
--
--  Modification Log:
-----------------------------------------------------------------------------------------
--  Author           Version              Date                     Comments
-----------------------------------------------------------------------------------------
--  sgopa4            1.0              09/29/2014          Centralization of Pscore
--                                                                                     
-----------------------------------------------------------------------------------------
set default_parallel 800

-- load 

score = LOAD '$scoreFltFinal' USING PigStorage('|') AS (
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

score1 = FILTER score BY score>0;

scoreJoin = JOIN score1 BY (membership_nbr,cardholder_nbr,membership_create_date,cat_subcat_nbr,system_item_nbr), itemMemberPopularity BY (membership_nbr,cardholder_nbr,membership_create_date,cat_subcat_nbr,system_item_nbr) PARALLEL 500;

scoreFlt = FOREACH scoreJoin GENERATE 
    score1::membership_nbr AS membership_nbr
    , score1::cardholder_nbr AS cardholder_nbr
    , score1::membership_create_date AS membership_create_date
    , score1::cat_subcat_nbr AS cat_subcat_nbr
    , score1::system_item_nbr AS system_item_nbr
    , score1::value_coupon_nbr AS value_coupon_nbr
    , score1::score + 1.0 / itemMemberPopularity::rank * (float) $delta AS score
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


