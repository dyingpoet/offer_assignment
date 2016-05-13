-----------------------------------------------------------------------------------------
--  Process Name : OA Automation - Pre-process
--  Script Name  : recommendRankDelta.pig
--  Description  : This pig script will create score output file for recommend.
--  Input Files  : scoreFltFinal, itemGlobalPopularity
--  Output Files : memberHDFS, scoreHDFS
--  Modification Log:
-----------------------------------------------------------------------------------------
--  Author           Version              Date                     Comments
-----------------------------------------------------------------------------------------
--  vmalaya            1.0              09/25/2014          Centralization of OA 
--                                                                                     
-----------------------------------------------------------------------------------------

set default_parallel 800

-- load 

scoretbl = LOAD '$scoreFltFinal' USING PigStorage('|') AS (
    membership_nbr: chararray
    , cardholder_nbr: chararray
    , membership_create_date: chararray
    , cat_subcat_nbr: chararray
    , system_item_nbr: int
    , value_coupon_nbr: int
    , score: float
	, anchor_cat_subcat_nbr: chararray

);


itemGlobalPopularity = LOAD '$itemGlobalPopularity' USING PigStorage('\u0001') AS (
    cat_subcat_nbr: chararray
    , system_item_nbr: int
    , rank: float
);

-- filter

scoretbl1 = FILTER scoretbl BY score>0;

scoreJoin = JOIN scoretbl1 BY (cat_subcat_nbr,system_item_nbr), itemGlobalPopularity BY (cat_subcat_nbr,system_item_nbr) PARALLEL 500;

scoreFlt = FOREACH scoreJoin GENERATE 
    scoretbl1::membership_nbr AS membership_nbr
    , scoretbl1::cardholder_nbr AS cardholder_nbr
    , scoretbl1::membership_create_date AS membership_create_date
    , scoretbl1::cat_subcat_nbr AS cat_subcat_nbr
    , scoretbl1::system_item_nbr AS system_item_nbr
    , scoretbl1::value_coupon_nbr AS value_coupon_nbr
    , scoretbl1::score + 1.0 / itemGlobalPopularity::rank * (float) $delta AS score
;

---- at the coupon level
scoreGrp = GROUP scoreFlt BY (membership_nbr,cardholder_nbr,membership_create_date, value_coupon_nbr);

scoreCoupon = FOREACH scoreGrp GENERATE FLATTEN(group) AS (membership_nbr,cardholder_nbr,membership_create_date, value_coupon_nbr), MAX(scoreFlt.score) AS score; 


scoreFinal = ORDER scoreCoupon BY membership_nbr,cardholder_nbr,membership_create_date ASC, score DESC parallel 1;


scoreFinal = FOREACH scoreFinal GENERATE value_coupon_nbr, CONCAT(CONCAT(CONCAT(membership_nbr,'_'),CONCAT(cardholder_nbr,'_')),membership_create_date) AS member, score;

STORE scoreFinal INTO '$scoreHDFS' USING PigStorage(' ');



members = FOREACH scoreFlt GENERATE CONCAT(CONCAT(CONCAT(membership_nbr,'_'),CONCAT(cardholder_nbr,'_')),membership_create_date) AS member;

member = DISTINCT members;

memberOrder = ORDER member BY member PARALLEL 1;

STORE memberOrder INTO '$memberHDFS' USING PigStorage('\u0001');
