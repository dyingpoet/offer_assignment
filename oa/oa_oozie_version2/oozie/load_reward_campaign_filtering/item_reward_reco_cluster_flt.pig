-----------------------------------------------------------------------------------------
--  Process Name : Offer_Assignment-Reward filtering
--  Script Name  : item_reward_reco_cluster_flt.pig
--  Description  : This will generate subcat score and member file including cluster
--  Parameters   : scoreFltFinal, clusterLabelFile, clusterRecoFile, scoreFinal,memberHDFS
--  Input Files  : scoreFltFinal, clusterLabelFile, clusterRecoFile
--  Output Files : scoreFinal,memberHDFS
--
--  Modification Log:
-----------------------------------------------------------------------------------------
--  Author           Version              Date                     Comments
-----------------------------------------------------------------------------------------
--  phala1            1.0              09/29/2014          Centralization of Pscore
--                                                                                     
-----------------------------------------------------------------------------------------
set default_parallel 800

-- load 

score0 = LOAD '$scoreFltFinal' USING PigStorage('|') AS (
    membership_nbr: chararray
    , cardholder_nbr: chararray
    , membership_create_date: chararray
    , cat_subcat_nbr: chararray
    , system_item_nbr: chararray
    , value_coupon_nbr: int
    , score: float
    , anchor_cat_subcat_nbr: chararray
);


clusterLabel = LOAD '$clusterLabelFile' USING PigStorage('\t') AS (
    cluster_source: chararray
    , cluster_create_date: chararray
    , version_nbr: int
    , propensity_score_source: chararray
    , campaign_id: int
    , vt_individual_customer_id: chararray
    , vt_customer_household_id: chararray
    , experian_household_id: chararray
    , experian_individual_id: chararray
    , membership_nbr: chararray
    , cardholder_nbr: chararray
    , membership_create_date: chararray
    , issuing_country_code: chararray
    , cluster_id: int
);

clusterReco = LOAD '$clusterRecoFile' USING PigStorage('|') AS (
    cat_subcat_nbr: chararray
    , cluster_id: int
);

-- filter
score = FILTER score0 BY score>0;

-- add cluster id
scoreLabelJoin = JOIN score BY (membership_nbr,cardholder_nbr,membership_create_date), clusterLabel BY (membership_nbr,cardholder_nbr,membership_create_date) USING 'replicated';

scoreLabel = FOREACH scoreLabelJoin GENERATE 
    score::membership_nbr AS membership_nbr
    , score::cardholder_nbr AS cardholder_nbr
    , score::membership_create_date AS membership_create_date
    , score::cat_subcat_nbr AS cat_subcat_nbr
    , score::system_item_nbr AS system_item_nbr
    , score::value_coupon_nbr AS value_coupon_nbr
    , score::score AS score
    , clusterLabel::cluster_id AS cluster_id
;



-- filter RECO subcat
scoreRecoJoin = JOIN scoreLabel BY (cat_subcat_nbr,cluster_id) LEFT, clusterReco BY (cat_subcat_nbr,cluster_id) USING 'replicated';

scoreRecoFlt = FILTER scoreRecoJoin BY clusterReco::cluster_id IS NULL;

scoreFlt = FOREACH scoreRecoFlt GENERATE
    scoreLabel::membership_nbr AS membership_nbr
    , scoreLabel::cardholder_nbr AS cardholder_nbr
    , scoreLabel::membership_create_date AS membership_create_date
    , scoreLabel::cat_subcat_nbr AS cat_subcat_nbr
    , scoreLabel::system_item_nbr AS system_item_nbr
    , scoreLabel::value_coupon_nbr AS value_coupon_nbr
    , scoreLabel::score AS score
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

STORE scoreFinal INTO '$scoreFinal' USING PigStorage(' ');


members = FOREACH scoreFlt GENERATE CONCAT(CONCAT(CONCAT(membership_nbr,'_'),CONCAT(cardholder_nbr,'_')),membership_create_date) AS member;

member = DISTINCT members;

memberOrder = ORDER member BY member PARALLEL 1;

STORE memberOrder INTO '$memberHDFS' USING PigStorage('\u0001');


