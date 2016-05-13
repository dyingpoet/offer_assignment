set default_parallel 800

-- load 

score = LOAD '$scoreFlt' USING PigStorage('|') AS (
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
    , rank: int
);

subcatMember = FOREACH itemMemberPopularity GENERATE membership_nbr, cardholder_nbr, membership_create_date, cat_subcat_nbr;

subcatMemberUniq = DISTINCT subcatMember;


-- filter

scoreJoin = JOIN score BY (membership_nbr,cardholder_nbr,membership_create_date,cat_subcat_nbr) LEFT, subcatMemberUniq BY (membership_nbr,cardholder_nbr,membership_create_date,cat_subcat_nbr);

scoreFlt = FILTER scoreJoin BY subcatMemberUniq::cat_subcat_nbr IS NULL;

scoreFlt1 = FOREACH scoreFlt GENERATE
    score::membership_nbr
    , score::cardholder_nbr
    , score::membership_create_date
    , score::cat_subcat_nbr
    , score::system_item_nbr
    , score::value_coupon_nbr
    , score::score
;

STORE scoreFlt1 INTO '$scoreFlt1' USING PigStorage('\u0001');


