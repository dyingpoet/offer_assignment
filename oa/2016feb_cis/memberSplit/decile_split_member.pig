set default_parallel 800


score = LOAD '$scoreAll' USING PigStorage('\t') AS (
    membership_nbr: int
    , cardholder_nbr: int
    , membership_create_date: chararray
    , cat_subcat_nbr: chararray
    , system_item_nbr: chararray
    --, value_coupon_nbr: int
    , score: float
);

-- 731209128	10	2014-05-01	US	2	GEC	437	0	2014-11-22	0.29569075	10

/* table deciles from Wei Prod 6 */
decile = LOAD '$decile' USING PigStorage('\t') AS (
    membership_nbr: int
    , membership_create_date: chararray
    , next_renew_date: chararray
    --, cardholder_nbr: int
    , score: float
    , decile_nbr: int
);


/* table deciles from Ganesh
decile = LOAD '$decile' USING PigStorage('\t') AS (
    membership_nbr: int
    , cardholder_nbr: int
    , membership_create_date: chararray
    , issuing_country_code: chararray
    , team_id: chararray
    , attrition_score_source: chararray
    , campaign_nbr
    , control_group: chararray
    , ds: chararray
    , score: float
    , decile_nbr: int
);
*/


/* table deciles_op_sample_data 
decile = LOAD '$decile' USING PigStorage('\t') AS (
    membership_nbr int,
    , membership_create_date string,
    , issuing_country_code string,
    , cardholder_nbr int,
    , next_renew_date string,
    , score float,
    , attrition_score_source string,
    , attrition_score_type_cd string,
    , decile_nbr int,
    , decile_input_score_type_nm string,
    , decile_group_nbr int,
    , campaign_nbr
);
*/


decile = FILTER decile BY decile_nbr>=5;

--scoreJoin = JOIN score BY (membership_nbr,cardholder_nbr,membership_create_date), decile BY (membership_nbr,cardholder_nbr,membership_create_date);
scoreJoin = JOIN score BY (membership_nbr,membership_create_date), decile BY (membership_nbr,membership_create_date);

scoreOut = FOREACH scoreJoin GENERATE
    score::membership_nbr AS membership_nbr
    , score::cardholder_nbr AS cardholder_nbr
    , score::membership_create_date AS membership_create_date
    , score::cat_subcat_nbr AS cat_subcat_nbr
    , score::system_item_nbr AS system_item_nbr
    , score::score AS score
;


STORE scoreOut INTO '$scoreOut' USING PigStorage('\t');




