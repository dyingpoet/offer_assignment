set default_parallel 800

/*
subcat reco score
*/

%default subcat_score_input '/user/jli21/pythia/Workspaces/SamsMEP/Recommend/Scoring/ClubInsider/2015-07-16/Decay/recommend_score_cobought_no_smoothing_max_cosine_cobought_transaction_debug_adj_study_scaled';
%default subcat_list '/user/pythia/Workspaces/SamsMEP/offerAssignment/2015aug555/subcatList';
%default subcat_score_output '/user/pythia/Workspaces/SamsMEP/offerAssignment/2015aug555/score/reco_subcat_score_grouped';



subcatScore = LOAD '$subcat_score_input' Using PigStorage('\t') AS (membership_nbr: chararray, cardholder_nbr: chararray, membership_create_date: chararray, subcat: chararray, score: double);

scList = LOAD '$subcat_list' Using PigStorage('\t') AS (subcat: chararray);



scoreFlt = FILTER subcatScore BY (score > 1e-3);

scoreJoin = JOIN scoreFlt BY subcat, scList BY subcat Using 'replicated';

scoreKeep = FOREACH scoreJoin GENERATE
    scoreFlt::membership_nbr AS membership_nbr
    , scoreFlt::cardholder_nbr AS cardholder_nbr
    , scoreFlt::membership_create_date AS membership_create_date
    , scoreFlt::subcat AS subcat
    , scoreFlt::score AS score
;

scoreOut = FOREACH (GROUP scoreKeep BY (membership_nbr, cardholder_nbr, membership_create_date)) {
    scoreOrder = ORDER scoreKeep BY score DESC;
    GENERATE FLATTEN(scoreOrder) AS (membership_nbr, cardholder_nbr, membership_create_date, subcat, score);
}

STORE scoreOut INTO '$subcat_score_output' Using PigStorage('\t');


