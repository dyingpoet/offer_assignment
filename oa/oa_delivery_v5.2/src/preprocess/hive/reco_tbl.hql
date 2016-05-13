CREATE EXTERNAL TABLE IF NOT EXISTS pythia.pis_member_reco_score_anchor (
                membership_nbr INT
                , cardholder_nbr SMALLINT
                , membership_create_date STRING
                , cat_subcat_nbr STRING
                , score FLOAT
                , anchor_cat_subcat_nbr STRING
)
PARTITIONED BY (campaign_month STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS TEXTFILE
LOCATION '/user/pythia/Workspaces/SamsMEP/pis_member_reco_score_anchor'
;


