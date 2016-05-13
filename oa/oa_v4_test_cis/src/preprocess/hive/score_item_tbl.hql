CREATE EXTERNAL TABLE IF NOT EXISTS pythia.pis_member_item_subcat_score (
                membership_nbr INT
                , cardholder_nbr SMALLINT
                , membership_create_date STRING
                , system_item_nbr INT
                , cat_subcat_nbr STRING
                , score FLOAT
)
PARTITIONED BY (campaign_month STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE
LOCATION '/user/pythia/Workspaces/SamsMEP/pis_member_item_subcat_score'
;


