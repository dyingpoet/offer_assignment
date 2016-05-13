CREATE EXTERNAL TABLE IF NOT EXISTS pythia.pis_member_subcat_item_coupon_score (
                membership_nbr INT
                , cardholder_nbr SMALLINT
                , membership_create_date STRING
                , cat_subcat_nbr STRING
                , system_item_nbr INT
                , value_coupon_nbr INT
                , score FLOAT
                , anchor_cat_subcat_nbr STRING
)
PARTITIONED BY (campaign_month STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE
LOCATION '/user/pythia/Workspaces/SamsMEP/pis_member_subcat_item_coupon_score'
;


