CREATE EXTERNAL TABLE IF NOT EXISTS pythia.snapshot_item_info_flat (
        cat_subcat_nbr STRING
        , system_item_nbr INT
        , customer_item_nbr INT
        , item1_desc STRING
        , item2_desc STRING
        , base_unit_retail_amt FLOAT
)
PARTITIONED BY (ds STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE
LOCATION '/user/pythia/Workspaces/SamsMEP/snapshot_item_info_flat'
;


