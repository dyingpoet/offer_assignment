
CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:transTbl} (
        visit_nbr INT
        store_nbr INT
        unit_qty FLOAT
        retail_price FLOAT
        card_holder_nbr INT
        membership_nbr INT
        scan_id AS system_item_nbr INT
        category_nbr INT
        sub_category_nbr INT
        visit_date STRING
)
PARTITIONED BY (ds STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS TEXTFILE
LOCATION '${hiveconf:transLoc}
;


