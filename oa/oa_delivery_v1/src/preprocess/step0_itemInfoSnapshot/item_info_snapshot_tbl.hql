CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:itemInfoTbl} (
        system_item_nbr INT
        , category_nbr INT
        , sub_category_nbr INT
)
PARTITIONED BY (ds STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS TEXTFILE
LOCATION '${hiveconf:itemInfoLoc}'
;


