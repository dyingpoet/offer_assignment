SET hive.exec.compress.output=false;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec;

CREATE EXTERNAL TABLE IF NOT EXISTS jli21.${hiveconf:OATable} (
    membership_nbr INT
    , card_holder_nbr INT
    , member_create_date STRING
    , catsub STRING
    , value_coupon_nbr STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' STORED AS TEXTFILE
LOCATION '${hiveconf:OfferData}'
;

