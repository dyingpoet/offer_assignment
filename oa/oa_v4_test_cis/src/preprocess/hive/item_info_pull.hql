SET hive.exec.compress.output=false;

INSERT OVERWRITE TABLE pythia.snapshot_item_info_flat PARTITION(ds='${hiveconf:ds}')
select 
        CONCAT(IF(category_nbr > 9, CAST(category_nbr AS string), CONCAT('0', CAST(category_nbr AS string))), IF(sub_category_nbr > 9, CAST(sub_category_nbr AS string), CONCAT('0', CAST(sub_category_nbr AS string)))) as cat_subcat_nbr
        , system_item_nbr
        , customer_item_nbr
        , item1_desc
        , item2_desc
        , base_unit_retail_amt
from sams_us_clubs.item_info
;

