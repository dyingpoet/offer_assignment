SET hive.exec.compress.output=false;

INSERT OVERWRITE TABLE ${hiveconf:itemInfoTbl} PARTITION(ds='${hiveconf:ds}')
select distinct
        --CONCAT(IF(category_nbr > 9, CAST(category_nbr AS string), CONCAT('0', CAST(category_nbr AS string))), IF(sub_category_nbr > 9, CAST(sub_category_nbr AS string), CONCAT('0', CAST(sub_category_nbr AS string)))) as cat_subcat_nbr
        system_item_nbr
        , category_nbr
        , sub_category_nbr
from ${hiveconf:itemInfoSourceTbl}
where ds='${hiveconf:ds}'
;

