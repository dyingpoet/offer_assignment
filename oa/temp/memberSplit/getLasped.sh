task_dir=/user/jli21/sams/lapsed_member_subcat/
hive -e "set hive.exec.compress.output=false; INSERT OVERWRITE DIRECTORY '${task_dir}' SELECT DISTINCT membership_nbr, CONCAT(IF(cat < 10, CONCAT('0', cat), CAST(cat AS STRING)), IF(subcat < 10, CONCAT('0', subcat), CAST(subcat AS STRING))) AS cat_subcat_nbr FROM myuan1.ipi_member_subcat WHERE ifregular = 1 AND iflapsed = 1;"


