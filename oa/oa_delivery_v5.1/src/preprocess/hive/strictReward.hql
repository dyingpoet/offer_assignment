SET hive.exec.compress.output=false;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.parallel=true;

use pythia;


INSERT OVERWRITE TABLE ${hiveconf:task_table} 
select a.* from  a join  b
on a.membership_nbr = b.membership_nbr and a.


