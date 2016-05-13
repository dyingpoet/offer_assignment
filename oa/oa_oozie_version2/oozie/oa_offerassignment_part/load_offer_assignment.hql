-----------------------------------------------------------------------------------------
--  Process Name : OA Automation - Summary Statistics Process
--  Script Name  : offer_assignment_intermediate.hql
--  Description  : This hql will load offer_assignment table using the data at ${oa_hdfs_path}.
--  Modification Log:
-----------------------------------------------------------------------------------------
--  Author           Version              Date                     Comments
-----------------------------------------------------------------------------------------
--  vraja2            1.0              12/12/2014          Centralization of SPE Changes 
--                                                                                     
-----------------------------------------------------------------------------------------
SET hive.exec.compress.output=false;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec;
SET hive.exec.dynamic.partition.mode=nonstrict;
USE ${database};

!hadoop fs -cp ${oa_hdfs_path} ${oa_hdfs_path_copy};

ALTER TABLE ${offer_assignment_table} ADD IF NOT EXISTS PARTITION(job_id='${job_id}') location '${oa_hdfs_path_copy}';
