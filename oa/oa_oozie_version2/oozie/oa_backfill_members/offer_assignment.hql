-----------------------------------------------------------------------------------------
--  Process Name : OA Automation -backfill subflow
--  Script Name  : offer_assignment.hql
--  Description  : This hql will load offer_assignment table using the data at ${oa_hdfs_path_copy}.
--  Modification Log:
-----------------------------------------------------------------------------------------
--  Author           Version              Date                     Comments
-----------------------------------------------------------------------------------------
--  phala1            1.0              01/12/2015          Centralization of SPE Changes 
--                                                                                     
-----------------------------------------------------------------------------------------
SET hive.exec.compress.output=false;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec;
SET hive.exec.dynamic.partition.mode=nonstrict;

USE ${database};

!hadoop fs -cp ${offerAssignment_member_decouple_cpy} ${oa_hdfs_path_copy};

ALTER TABLE ${offer_assignment_table} ADD IF NOT EXISTS PARTITION(job_id='${job_id}');