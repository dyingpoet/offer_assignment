-----------------------------------------------------------------------------------------
--  Process Name : OA Automation - Summary Statistics Process
--  Script Name  : summary_statistics_metrics_n_coreflow.hql
--  Description  : This hql will derives number of member qualifying for n coupons .
--  Modification Log:
-----------------------------------------------------------------------------------------
--  Author           Version              Date                     Comments
-----------------------------------------------------------------------------------------
--  vraja2            1.0              12/12/2014          Centralization of SPE Changes 
--                                                                                     
-----------------------------------------------------------------------------------------


USE ${database};

INSERT INTO TABLE JOB_SUMMARY_RESULT
SELECT s.ccnt as summary_chunk_nbr,
${job_id} AS  job_request_id,
 concat(ccnt ,'='
,CASE WHEN stats IS NULL 
THEN 0 
ELSE cast(b.stats AS INT) 
END) summary_chunk_txt FROM
(SELECT cnt AS msg_seq_no,count(cnt) stats FROM (
SELECT triple_key,count(distinct coupon_no) cnt
FROM ${offer_assignment_table} WHERE job_id= '${job_id}'
GROUP BY triple_key) a
GROUP BY cnt) b
FULL OUTER JOIN coupon_cnt_desc s
ON b.msg_seq_no=s.ccnt
WHERE s.ccnt <= ${coupon_cnt_upper_lmt} 