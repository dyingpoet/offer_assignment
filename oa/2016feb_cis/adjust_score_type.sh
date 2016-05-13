
source combined_offer_assignment_param.cfg
#source combined_offer_assignment_param_run2.cfg


scoreFile=/home/jli21/sams/working_session/${CAMPAIGN_MONTH}/${CAMPAIGN_MONTH}-rew/scoreFile
CKP_INPUT_FILE=${WORK_DIR}/offerAssignment_CKP_${CAMPAIGN_MONTH}_new
#CKP_INPUT_FILE=${WORK_DIR}/offerAssignment_CKP_Apr2015-477-run2_new
#CKP_OUTPUT_FILE=${WORK_DIR}/offerAssignment_CKP_${CAMPAIGN_MONTH}_new_non_static_scoreTypeAdj
CKP_OUTPUT_FILE=${WORK_DIR}/offerAssignment_CKP_${CAMPAIGN_MONTH}_new_scoreTypeAdj

python ${SRC_POSTPROCESS_PY_DIR}/adjustScoreType.py ${CKP_INPUT_FILE} ${scoreFile}  rew > ${CKP_OUTPUT_FILE}

