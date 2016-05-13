
source combined_raw_offer_assignment_param.cfg

scoreFile=/home/jli21/sams/working_session/Apr2015-477/Apr2015-477/scoreFile
CKP_INPUT_FILE=${WORK_DIR}/offerAssignment_CKP_Apr2015-477_new
CKP_OUTPUT_FILE=${WORK_DIR}/offerAssignment_CKP_Apr2015-477_new_scoreTypeAdj

python ${SRC_POSTPROCESS_PY_DIR}/adjustScoreType.py ${CKP_INPUT_FILE} ${scoreFile}  rew > ${CKP_OUTPUT_FILE}

