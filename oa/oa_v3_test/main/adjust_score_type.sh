
source combined_offer_assignment_param.cfg

scoreFile=/home/jli21/sams/working_session/Mar2015-79/Mar2015-79-rew/scoreFile
CKP_INPUT_FILE=${WORK_DIR}/offerAssignment_CKP_Mar2015-79_new
CKP_OUTPUT_FILE=${WORK_DIR}/offerAssignment_CKP_Mar2015-79_new_scoreTypeAdj

python ${SRC_POSTPROCESS_PY_DIR}/adjustScoreType.py ${CKP_INPUT_FILE} ${scoreFile}  rew > ${CKP_OUTPUT_FILE}

