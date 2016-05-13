
source combined_raw_offer_assignment_param.cfg

scoreFile=/home/jli21/sams/working_session/Jan2015-78/Jan2015-78-rew/scoreFile
CKP_INPUT_FILE=${WORK_DIR}/offerAssignment_CKP_Jan2015-78-raw_new
CKP_OUTPUT_FILE=${WORK_DIR}/offerAssignment_CKP_Jan2015-78-raw_new_scoreTypeAdj

python ${SRC_POSTPROCESS_PY_DIR}/adjustScoreType.py ${CKP_INPUT_FILE} ${scoreFile}  rew > ${CKP_OUTPUT_FILE}

