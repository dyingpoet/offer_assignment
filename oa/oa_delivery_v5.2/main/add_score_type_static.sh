
source combined_offer_assignment_param.cfg

recoScoreFile=./memberSplit/reco.sample.input
rewardScoreFile=./memberSplit/reward.sample.input
CKP_INPUT_FILE=${WORK_DIR}/ckp_static
CKP_OUTPUT_FILE_1=${WORK_DIR}/ckp_static_1_reco
CKP_OUTPUT_FILE_2=${WORK_DIR}/ckp_static_2_reward

python ${SRC_POSTPROCESS_PY_DIR}/add_score_type_static.py ${CKP_INPUT_FILE} ${recoScoreFile}  rec > ${CKP_OUTPUT_FILE_1}
python ${SRC_POSTPROCESS_PY_DIR}/add_score_type_static.py ${CKP_OUTPUT_FILE_1} ${rewardScoreFile}  rew > ${CKP_OUTPUT_FILE_2}

