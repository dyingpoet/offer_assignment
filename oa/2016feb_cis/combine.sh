
CONFIG_FILE_PATH=./config
CAMPAIGN_MONTH=Aug2015-538
WORK_DIR=~/sams/working_session/${CAMPAIGN_MONTH}
REWARD_DIR=$WORK_DIR/${CAMPAIGN_MONTH}-rew
RECO_DIR=$WORK_DIR/${CAMPAIGN_MONTH}-rec
TARGET_DIR=$WORK_DIR/${CAMPAIGN_MONTH}
MEMBER_LB=6
MEMBER_UB=6

[ -d $TARGET_DIR ] || mkdir -p $TARGET_DIR

cat ${REWARD_DIR}/offerFile | python ~/flt.py ${CONFIG_FILE_PATH}/vc_to_remove 0 ' ' 1>${TARGET_DIR}/offerFile
cat ${REWARD_DIR}/offer2subcatFile | python ~/flt.py ${CONFIG_FILE_PATH}/vc_to_remove 0 ' ' 1>${TARGET_DIR}/offer2subcatFile
cat ${REWARD_DIR}/sc_coupon_cnt | python ~/flt.py ${CONFIG_FILE_PATH}/vc_to_remove 1 ' ' 1>${TARGET_DIR}/sc_coupon_cnt
cat ${REWARD_DIR}/scoreFile ${RECO_DIR}/scoreFile | python ~/flt.py ${CONFIG_FILE_PATH}/vc_to_remove 0 ' ' 1> ${TARGET_DIR}/scoreFile 2> ${WORK_DIR}/scoreRemoveVC
cat ${TARGET_DIR}/scoreFile | cut -d ' ' -f2 | sort | uniq | sed "s/$/ 1 ${MEMBER_UB}/" > ${TARGET_DIR}/memberFile



