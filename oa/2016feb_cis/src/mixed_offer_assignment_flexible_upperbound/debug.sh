##cut -d ' ' -f2 $WORK_DIR/scoreFile | sort | uniq | python ${SRC_PREPROCESS_PY_DIR}/adjMemberFile.py $FINAL_DIR/offerAssignment $MEMBER_UB $MEMBER_UB_ALL > $WORK_DIR/memberFile

FINAL_DIR=/home/jli21/sams/working_session/Oct2015-601-groupB/Oct2015-601-groupB
#cut -d ' ' -f2 scoreFile | sort | uniq | python /home/jli21/sams/oa_module/oa_v4_test/src/preprocess/py/adjMemberFile.py $FINAL_DIR/offerAssignment 6 6
head scoreFile | python /home/jli21/sams/oa_module/oa_v4_test/src/preprocess/py/adjMemberFile.py $FINAL_DIR/offerAssignment 6 6

