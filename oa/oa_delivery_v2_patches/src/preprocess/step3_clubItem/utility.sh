checkfile() {
    FILE=${1}

    echo checking ${FILE}
    `${HDFS} -test -e "${FILE}"`
    return $?
}

checkfilewithalert() {
    FILE=${1}
    HOURTHRESHOLD=${2}

    echo checking ${FILE}
    `${HDFS} -test -e "${FILE}"`
    status=$?

    if [ ${status} -ne 0 ]; then
current_hour=`date +"%H"`
        if [ ${current_hour} -gt ${HOURTHRESHOLD} ]; then
echo "ERROR: HDFS ${FILE} is not available. Please open BFD support ticket." | mail -s "ERROR: HDFS ${FILE} is not available on ${ADSAVER_SERVER})" ${ADSAVER_EMAIL}
        fi;
    fi;

    return ${status};
}

# 0 if success
# 1 if directory does not exist
# 2 if directory size is smaller than expected
checkdirectory() {
    DIRECTORY=${1}
    MINSIZE=${2}

    echo checking ${DIRECTORY} for minimum size ${MINSIZE}
    `${HDFS} -test -d "${DIRECTORY}"`
    if [ $? -ne 0 ]; then
return 1;
    fi;

    DSU=`${HDFS} -dus "${DIRECTORY}" | cut -f2`
    if [ ${DSU} -lt ${MINSIZE} ]; then
return 2;
    fi;

    return 0;
}

checkdirectorywithalert() {
    DIRECTORY=${1}
    MINSIZE=${2}
    HOURTHRESHOLD=${3}

    echo checking ${DIRECTORY} for minimum size ${MINSIZE} with alert for ${HOURTHRESHOLD}
    checkdirectory ${DIRECTORY} ${MINSIZE}

    status=$?

    if [ ${status} -ne 0 ]; then
current_hour=`date +"%H"`
        if [ ${current_hour} -gt ${HOURTHRESHOLD} ]; then
echo "ERROR: HDFS ${DIRECTORY} is not available or has than ${MINSIZE} size" | mail -s "ERROR: HDFS ${DIRECTORY} is not available or has than ${MINSIZE} size on ${ADSAVER_SERVER})" ${ADSAVER_EMAIL}
        fi;
    fi;

    return ${status};
}

checkdirectorieswithalert() {
    STARTPOINT=${1}
    ENDPOINT=${2}
    ROOTDIRECTORY=${3}
    MINSIZE=${4}
    HOURTHRESHOLD=${5}

    for day in $(seq ${STARTPOINT} ${ENDPOINT})
    do
hive_date_string=`date -d "$date + -${day} days" +%Y-%m-%d`;
        current_month=`date -d "$date + -${day} days" +%m`;
        current_day=`date -d "$date + -${day} days" +%d`;

        if [ ${current_month} -ne "12" -o ${current_day} -ne "25" ]; then
current_directory="${ROOTDIRECTORY}/visit_date=${hive_date_string}"
            checkdirectorywithalert ${current_directory} ${MINSIZE} ${HOURTHRESHOLD}
        
            status=$?

            if [ ${status} -ne 0 ]; then
break;
            fi;
        fi;
    done

return ${status}
}

run_job() {
    COMMAND=${1}
    RETRY=${2}
    WAITTIME=${3}
    LOG=${4}

    echo "INFO: Running ${COMMAND}"
    NEXT_WAIT_TIME=0
    COMMAND_STATUS=1
    EXTRAPROPERTY=""

    if [ ${ADSAVER_SERVER} == "adsaver-engine01" ] || [ ${ADSAVER_SERVER} == "adsaver-engine02" ]; then
if [[ "$COMMAND" =~ ^hive ]]; then
export HADOOP_CLASSPATH=/opt/mapr/hbase/hbase-0.94.5/conf/:/opt/mapr/hadoop/hadoop-0.20.2/:/opt/mapr/hive-hbase-integration/zookeeper-3.3.6.jar
            export HIVE_AUX_JARS_PATH=/opt/mapr/hive-hbase-integration/zookeeper-3.3.6.jar
            EXTRAPROPERTY=" -hiveconf hbase.zookeeper.quorum=10.93.128.213,10.93.128.214,10.93.128.215 -hiveconf hbase.zookeeper.property.clientPort=5181"
        else
export HADOOP_CLASSPATH=`hbase classpath`
        fi;
    elif [ ${ADSAVER_SERVER} == "cdc-adsav-enginedev00" ]; then
if [[ "$COMMAND" =~ ^hive ]]; then
export HADOOP_CLASSPATH=/opt/mapr/hbase/hbase-0.94.9/conf/:/opt/mapr/hadoop/hadoop-0.20.2/:/opt/mapr/hive-hbase-integration/zookeeper-3.3.6.jar
            export HIVE_AUX_JARS_PATH=/opt/mapr/hive-hbase-integration/zookeeper-3.3.6.jar
            EXTRAPROPERTY=" -hiveconf hbase.zookeeper.quorum=10.224.160.85,10.224.160.149,10.224.160.213,10.224.161.21,10.224.161.85 -hiveconf hbase.zookeeper.property.clientPort=5181"
        else
export HADOOP_CLASSPATH=`hbase classpath`
        fi;
    fi;

    until [ $COMMAND_STATUS -eq 0 -o $NEXT_WAIT_TIME -eq ${RETRY} ]; do
        ${COMMAND} ${EXTRAPROPERTY} >> ${LOG} 2>&1
        COMMAND_STATUS=$?
        let NEXT_WAIT_TIME=NEXT_WAIT_TIME+1
        if [ ${WAITTIME} -gt 0 ]; then
sleep ${WAITTIME}
        fi;
    done

return ${COMMAND_STATUS}
}

run_hive_job() {
    COMMAND=${1}
    RETRY=${2}
    WAITTIME=${3}
    LOG=${4}

    NEXT_WAIT_TIME=0
    COMMAND_STATUS=1
    EXTRAPROPERTY=""

    EXIST_HADOOP_CLASSPATH=${HADOOP_CLASSPATH}
    if [ ${ADSAVER_SERVER} == "adsaver-engine01" ] || [ ${ADSAVER_SERVER} == "adsaver-engine02" ]; then
HADOOP_CLASSPATH=/opt/mapr/hbase/hbase-0.94.5/conf/:/opt/mapr/hadoop/hadoop-0.20.2/:/opt/mapr/hive-hbase-integration/zookeeper-3.3.6.jar
        HIVE_AUX_JARS_PATH=/opt/mapr/hive-hbase-integration/zookeeper-3.3.6.jar
        EXTRAPROPERTY=" -hiveconf hbase.zookeeper.quorum=10.93.128.213,10.93.128.214,10.93.128.215 -hiveconf hbase.zookeeper.property.clientPort=5181"
    elif [ ${ADSAVER_SERVER} == "cdc-adsav-enginedev00" ]; then
HADOOP_CLASSPATH=/opt/mapr/hbase/hbase-0.94.9/conf/:/opt/mapr/hadoop/hadoop-0.20.2/:/opt/mapr/hive-hbase-integration/zookeeper-3.3.6.jar
        HIVE_AUX_JARS_PATH=/opt/mapr/hive-hbase-integration/zookeeper-3.3.6.jar
        EXTRAPROPERTY=" -hiveconf hbase.zookeeper.quorum=10.224.160.85,10.224.160.149,10.224.160.213,10.224.161.21,10.224.161.85 -hiveconf hbase.zookeeper.property.clientPort=5181"
    else
HADOOP_CLASSPATH=`hbase classpath`
    fi;
    echo "INFO: Running hive ${EXTRAPROPERTY} ${COMMAND}"

    until [ $COMMAND_STATUS -eq 0 -o $NEXT_WAIT_TIME -eq ${RETRY} ]; do
hive ${EXTRAPROPERTY} ${COMMAND} 2>${LOG}
        COMMAND_STATUS=$?
        let NEXT_WAIT_TIME=NEXT_WAIT_TIME+1
        if [ ${WAITTIME} -gt 0 ]; then
sleep ${WAITTIME}
        fi;
    done

HADOOP_CLASSPATH=${EXIST_HADOOP_CLASSPATH}

    return ${COMMAND_STATUS}
}

run_hive_redirect_job() {
    SCRIPT=${1}
    FILE=${2}

    EXIST_HADOOP_CLASSPATH=${HADOOP_CLASSPATH}
    if [ ${ADSAVER_SERVER} == "adsaver-engine01" ] || [ ${ADSAVER_SERVER} == "adsaver-engine02" ]; then
unset HADOOP_CLASSPATH
    else
HADOOP_CLASSPATH=`hbase classpath`
    fi;
        
    echo "INFO: Running hive -f ${SCRIPT} > ${FILE}"
    hive -f ${SCRIPT} > ${FILE}

    export HADOOP_CLASSPATH=`hbase classpath`:${ADSAVER_ROOT_DIRECTORY}/lib/adsaver-config.jar:${ADSAVER_ROOT_DIRECTORY}/lib/adsaver-data.jar:${ADSAVER_ROOT_DIRECTORY}/lib/adsaver-util.jar:${ADSAVER_ROOT_DIRECTORY}/lib/adsaver-global.jar
    export CLASSPATH=`hbase classpath`:${ADSAVER_ROOT_DIRECTORY}/lib/adsaver-config.jar:${ADSAVER_ROOT_DIRECTORY}/lib/adsaver-data.jar:${ADSAVER_ROOT_DIRECTORY}/lib/adsaver-util.jar:${ADSAVER_ROOT_DIRECTORY}/lib/adsaver-global.jar

    return ${COMMAND_STATUS}
}

`
