#!/bin/bash

email_id=$1
raw_keyword=$2
hdfspath=$3
load=$4
email_sub_error="Error generating .info file for data delivery handshake in $load load"

encoded_keyword=`echo $raw_keyword | openssl base64 -e`

fileName="$load".info;

fileContent="$encoded_keyword
datasetname=$load"

#notify if hdfs path does not exists
if ! hadoop fs -test -e $hdfspath; then
        echo "hdfs path '$hdfspath' does not exists" | mail -s "$email_sub_error" $email_id
fi

#replace if file exists
if hadoop fs -test -e $hdfspath/$fileName; then
        hadoop fs -rm -r $hdfspath/$fileName;
        $(echo "$fileContent" | hadoop fs -put - $hdfspath/$fileName);
else
        $(echo "$fileContent" | hadoop fs -put - $hdfspath/$fileName);
fi

fileName="$load".trigger;

#replace if trigger file exists
if hadoop fs -test -e $hdfspath/$fileName; then
        hadoop fs -rm -r $hdfspath/$fileName;
        $(echo "$fileContent" | hadoop fs -put - $hdfspath/$fileName);
else
        $(echo "$fileContent" | hadoop fs -put - $hdfspath/$fileName);
fi

