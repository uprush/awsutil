#!/bin/bash

DIST_DIR=`dirname $0`
DIST_DIR=`cd $DIST_DIR; pwd`

if [ "$#" -ne 2 ]; then
    echo "Usage: purge-bucket.sh <region> <bucket>"
    exit
fi

REGION=$1
BUCKET=$2

for JAR in $DIST_DIR/lib/*.jar
do
    CP=$CP:$JAR
done

java -cp $CP com.purestorage.reddot.awsutil.PurgeBucket $REGION $BUCKET
