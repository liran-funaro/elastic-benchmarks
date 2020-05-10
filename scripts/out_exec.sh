#!/bin/sh
DATE_FORMAT="%Y-%m-%dT%H%M%S"
LOG_DIR="$HOME/output_logs"
#LOG_DIR="/tmp"
REMOTE_USER="nirla"
REMOTE_SERVER="t2"
REMOTE_DIR="~/experiments"
MAIL_TO="dl8.nir@gmail.com"
COMPRESS=0

EXP_NAME=`echo "$1"|sed 's/.*\///g'`

START=`date +$DATE_FORMAT`
LOG_FILE="${START}_${EXP_NAME}.log"

LOG_PATH="$LOG_DIR/$LOG_FILE"

echo "experiment output file: $LOG_PATH"
echo "start time: $START"
echo "$1" > "$LOG_PATH"
echo "start time: $START" >> "$LOG_PATH"
python $1 >> "$LOG_PATH" 2>&1
echo "error code: $?" >> "$LOG_PATH"
END=`date +$DATE_FORMAT`
echo "ended on $END" >> "$LOG_PATH"
echo "ended on $END"

if [ "$COMPRESS" -eq 1 ]; then
	bzip2 "$LOG_PATH"
	if [ "$?" -eq 0 ]; then
		LOG_FILE="$LOG_FILE.bz2"
		LOG_PATH="$LOG_PATH.bz2"
	fi
fi

#scp "$LOG_PATH" "$REMOTE_USER@$REMOTE_SERVER:$REMOTE_DIR"
tail -n 30 "$LOG_PATH"|ssh "$REMOTE_USER@$REMOTE_SERVER" mail -s "$1" dl8.nir@gmail.com
