#!/bin/sh

#
# YYYY-MM-DD
#
STARTDATE=2017-09-01
ENDDATE=2017-09-07

CURRENTDATE=$STARTDATE
while :
do
  # 処理
  echo $CURRENTDATE
  if [ $CURRENTDATE = $ENDDATE ] ; then
          break
  fi

  # GNU
#  CURRENTDATE=`date -d "$CURRENTDATE 1day" "+%Y-%m-%d"`
  # BSD(mac)
  CURRENTDATE=`date -v+1d -j -f %Y-%m-%d $CURRENTDATE +%Y-%m-%d`
done
