#!/usr/bin/python
# -*- coding: utf-8 -*-
 
import json
import sys
import GoogleAnalyticsBase
from apiclient.errors import HttpError
from oauth2client.client import AccessTokenRefreshError
import csv
import codecs
import subprocess
import datetime
import os
 
def get_secret():
  secret = json.load(open(os.path.join(os.path.expanduser('~'), '.google_analytics', 'secret.json')))
  return secret

def get_view_id():
  secret = get_secret()
  return secret["view_id"]

def getUniqueUsers(service, view_id, exec_date, start_index):
  results = service.data().ga().get(
    ids='ga:' + view_id,
    start_date=exec_date,
    end_date=exec_date,
    metrics='ga:users,ga:pageviews',
    dimensions='ga:landingPagePath',
    filters='ga:landingPagePath=~^\/[0-9]+$;ga:sourceMedium=@organic;ga:landingPagePath!@topics;ga:landingPagePath!@page;ga:landingPagePath!@?',
    start_index=start_index,
    max_results=10000,
    samplingLevel='HIGHER_PRECISION'
  ).execute()
  rows = results.get('rows')
  if rows and len(rows) > 0:
    return rows
  else:
    return []

def main():

  # information for call API.
  scope = ['https://www.googleapis.com/auth/analytics.readonly']
  service_account_email = GoogleAnalyticsBase.get_service_account_email()
  key_file_location = GoogleAnalyticsBase.get_key_file_location()
  view_id = get_view_id()
   
  # Authenticate and construct service.
  service = GoogleAnalyticsBase.get_service('analytics', 'v3', scope, key_file_location, service_account_email)
   
  args = sys.argv
  arglen = len(args)
  # コマンドライン引数の内容を確認
  if (arglen < 2):
    # 引数[1]が無い場合はシステム日付を設定
    exec_date = datetime.datetime.today().strftime("%Y-%m-%d")
  else:
    # 引数[1]がある場合はその引数を実行日付に設定
    exec_date = args[1]
     
  print("")
  print("exec_date:" + exec_date)
   
  #--------------------------------------------
  # データ収集
  # UUを日単位で取得
  #--------------------------------------------
  rows1 = getUniqueUsers(service, view_id, exec_date, 1)
  rows2 = getUniqueUsers(service, view_id, exec_date, 10000)
  rows = rows1 + rows2
  
  # Write Output File(CSV)
  exec_date_str = exec_date.replace('-','')
  csvFileName = "ga_dau_per_page_" + exec_date_str + ".csv"
  fout = codecs.open(csvFileName, 'w', 'utf-8')
   
  count = 0;
  for row in rows:
    pagepath = row[0];
    users = row[1];
    pageviews = row[2];
    filestr = pagepath.replace('/','') \
      + "," + users \
      + "," + pageviews \
      + "," + exec_date + "\n"
     
    fout.write(filestr)
    count = count+1;
   
  print(count);
  print("command start")
  command_str = 'bq load test.dau_' + exec_date_str + ' ga_dau_per_page_' + exec_date_str + '.csv schema_dau.txt'
  os.system(command_str)
  print("command end")

if __name__ == '__main__':
  main()
