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
  secret = json.load(open(os.path.join(os.path.expanduser('.'), '.google_analytics', 'secret.json')))
  return secret

def get_view_id(kbn):
  secret = get_secret()
  target_view_id = ""
  if kbn == "Android": target_view_id = secret["Android"]["app_Android"]
  if kbn == "iOS": target_view_id = secret["iOS"]["app_iOS"]
  if len(target_view_id) == 0: exit("args Error")
  return target_view_id

def get_unique_users(service, view_id, exec_date, start_index, target_metrics):
  results = service.data().ga().get(
    ids='ga:' + view_id,
    start_date=exec_date,
    end_date=exec_date,
    metrics=target_metrics,
    dimensions='ga:date',
    sort='ga:date',
    start_index=start_index,
    max_results=1,
    samplingLevel='HIGHER_PRECISION'
  ).execute()
  rows = results.get('rows')
  if rows and len(rows) > 0:
    return rows
  else:
    return []

def main():

  device = sys.argv[1]

  # information for call API.
  scope = ['https://www.googleapis.com/auth/analytics.readonly']
  service_account_email = GoogleAnalyticsBase.get_service_account_email()
  key_file_location = GoogleAnalyticsBase.get_key_file_location()
  view_id = get_view_id(device)
  # Authenticate and construct service.
  service = GoogleAnalyticsBase.get_service('analytics', 'v3', scope, key_file_location, service_account_email)
   
  args = sys.argv
  arglen = len(args)
  # コマンドライン引数の内容を確認
  if (arglen < 3):
    # 引数[1]が無い場合はシステム日付を設定
    exec_date = datetime.datetime.today().strftime("%Y-%m-%d")
  else:
    # 引数[1]がある場合はその引数を実行日付に設定
    exec_date = args[2]
     
  print("")
  print("exec_date:" + exec_date)

  target_metrics = ['ga:1dayUsers', 'ga:7dayUsers', 'ga:14dayUsers', 'ga:30dayUsers']

  #------------------------------------------------------------
  # データ収集
  # 前日のスマートフォンデバイスのアクティブユーザーを日単位で取得
  # ※各カラムで1日、7日間、14日間、30日間の集計結果
  #------------------------------------------------------------
  rowsArr = []
  start_index = 1
  for i in xrange(len(target_metrics)):
    rows = get_unique_users(service, view_id, exec_date, start_index, target_metrics[i])
    rowsArr.append(rows)

  # Write Output File(CSV)
  csv_filename = "./csv/ga_active_users_app_" + device + ".csv"
  fout = codecs.open(csv_filename, 'w', 'utf-8')
   
  app_user = [rowsArr[0][0][0]];
  for newArr in rowsArr:
    if len(newArr) > 0:
      app_user.append(newArr[0][1]);

  filestr = device \
    + "," + exec_date \
    + "," + app_user[1] \
    + "," + app_user[2] \
    + "," + app_user[3] \
    + "," + app_user[4] + "\n"
   
  fout.write(filestr)

  # import data to Bigquery
  schema_filename = './schema/active_users_app.txt'
  tablename = 'active_users_app'
  print("import data to Bigquery")
  command_str = 'bq load event.' + tablename + ' ' + csv_filename + ' ' + schema_filename
  print(command_str)
  os.system(command_str)
  print("end")

if __name__ == '__main__':
  main()