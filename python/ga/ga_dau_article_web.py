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

def get_view_id():
  secret = get_secret()
  return secret["view_id_web"]

def get_unique_users(service, view_id, exec_date, start_index, max_results):
  results = service.data().ga().get(
    ids='ga:' + view_id,
    start_date=exec_date,
    end_date=exec_date,
    metrics='ga:users,ga:pageviews',
    dimensions='ga:landingPagePath,ga:deviceCategory',
    filters='ga:landingPagePath=~^\/[0-9]+$',
    sort='-ga:users',
    start_index=start_index,
    max_results=max_results,
    samplingLevel='HIGHER_PRECISION'
  ).execute()
  rows = results.get('rows')
  if rows and len(rows) > 0:
    return rows
  else:
    return []

def writeCsv(csv_filename, exec_date, rows):
  write_count = 0;
  fout = codecs.open(csv_filename, 'a', 'utf-8')
  for row in rows:
    pagepath = row[0];
    device = row[1];
    users = row[2];
    pageviews = row[3];
    filestr = pagepath.replace('/','') \
      + "," + device \
      + "," + users \
      + "," + pageviews \
      + "," + exec_date + "\n"
     
    fout.write(filestr)
    write_count = write_count+1;
  return write_count

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
   
  # Write Output File(CSV)
  exec_date_str = exec_date.replace('-','')
  csv_filename = "./csv/ga_dau_article_web_" + exec_date_str + ".csv"
  fout = codecs.open(csv_filename, 'w', 'utf-8')
   
  #--------------------------------------------
  # データ収集
  # UUを日単位で取得
  #--------------------------------------------
  flg = 1
  count = 0;
  start_index = 1
  max_results = 10000
  while flg > 0:
    rows = get_unique_users(service, view_id, exec_date, start_index, max_results)
    start_index = start_index + max_results
    flg = len(rows)
    if flg > 0: count = count + writeCsv(csv_filename, exec_date, rows);
    # 結果数がmax_results以下であれば次APIを回す必要がない為、処理終了
    if flg < max_results: flg = 0;

  print(count);

  # import data to Bigquery
  schema_filename = './schema/event_dau_article_web.txt'
  tablename_prefix = 'dau_article_web_'
  print("import data to Bigquery")
  command_str = 'bq load event.' + tablename_prefix + exec_date_str + ' ' + csv_filename + ' ' + schema_filename
  os.system(command_str)
  print("end")

if __name__ == '__main__':
  main()
