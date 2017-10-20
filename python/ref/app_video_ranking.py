#!/usr/bin/python
# -*- coding: utf-8 -*-
 
import json
import sys
import csv
import argparse
import codecs
import subprocess
import datetime
import os
import commands
from os import path
from dateutil.parser import parse

DATABASE = 'ref_datastudio'
DATANAME = path.splitext(__file__)[0]

def checkDate(data):
  try:
    parse(data)
  except ValueError:
    exit("Data Error")
  except:
    exit("Unexpected error:", sys.exc_info()[0])

def checkNum(data):
  if str(data).isdigit() == False:
    exit("Data Error")
  if str(data) == "0":
    exit("Data Error")

def getArgs():
  parser = argparse.ArgumentParser(description='Get App Search Event')
  parser.add_argument('--date')
  parser.add_argument('--days')
  args = parser.parse_args()

  date = args.date
  days = args.days
  if (date == None): date = datetime.datetime.today().strftime("%Y%m%d")
  if (days == None): days = 1

  checkDate(date)
  checkNum(days)

  return date, days

def processdate(exec_date, exec_days):
  dt = datetime.datetime.strptime(exec_date, '%Y-%m-%d')
  date = dt - datetime.timedelta(days=int(exec_days)-1)
  dt = dt.strftime('%Y%m%d')
  date = date.strftime('%Y%m%d')
  return dt, date

def processfile(exec_date, start_date):
  f = open('./sql/' + DATANAME + '.sql')
  data = f.read()
  data = data.replace('@replace001@', start_date)
  data = data.replace('@replace002@', exec_date)
  f.close()
  return data

def main():

  print("--- Process Start ---")

  res_date = getArgs()
  exec_date = res_date[0]
  exec_days = res_date[1]
  print("--- Args(exec_date) " + str(exec_date) + " ---")
  print("--- Args(exec_days) " + str(exec_days) + " ---")
  
  btweendate = processdate(exec_date, exec_days)

  check = "bq query --replace --dry_run --use_legacy_sql=false --destination_table=" + DATABASE + "." + DATANAME + " '" + processfile(btweendate[0], btweendate[1]) + "'"
  query = "bq query --replace --use_legacy_sql=false --destination_table=" + DATABASE + "." + DATANAME + " '" + processfile(btweendate[0], btweendate[1]) + "'"
  print(query)
  cap = commands.getstatusoutput(check)
  print(cap[1])
  commands.getstatusoutput(query)
  print("--- Process End ---")
  
if __name__ == '__main__':
  main()
