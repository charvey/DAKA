#!/usr/bin/python
import csv
import json
import argparse
from pymongo import MongoClient

parser = argparse.ArgumentParser(prog="csv2json.py", usage="%(prog)s -d database -c collection -f csv")
parser.add_argument('-d', help = 'Database name', required = True)
parser.add_argument('-c', help = 'Colelction name',required = True)
parser.add_argument('-f', help = 'CSV file path', required = True)
args = parser.parse_args()
dbName = args.d
colName = args.c
csvFile = args.f

client = MongoClient()
db = client[dbName]
col = db[colName]
f = open(csvFile, 'r' )
header = f.readline()
header = header.replace('"','').replace('\n','')
reader = csv.DictReader( f, fieldnames = header.split(',') )
out = json.dumps( [ row for row in reader ] )
col.insert(json.loads(out))
f.close()
