#!/usr/bin/python
import csv
import json
import sys

if len(sys.argv) != 3:
        print("usage: csv2json.py input output")
        sys.exit(0)

f = open( sys.argv[1], 'r' )
header = f.readline()
header = header.replace('"','').replace('\n','')
reader = csv.DictReader( f, fieldnames = header.split(',') )
out = json.dumps( [ row for row in reader ] )
f1 = open(sys.argv[2], 'w')
f1.write(out)
f1.close()
