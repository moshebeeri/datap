from datetime import datetime
from datetime import timedelta
import os
import pymongo 
from pymongo import MongoClient

import bson
import pytz
from bson.objectid import ObjectId
from dateutil import tz
import calendar
import sys
import pprint
import logging
import json
import boto3
from botocore.exceptions import ClientError
from bson.json_util import dumps
from bson.json_util import loads
from bson import json_util

from datetime import date
from datetime import time

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import argparse
from elasticsearch import Elasticsearch

dry_run = False
collection = None
database = None
time_field = 'timestamp'
save_to_elk = False
elk_url = None
s3_bucket_name = ""
local_file_name = ""
upload_local_file_to_s3 = False
save_local_file = False
save_to_firestore = False

# Use a service account
cred = credentials.Certificate(os.path.join(os.path.dirname(__file__), 'firebase-role.json'))
firebase_admin.initialize_app(cred)
db = firestore.client()
s3 = boto3.client('s3')
# profile_name = os.environ.get('AWS_PROFILE') or 'stage'
# session = boto3.Session(profile_name)
# s3 = session.client('s3')

es = Elasticsearch()

def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

a = "mongodb://analytics:analytics@staging-analytics-shard-00-00-fhv7t.mongodb.net:27017,staging-analytics-shard-00-01-fhv7t.mongodb.net:27017,staging-analytics-shard-00-02-fhv7t.mongodb.net:27017/admin?readPreference=primary&ssl=true"
INT_IL = "mongodb://dev-rw:dev-rw@int-stg-dev-shard-00-00-fhv7t.mongodb.net:27017,int-stg-dev-shard-00-01-fhv7t.mongodb.net:27017,int-stg-dev-shard-00-02-fhv7t.mongodb.net:27017/dev-2?ssl=true&replicaSet=INT-STG-DEV-shard-0&connectTimeoutMS=10000&authSource=admin&authMechanism=SCRAM-SHA-1&3t.uriVersion=3&3t.connection.name=INT&3t.databases=dev-2"
analyticsINT_IL_URI = "mongodb://analytics:analytics@staging-analytics-shard-00-00-fhv7t.mongodb.net:27017,staging-analytics-shard-00-01-fhv7t.mongodb.net:27017,staging-analytics-shard-00-02-fhv7t.mongodb.net:27017/tabit?ssl=true&readPreference=primary&connectTimeoutMS=10000&authSource=admin&authMechanism=SCRAM-SHA-1"
client = pymongo.MongoClient(analyticsINT_IL_URI)
year = timedelta(days=365)
day = timedelta(1)

now = datetime.now(pytz.timezone('Asia/Jerusalem'))
start_today = datetime(now.year, now.month, now.day, tzinfo=tz.tzutc())

def saveDocs(docs, filename):
  if save_local_file:
    f = open(filename, "w")
  i = 0
  for doc in docs:
    try:
      if save_local_file:
        f.write(dumps(doc, sort_keys=True, default=str) + os.linesep)
        i = i + 1
        if i % 100 == 0:
          f.flush()
      
      if dry_run:
        continue
      
      if save_to_firestore:
        _id = str(doc['_id'])
        data = json.loads(json.dumps(doc, sort_keys=True, default=str))
        doc_ref = db.collection(collection).document(_id)
        doc_ref.set(data)

      if save_to_elk:
        _id = doc.pop('_id', None)
        _type = doc.pop('_type', None)
        doc['type'] = _type
        body = json.dumps(doc, sort_keys=True, default=str)    
        res = es.index(index=collection, id=str(_id), body=body)
        doc['_id'] = _id
        doc['_type'] = _type

    except Exception as e:
        print('Failed:', str(e))
  if save_local_file:
    f.close()
  print('proccessed', i, 'docs', filename)
  return i

def add_months(date, months):
    months_count = date.month + months

    # Calculate the year
    year = date.year + int(months_count / 12)

    # Calculate the month
    month = (months_count % 12)
    if month == 0:
        month = 12

    # Calculate the day
    day = date.day
    last_day_of_month = calendar.monthrange(year, month)[1]
    if day > last_day_of_month:
        day = last_day_of_month

    new_date = datetime(year, month, day)
    return new_date

def timestamp(datetime_):
  return str(int(datetime.timestamp(datetime_))*1000)

def mongodump(type_name, type_id, start,end, file_name):
  try:
    query_str = '"{'+type_name+': ObjectId(\\"' + str(
        type_id) + '\\"), \$and: [{created:{\$gt: new Date(' + timestamp(start) + ')}},{created:{ \$lt: new Date(' + timestamp(end) + ')}}]}"'

    #use --archive= file for compressed bson
    cmd_command = "mongodump --collection "+ collection +" --uri=\"" + \
        analyticsINT_IL_URI + "\" --query " + query_str + \
        " -o " + file_name
        
    print(cmd_command)
    print("cmd_command")
    res = os.popen(cmd_command).read()
  except Exception as e:
      print('Failed:', str(e))

def exportDataByTimeRangeUsingMongodump(db, type_name, type_id, start, end, bucket, path):
  try:
    dir_path = start.isoformat() 
    mongodump(type_name, type_id, start, end, dir_path)
    if upload_local_file_to_s3 and not dry_run:
      print('upload file to S3 file:', dir_path, 'bucket: ', bucket, 'path', path)
      with open(dir_path+'/downloaded/data.bson', "rb") as f:
        try:
          print('Uploading dir_name', dir_path + '/downloaded/data')
          s3.upload_fileobj(f, bucket, path)
        except Exception as e:
          print('Failed:', str(e))
    os.popen('rm -rf ' + dir_path)
  except Exception as e:
      print('Failed:', str(e))

def exportDataByTimeRange(db, type_name, type_id, start, end, bucket, path):
  # print('query', type_id, 'start', start, 'end', end)
  createria = {type_name: ObjectId(type_id),
               "$and": [
      {"created": {"$gt": start}},
      {"created": {"$lt": end}}
  ]}
  data = db[collection].find(createria)

  saveDocs(data, start.isoformat())

  if upload_local_file_to_s3 and not dry_run:
    print('upload file to S3 file:', start.isoformat(), 'bucket: ', bucket, 'path', path)
    with open(start.isoformat(), "rb") as f:
      try:
        s3.upload_fileobj(f, bucket, path)
      except Exception as e:
        print('Failed:', str(e))
  if save_local_file:
    os.remove(start.isoformat())

def exportByTimeRange(db, start, end, bucket, path):
  gt = {}
  lt = {}
  gt[time_field] = {"$gt": start}
  lt[time_field] = {"$lt": end}
  createria = {"$and": [gt, lt]}
  docs = db[collection].find(createria)

  saveDocs(docs, start.isoformat())

  if upload_local_file_to_s3:
    print('upload file to S3 file:', start.isoformat(),
          'bucket: ', bucket, 'path', path)
    with open(start.isoformat(), "rb") as f:
      try:
        s3.upload_fileobj(f, bucket, path)
      except Exception as e:
        print('Failed:', str(e))
  if save_local_file:
    os.remove(start.isoformat())

def queryCollectionDaily(db, start, end, bucket, path, format='bson'):

  from_time = datetime(start.year, start.month, start.day,
                      minute=0, second=0, microsecond=0, tzinfo=tz.tzutc())
  to_time = datetime(start.year, start.month, start.day, minute=0, second=0, microsecond=0, tzinfo=tz.tzutc()) + day
  while to_time <= end:
    print('running from', from_time, 'to', to_time)

    file_part = '/file.bson' if format == 'bson' else '/file.json'
    s3Path = path + "/" + collection + "/" + from_time.isoformat() + file_part

    if format == 'bson':
      print('bson format not supported for general collection')
    else:
      exportByTimeRange(db, from_time, to_time, bucket, s3Path)
    
    to_time = datetime(to_time.year, to_time.month, to_time.day,
                       minute=0, second=0, microsecond=0, tzinfo=tz.tzutc()) + day
    from_time = datetime(from_time.year, from_time.month, from_time.day,
                         minute=0, second=0, microsecond=0, tzinfo=tz.tzutc()) + day


def runProject(project, format='bson'):
  f = project['from']
  t = project['to']
  db = client[database]
  from_time = datetime(f.year, f.month, f.day, minute=0, second=0, microsecond=0, tzinfo=tz.tzutc())
  to_time = datetime(t.year, t.month, t.day, minute=0, second=0, microsecond=0, tzinfo=tz.tzutc())

  path = 'json/' + collection
  queryCollectionDaily( db,
                        from_time,
                        to_time,
                        project['bucket'],
                        path,
                        format)


def readExporterProjects():
  projectsToRun = []
  projects = client[database].exporterProjects.find({})
  for project in projects:
    projectsToRun.append(project)
  return projectsToRun
        
def updateProjectExec(project, path="json/data"):
  now = datetime.now(tz.tzutc())
  today = datetime(
      now.year, now.month, now.day, minute=0, second=0, microsecond=0, tzinfo=tz.tzutc())
  client.[database].exporterProjects.update_one({"_id": project["_id"]},
                                             {"$set": {
                                                "ended": datetime.now(tz.tzutc()),
                                                "to": today - timedelta(days=project["days_to_keep"]),
                                                "from": project["to"],
                                                "path": path
                                             }
    })

def run(project, format='json'):
  print(json.dumps(project, sort_keys=True, default=str))
  f = project['from']
  t = project['to']

  project['from'] = datetime(
      f.year, f.month, f.day, minute=0, second=0, microsecond=0, tzinfo=tz.tzutc())
  project['to'] = datetime(t.year, t.month, t.day,
                           minute=0, second=0, microsecond=0, tzinfo=tz.tzutc())

  results = runProject(project, format)



# MAIN
if __name__ == '__main__':
  ap = argparse.ArgumentParser()
  ap.add_argument("--bucket", type=str,
                  help="target bucket")

  ap.add_argument("--dry", default=False, help="dry run")

  ap.add_argument("--path",  type=str, 
                  default='.', help="path to save to")

  ap.add_argument("--connection",  type=str, help="mongo connection string")

  ap.add_argument("--keep", type=int, default=365 * 2, help="day to keep in mongo")

  ap.add_argument("--elk", default=False,
                  help="export json to elk ")
  
  ap.add_argument("--elk_url", type=str,  default='http://localhost:9200', 
                  help="elk url")
  
  ap.add_argument("--s3", type=str, 
                  help="export to S3 bucket -- name")

  ap.add_argument("--collection", type=str,
                  help="the collection you'd like to copy")
  
  ap.add_argument("--database", type=str,
                  help="the database where the collection you'd like to copy is stored")
  
  ap.add_argument("--time_field", type=str,  default='timestamp',
                  help="document timestamp field name")
  
  ap.add_argument("--firestore", default=False,
                  help="save json to firestore")

  ap.add_argument("--update", default=False,
                  help="update project collection")
  
  ap.add_argument("--file", default='mongo_exported.dump',
                  help="local file name to hold the exported data")
  
  ap.add_argument('--format', default='json', choices=[
                  'jsonx', 'bson', 'json'], type=str.lower,
                  help="export file format")

  args = ap.parse_args()

  if args.collection:
    print('collection', args.collection)
    collection = args.collection

  if args.database:
    print('database', args.database)
    database = args.database

  if args.connection:
    print('connection str:', args.connection)
    client = pymongo.MongoClient(args.connection)

  if args.time_field:
    print('time_field', time_field)
    time_field = args.time_field

  if args.elk:
    print('save to elk', args.elk)
    save_to_elk = True

  if args.elk_url:
    print('elk url', args.elk_url)
    save_to_elk = True
    elk_url = args.elk_url

  if args.file:
    print('save to file', args.file)
    save_local_file = True
    local_file_name = args.file

  if args.firestore:
    print('save to firestore')
    save_to_firestore = True

  if args.s3:
    print('upload to s3 bucket:', args.s3)
    upload_local_file_to_s3 = True
    if not args.file:
      print('To enable upload to s3 save to file should be enabled, file is deleted upon upload')
      exit(1)
    s3_bucket_name = args.s3
    s3_path = "/" + args.format + "/" + args.collection

  if args.dry:
    print('dry run')
    dry_run = True
    save_to_elk = False
    upload_local_file_to_s3 = False
    save_local_file = False
    save_to_firestore = False
    save_to_elk = False

  projects = readExporterProjects()
  for project in projects:
    run(project, args.format)
    if args.update:
      updateProjectExec(project)
