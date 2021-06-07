import csv
from io import StringIO
import json
import re

import boto3

def read_csv(data):
    f = StringIO(data)
    reader = csv.DictReader(f)
    objects = []
    for row in reader:
        objects.append(row)
    return objects

def to_csv(objects):
    output = StringIO()
    fieldnames = objects[0].keys()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for obj in objects:
        writer.writerow(obj)
    return output.getvalue()

s3 = boto3.client('s3')
cont = ""
first_time = True
analysis_keys = []
while cont or first_time:
    first_time = False
    args = {
        'Bucket': 'elguitar-data-engineering-demo-bucket',
        'MaxKeys': 1000,
        'Prefix': 'track_analysis_',
    }
    if cont:
        args['ContinuationToken'] = cont
    r = s3.list_objects_v2(
        **args
    )
    cont = r.get('ContinuationToken', False)
    objs = r.get('Contents', [])
    for obj in objs:
        analysis_keys.append(obj.get('Key'))

"""
analysis_list_r = s3.get_object(
    Bucket='elguitar-data-engineering-demo-bucket',
    Key='the_analysis_list.csv',
)
analysis_list = read_csv(analysis_list_r.get('Body') \
                      .read().decode('utf-8'))
"""
analysis_list = []
analysis_set = {a['id'] for a in analysis_list}
s = {"Added": 0, "Skipped": 0}
analysis_key_amount = len(analysis_keys)
include_cols = ("duration", "loudness", "tempo", "time_signature", "key", "mode")
id_matcher = re.compile('track_analysis_(.*)\.json')
for i,key in enumerate(analysis_keys):
    r = s3.get_object(
        Bucket="elguitar-data-engineering-demo-bucket",
        Key=key,
    )
    analysis = json.loads(r.get('Body') \
                      .read().decode('utf-8'))
    if analysis.get('id') not in analysis_set:
        small_analysis = {k: analysis['track'][k] for k in include_cols}
        small_analysis['id'] = id_matcher.match(key)[1]
        analysis_list.append(small_analysis)
        analysis_set.add(small_analysis.get('id'))
        s["Added"] += 1
    else:
        s["Skipped"] += 1
    print(f"{i}/{analysis_key_amount}")
    print(s)
s3.put_object(
    Body=to_csv(analysis_list),
    Bucket="elguitar-data-engineering-demo-bucket",
    Key="the_analysis_list.csv"
)
