import csv
from io import StringIO

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
track_keys = []
while cont or first_time:
    first_time = False
    args = {
        'Bucket': 'elguitar-data-engineering-demo-bucket',
        'MaxKeys': 1000,
        'Prefix': 'track_list_',
    }
    if cont:
        args['ContinuationToken'] = cont
    r = s3.list_objects_v2(
        **args
    )
    cont = r.get('ContinuationToken', False)
    objs = r.get('Contents', [])
    for obj in objs:
        track_keys.append(obj.get('Key'))

track_list_r = s3.get_object(
    Bucket='elguitar-data-engineering-demo-bucket',
    Key='the_track_list.csv',
)
track_list = read_csv(track_list_r.get('Body') \
                      .read().decode('utf-8'))
track_list = []  # REMOVE ME
track_set = {t['played_at'] for t in track_list}
s = {"Added": 0, "Skipped": 0}
track_key_amount = len(track_keys)
for i,key in enumerate(track_keys):
    r = s3.get_object(
        Bucket="elguitar-data-engineering-demo-bucket",
        Key=key,
    )
    tracks = read_csv(r.get('Body') \
                      .read().decode('utf-8'))
    for t in tracks:
        if t.get('played_at') not in track_set:
            track_set.add(t.get('played_at'))
            track_list.append(t)
            s["Added"] += 1
        else:
            s["Skipped"] += 1
    print(f"{i}/{track_key_amount}")
    print(s)
s3.put_object(
    Body=to_csv(track_list),
    Bucket="elguitar-data-engineering-demo-bucket",
    Key="the_track_list.csv"
)
