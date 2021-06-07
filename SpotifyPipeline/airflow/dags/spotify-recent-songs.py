import csv
from datetime import datetime, timedelta
from io import StringIO
import json
import pathlib
from pprint import pprint

from airflow import DAG
from airflow.hooks import S3_hook
from airflow.operators.python_operator import PythonOperator
import spotipy
from spotipy.oauth2 import SpotifyOAuth

default_args = {
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime.now() - timedelta(days=1),
}

BUCKET_NAME="elguitar-data-engineering-demo-bucket"

def get_s3_object(check=False):
    """Simple wrapper, which ensures that the bucket exists if wanted"""
    sthree = S3_hook.S3Hook()
    if check and not sthree.check_for_bucket(BUCKET_NAME):
        sthree.create_bucket(BUCKET_NAME)
    return sthree

def save_to_s3(data, key):
    sthree = get_s3_object()
    return sthree.load_string(data, key, BUCKET_NAME, replace=True)

def save_as_json_to_s3(data, key):
    return save_to_s3(json.dumps(data), key)

def s3_key_exists(key):
    sthree = get_s3_object()
    return sthree.check_for_key(key, bucket_name=BUCKET_NAME)

def read_from_s3(key):
    sthree = get_s3_object()
    return sthree.read_key(key, bucket_name=BUCKET_NAME)

def read_json_from_s3(key):
    return json.loads(read_from_s3(key))

def read_csv_from_s3(key):
    data = read_from_s3(key)
    f = StringIO(data)
    reader = csv.DictReader(f)
    objects = []
    for row in reader:
        objects.append(row)
    return objects

def timestamp(dt_object, **kwargs):
    return int(datetime.timestamp(dt_object, **kwargs))*1000

def csv_string(objects):
    output = StringIO()
    fieldnames = objects[0].keys()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for obj in objects:
        writer.writerow(obj)
    return output.getvalue()

with DAG(
    'fetch_recent_tracks',
    default_args=default_args,
    description="Fetch the recently played tracks from Spotify Web API and store them to S3",
    schedule_interval=timedelta(minutes=25),
    catchup=False,
) as dag:

    def print_context(ds, **kwargs):
        pprint(kwargs)
        print(ds)
        return "Starting the retrieving"

    def fetch_listens_from_spotify():
        """Fetch daily 'recently listened tracks' from Spotify."""

        scope = "user-read-recently-played"
        sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))

        try:
            data = sp.current_user_recently_played(
                    limit=50, # Fetch all the data at once
            )
        except requests.exceptions.ReadTimeout:
            print("Timed out!")
            return 1

        now = datetime.now()
        key = f"recently_listened_{timestamp(now)}.json"
        save_as_json_to_s3(data, key)
        return key


    def generate_tracks_list(**context):
        key = context['task_instance'].xcom_pull(task_ids='fetch_spotify_listens')
        data = read_json_from_s3(key)
        song_list = []
        for x in data.get('items', []):
            played_at = x.get('played_at', None)

            t = x.get('track', {})
            song_list.append({
                'played_at': played_at,
                'artist': t.get('artists', [{}])[0].get('name', None),
                'album': t.get('album', {}).get('name', None),
                'popularity': t.get('popularity', None),
                'name': t.get('name', None),
                'explicit': t.get('explicit', None),
                'id': t.get('id', None),
            })
        now = datetime.now()
        key = f"track_list_{timestamp(now)}.csv"
        save_to_s3(csv_string(song_list), key)
        return key

    def generate_artists_list(**context):
        key = context['task_instance'].xcom_pull(task_ids='fetch_spotify_listens')
        data = read_json_from_s3(key)
        artist_list = []
        for x in data.get('items', []):
            for a in x.get('track', {}).get('artists', [{}]):
                artist_list.append({'name': a.get('name', None)})
        now = datetime.now()
        key = f"artist_list_{timestamp(now)}.csv"
        save_to_s3(csv_string(artist_list), key)
        return key

    def fetch_track_analysis_from_spotify(**context):
        """Fetch track analysis from Spotify."""
        key = context['task_instance'].xcom_pull(task_ids='generate_track_list')
        track_list = read_csv_from_s3(key)
        print(track_list)

        scope = "user-read-recently-played"
        sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))

        created = 0
        skipped = 0
        failed = 0
        total = len(track_list)

        keys = []

        for track in track_list:
            track_id = track.get('id', None)
            key = f"track_analysis_{track_id}.json"
            if not s3_key_exists(key):
                try:
                    data = sp.audio_analysis(track_id)
                except requests.exceptions.ReadTimeout:
                    failed += 1
                    print(f"Fetching analysis for {track.get('name')} timed out!")
                save_as_json_to_s3(data, key)
                keys.append(key)
                created += 1
            else:
                skipped += 1
        print(f"Created: {created}\nSkipped: {skipped}\nFailed: {failed}\nTotal: {total}")
        return keys


    def clean_the_track_analysis(**context):
        keys = context['task_instance'].xcom_pull(task_ids='fetch_tracks_analysis_from_spotify')
        analyses = []
        if keys == []:
            return
        for key in keys:
            analysis = read_json_from_s3(key)

            include_cols = ("id", "duration", "loudness", "tempo", "time_signature", "key", "mode")
            analyses.append({k: analysis['track'][k] for k in include_cols})

        now = datetime.now()
        key = f"clean_track_analysis_{timestamp(now)}.csv"
        save_to_s3(csv_string(analyses), key)
        return key

    def upsert_to_track_list(**context):
        key = context['task_instance'].xcom_pull(task_ids='generate_track_list')
        print(key)
        last_track_list = read_csv_from_s3(key)
        track_list = read_csv_from_s3('the_track_list.csv')
        track_set = {t['id'] + t['played_at'] for t in track_list}
        status = {"Added":0, "Skipped":0}
        for track in last_track_list:
            if track['id'] + track['played_at'] not in track_set:
                track_list.append(track)
                status["Added"] += 1
            else:
                status["Skipped"] += 1
        save_to_s3(csv_string(track_list), 'the_track_list.csv')
        print(status)




    context = PythonOperator(
        task_id = 'print_the_context',
        provide_context = True,
        python_callable = print_context,
    )

    fetch = PythonOperator(
        task_id = 'fetch_spotify_listens',
        python_callable = fetch_listens_from_spotify,
    )

    list_tracks = PythonOperator(
        task_id = 'generate_track_list',
        python_callable = generate_tracks_list,
        provide_context = True,
    )

    list_artists = PythonOperator(
        task_id = 'generate_artist_list',
        python_callable = generate_artists_list,
        provide_context = True,
    )

    fetch_analysis = PythonOperator(
        task_id = 'fetch_tracks_analysis_from_spotify',
        python_callable = fetch_track_analysis_from_spotify,
        provide_context = True,
    )

    clean_analysis = PythonOperator(
        task_id = 'clean_track_analysis',
        python_callable = clean_the_track_analysis,
        provide_context = True,
    )

    upsert_track_list = PythonOperator(
        task_id = 'upsert_track_list',
        python_callable = upsert_to_track_list,
        provide_context = True,
    )

    context >> fetch >> [list_artists, list_tracks]

    list_tracks >> fetch_analysis >> clean_analysis
    list_tracks >> upsert_track_list
