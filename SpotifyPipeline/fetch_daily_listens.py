import argparse
from datetime import date, datetime, time, timedelta
import pprint

import spotipy
from spotipy.oauth2 import SpotifyOAuth



def timestamp(dt_object, **kwargs):
    return int(datetime.timestamp(dt_object, **kwargs))*1000


def fetch_listens_from_spotify(the_date=date.today()):
    # None defaults to today
    if the_date is None:
        the_date = date.today()

    scope = "user-read-recently-played"
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))

    day_start = datetime.combine(the_date, time(0,0,0,0))
    next_day = the_date + timedelta(days=1)

    # Do in a loop
    # while results['next'] and the "next" timestamp is not over yesterday_end
    recently_played = sp.current_user_recently_played(
            limit=50,
            after=timestamp(day_start),
    )

    pp = pprint.PrettyPrinter()
    pp.pprint(recently_played)

def main():
    p = argparse.ArgumentParser(description="Fetch daily listens from Spotify")
    p.add_argument('--date', type=date.fromisoformat)
    args = p.parse_args()
    fetch_listens_from_spotify(args.date)

if __name__ == "__main__":
    main()
