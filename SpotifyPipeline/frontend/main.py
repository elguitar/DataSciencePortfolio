import datetime

import pandas as pd
import streamlit as st


@st.cache
def load_data(url):
    return pd.read_csv(url)


def load_daily_listened_data(date):
    url = ("s3://elguitar-data-engineering-demo-bucket/daily_listened_tracks_" + date.strftime("%Y-%m-%d") + ".csv")
    try:
        return load_data(url)
    except FileNotFoundError:
        return pd.DataFrame()


def load_top_artists():
    return load_data("s3://elguitar-data-engineering-demo-bucket/top_artists_2020-11-21.csv")


def _selection_formatter(s):
    """This translates pandas subsampling strings 'W', 'D', 'H' into something
    more human readable."""
    if s == "W":
        return "Week"
    elif s == "D":
        return "Day"
    elif s == "H":
        return "Hour"
    else:
        return "Day"


st.title("Spotify Listening Data")

page = st.sidebar.radio(
        "Select page", ["Date", "Total", "Top lists"])

if page == "Date":
    date = st.date_input("start date", datetime.date(2020, 11, 7))
    df = load_daily_listened_data(date).copy()
    df['played_at'] = pd.to_datetime(df['played_at'])
    df['plays'] = 1
    st.subheader(date)
    st.write(df.set_index('played_at').plays.resample('H').sum())
    st.line_chart(df.set_index('played_at').plays.resample('H').sum())


elif page == "Total":
    date = st.date_input("start date", datetime.date(2020, 11, 7))
    enddate = st.date_input("end date", datetime.date(2020, 11, 10))
    df = load_daily_listened_data(date)
    st.subheader("Totals")
    dfs = [df]
    for d in pd.date_range(start=date + datetime.timedelta(days=1), end=enddate):
        dfs.append(load_daily_listened_data(d))
    df = pd.concat(dfs[::-1]).drop_duplicates().reset_index(drop=True)
    df['played_at'] = pd.to_datetime(df['played_at'])
    df['plays'] = 1
    st.write(df)
    res = st.selectbox(
            "Select the resolution", ("W", "D", "H"), format_func=_selection_formatter)
    st.write(df.set_index('played_at').plays.resample(res).sum())
    st.line_chart(df.set_index('played_at').plays.resample(res).sum())

elif page == "Top lists":
    df = load_top_artists()
    x = st.slider("Select how many results to show at most", 1, 100, 10)
    sorted_df = df.sort_values('n', ascending=False)
    st.write(sorted_df.head(x))
