#!/usr/bin/env python3

import csv, datetime, sys, time
import requests
import soundcloud

import config

licenses = [
    ##"no-rights-reserved",
    #####"all-rights-reserved",
    "cc-by",
    #"cc-by-nc",
    #"cc-by-nd",
    #"cc-by-sa",
    #"cc-by-nc-nd",
    #"cc-by-nc-sa"
]

page_size = 100

# https://blog.soundcloud.com/2008/10/17/cc/
date_start = datetime.datetime(2016, 2, 6)
date_today = datetime.date.today()
date_stride = datetime.timedelta(days=1)
date_to_offset = datetime.timedelta(hours=23, minutes=59, seconds=59)

def created_at_range (date):
    date_to = date + date_to_offset
    print(date.strftime("%Y-%m-%d %H:%M:%S"))
    print(date_to.strftime("%Y-%m-%d %H:%M:%S"))
    return {"from": date.strftime("%Y-%m-%d %H:%M:%S"),
            "to": date_to.strftime("%Y-%m-%d %H:%M:%S")}

def escape (field):
    if field:
        field = field.replace("\n", "\\n")
        field = field.replace("\r", "\\r")
        field = field.replace("\t", "\\t")
    return field

def print_track (writer, track):
    writer.writerow((track.download_url,
                     track.license,
                     track.uri,
                     escape(track.title),
                     escape(track.description),
                     track.created_at,
                     escape(track.genre),
                     escape(track.tag_list),
                     escape(track.track_type),
                     escape(track.user['username']),
                     escape(track.label_name)))

def print_tracks (writer, tracks):
    for track in tracks.collection:
        print_track(writer, track)

def get_next_href (tracks):
    try:
        next_href = tracks.next_href
    except Exception as e:
        next_href = False
    return next_href

#TODO: check for urlsfile and use last url if appropriate
# use the offset from the last url if that's invalid
# or just restart if not present/catastrophic failure
# get first 100 tracks

def initial_fetch (client, csvwriter, license_to_find):
    try:
        tracks = client.get('/tracks', license=license_to_find,
                            created_at=created_at_range(date_start),
                            filter='public', order='created_at',
                            limit=page_size, linked_partitioning=1)
    except requests.HTTPError as e:
        print(e)
        return False
    except Exception as e:
        print(e)
        exit(1)
    print_tracks(csvwriter, tracks)
    return get_next_href(tracks)

def subsequent_fetches (client, csvwriter, urlsfile, next_href):
    while next_href:
        print(".")
        print(next_href, file=urlsfile)
        # Make sure it's written immediately
        urlsfile.flush()
        time.sleep(1)
        try:
            tracks = client.get(next_href)
        except requests.HTTPError as e:
            print(e)
            return
        except Exception as e:
            print(e)
        print_tracks(csvwriter, tracks)
        next_href = get_next_href(tracks)

def fetch_all_licenses_sequentially (client, licenses):
    for license_to_find in licenses:
        with open(license_to_find + '.tsv', mode='w',
                  encoding='utf-8') as outfile:
            csvwriter = csv.writer(outfile, delimiter="\t")
            urlsfile = open(license_to_find + '-urls.txt', 'w+')
            next_href = initial_fetch(client, csvwriter, license_to_find)
            subsequent_fetches (client, csvwriter, urlsfile, next_href)

if __name__ == "__main__":
    client = soundcloud.Client(client_id=config.client_id)
    fetch_all_licenses_sequentially(client, licenses)
