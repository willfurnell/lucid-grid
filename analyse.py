from lucid_utils import xycreader, blobbing, telemetry
from lucid_classifiers.analysis import classify
import requests
import json
import datetime
import ephem
import sqlite3
import argparse
import tensorflow as tf
import os
from lucid_utils.classification.lucid_algorithm_data import classify as metric_classify

from config import *

tle_file = json.load(open("all_tles.json", 'r'))
conn = sqlite3.connect('db.db')
cursor = conn.cursor()


def get_tle(timestamp):
    """
    Get a TLE (Two Line Element) file for a given timestamp
    :param date: UNIX timestamp
    :return: The TLE file
    """
    times = []

    date = datetime.datetime.utcfromtimestamp(timestamp)

    for tle in tle_file:
        # 2014-07-08 22:43:05
        times.append(datetime.datetime.strptime(tle['EPOCH'], '%Y-%m-%d %H:%M:%S'))

    # https://stackoverflow.com/questions/43496246/pythonic-way-to-find-datetime-with-smallest-time-difference-to-target-from-list/43496620

    min_diff = min(enumerate(times), key=lambda t: abs(date - t[1]))

    return tle_file[min_diff[0]]['TLE_LINE0'], tle_file[min_diff[0]]['TLE_LINE1'], tle_file[min_diff[0]]['TLE_LINE2']


def get_lat_and_long(timestamp):
    """
    Get latitude and longitude for a given timestamp
    :param timestamp:
    :return:
    """

    line1, line2, line3 = get_tle(timestamp)

    etle = ephem.readtle(line1, line2, line3)

    try:
        etle.compute(datetime.datetime.utcfromtimestamp(timestamp))
    except ValueError as e:
        print("Exception encountered with this frame: {}".format(e))

    latitude = telemetry.dms_to_dd(etle.sublat)
    longitude = telemetry.dms_to_dd(etle.sublong)

    return latitude, longitude


def post_run(date):
    """
    Add a new run to TAPAS
    :param date: The start date of the run
    :param ptr: The PTR file used
    :return: the request object
    """
    payload = {
        'run_start': date,
    }

    r = requests.post((API_BASE_URL + "lucidrun/"), data=payload, headers=HEADERS)

    print(r.json())

    return r


def post_file(id, start_time, end_time, config, run):
    """
    Add a new file to TAPAS
    :param id: The unique ID of the file
    :param start_time: The UNIX start time of the capture
    :param end_time: The UNIX end time of the capture
    :param config: The config used
    :param run: The URL to the run
    :return: the request object
    """
    payload = {
        'id': id,
        'start_time': start_time,
        'end_time': end_time,
        'config': config,
        'run': run,
    }

    r = requests.post(API_BASE_URL + "lucidfile/", data=payload, headers=HEADERS)

    print(r.json())

    return r


def post_frame(capture_time, detector, counts, processed, file, latitude, longitude, number):
    """
    Add a new frame to TAPAS
    :param capture_time: UNIX capture time
    :param detector: The detector used (0-4)
    :param counts: Dictionary of particle counts
    :param processed: True or False, whether frame was successfully processed
    :param file: The URL to the corresponding file
    :param latitude: Latitude of capture
    :param longitude: Longitude of capture
    :return: the request object
    """
    payload = {
        'capture_time': capture_time,
        'detector': detector,
        'alpha': counts['alpha'],
        'beta': counts['beta'],
        'gamma': counts['gamma'],
        'proton': counts['proton'],
        'muon': counts['muon'],
        'other': counts['other'],
        'processed': processed,
        'file': file,
        'latitude': latitude,
        'longitude': longitude,
        'number': number,
    }

    r = requests.post(API_BASE_URL + "lucidframe/", data=payload, headers=HEADERS)

    print(r.json())

    return r


def analyse_run(run):
    """
    Analyse a given run of data, using Tensorflow

    Using the existing LUCID data browser database, get the information we need for a run, file and frame.

    Note: This function has been kept for smaller runs - but now a job should be submitted for every file. This way we
    don't hit the 24h limit.

    :param run: The run to analyse (2017-01-01)
    :return:
    """

    post_run(run)

    cursor.execute('SELECT * FROM DATA_FILES WHERE Run == ?', (run,))

    data_files = cursor.fetchall()

    for data_file in data_files:
        analyse_file(run, data_file)


def analyse_file(run, data_file_in):
    """
    Analyse a given fi.e of data, using Tensorflow

    Using the existing LUCID data browser database, get the information we need for a run, file and frame.
    :param run: The run that the file is part of
    :param data_file: The file ID
    :return:
    """

    cursor.execute('SELECT * FROM DATA_FILES WHERE Id == ?', (data_file_in,))

    data_file = cursor.fetchone()

    tf.reset_default_graph()

    classes = ["gamma", "beta", "muon", "proton", "alpha", "other"]
    model_name = os.path.sep + 'model5.meta'
    log_dir = os.path.join('/cvmfs/researchinschools.egi.eu/software/miniconda3/lib/python3.6/site-packages/lucid_classifiers/', 'neural_model')

    checkpoint = tf.train.latest_checkpoint(log_dir)
    # Launch the graph in a session
    with tf.Session() as sess:
        # you need to initialize all variables
        sess.run(tf.global_variables_initializer())

        saver = tf.train.import_meta_graph(log_dir + model_name)
        saver.restore(sess, checkpoint)
        X = tf.get_collection("X")[0]
        predict_op = tf.get_collection('predict_op')[0]
        full_op = tf.get_collection('full_op')[0]

        # folder is 10 digit long -- left padded 0s
        data_file_id = '{0:010d}'.format(data_file[0])

        if data_file[5] == 'Unknown':
            # Database requires configs to be integers, 0 will be one that is unknown
            config = 0
        else:
            config = data_file[5]

        post_file(id=data_file_id, start_time=data_file[2], end_time='', config=config,
                  run=API_BASE_URL + "lucidrun/" + run + "/")

        for f in range(1, data_file[7] + 1):
            for c in data_file[6].split(","):

                cursor.execute('SELECT * FROM frames WHERE Data_file == ? AND Frame_number == ?', (str(data_file[0]), str(f)))

                db_frame = cursor.fetchone()

                try:
                    # Analyse frame
                    counts = {'alpha': 0, 'beta': 0, 'gamma': 0, 'proton': 0, 'muon': 0, 'other': 0}
                    frame = xycreader.read(run + "/" + str(data_file_id) + "/frame" + str(f) + "c" + str(c) + ".txt")
                    clusters = blobbing.find(frame)
                    for cluster in clusters:
                        index = sess.run(predict_op, feed_dict={X: [metric_classify(cluster)]})[0]
                        counts[classes[index]] += 1

                    lat, long = get_lat_and_long(db_frame[0])

                    post_frame(db_frame[0], c, counts, 1, API_BASE_URL + "lucidfile/" + data_file_id + "/", lat, long, f)
                except FileNotFoundError as e:
                    print("Exception encountered with this frame: {}".format(e))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process a file of LUCID data')
    parser.add_argument('run', metavar='r', type=str, help='the start date of the run')
    parser.add_argument('file', metavar='f', type=str, help='the file ID')
    args = parser.parse_args()
    analyse_file(args.run, args.file)
