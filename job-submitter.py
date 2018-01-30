import argparse
import os
import subprocess
import sqlite3
import requests
from config import *


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


def submit_file(run, file_id, conn, cursor):
    """
    Submit a file to be analysed on the grid
    :param file_id: The unique ID of the file being analysed
    :return:
    """
    jdl = """
    [
    Executable = "wrapper.sh";
    Arguments = "@@file@@";
    inputSandbox = { "wrapper.sh", "analyse.py", "all_tles.json", "db.db", "config.py"};
    StdOutput = "sbx.out";
    StdError = "sbx.err";
    OutputSandbox = {"sbx.out", "sbx.err"};
    outputsandboxbasedesturi = "gsiftp://localhost";
    GPUNumber=1;
    VirtualOrganisation = "cernatschool.org";
    ]
    """
    jdl_out = jdl.replace("@@file@@", str(run) + " " + str(file_id))

    with open("job_" + str(file_id) + ".jdl", "w+") as f:
        f.writelines(jdl_out)

    subprocess.call(["glite-ce-job-submit", "-a", "-r", "ice.esc.qmul.ac.uk:8443/cream-slurm-sl6_lcg_gpu", "job_" + str(file_id) + ".jdl", "-o", "output_" + str(file_id)])

    cursor.execute('INSERT INTO status (file, processed, run) VALUES(?, ?, ?)', (file_id, 1, run))

    conn.commit()


if __name__ == "__main__":
    """
    Why we use two databases - the 'db.db' file will be uploaded continuously to grid nodes - and we don't want to be 
    uploading it if we are modifying data in it.
    """
    parser = argparse.ArgumentParser(description='Submit a run of LUCID data')
    parser.add_argument('run', metavar='r', type=str, help='the start date of the run')
    args = parser.parse_args()

    post_run(args.run)

    conn1 = sqlite3.connect('db.db')
    cursor1 = conn1.cursor()
    conn2 = sqlite3.connect('status.db')
    cursor2 = conn2.cursor()

    cursor2.execute('SELECT file FROM status WHERE processed = 1')

    processed_files = cursor2.fetchall()

    cursor1.execute('SELECT * FROM DATA_FILES WHERE Run = ? AND Id NOT IN (?)', (args.run, ",".join(map(str, processed_files))))

    data_files = cursor1.fetchall()

    for data_file in data_files:
        submit_file(args.run, data_file[0], conn2, cursor2)



