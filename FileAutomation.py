import ftplib
import os
import datetime
import logging
import shutil
import schedule
import time

import LoginInformation

# Constants
date_format = "%d-%m-%Y"
logging.basicConfig(filename="file_download.log", encoding='utf-8', level=logging.DEBUG)

# This should be an actual path to directory handled by the local network, but without that
# implementation, I will create another local directory
internal_network_path = os.path.join(os.getcwd(), "InternalNetwork")
# just to make sure the program runs
if not os.path.isdir(internal_network_path):
    os.mkdir(internal_network_path)


def ftp_connection():
    logging.info(f"Connection attempted at {LoginInformation.server_name}. at {datetime.datetime.now()}")
    ftp = ftplib.FTP(host=LoginInformation.server_name,
                     user=LoginInformation.username,
                     passwd=LoginInformation.password)
    status = ftp.getwelcome()
    if status[:3] == "220":
        logging.info(f"Connection successful. {status}")
    else:
        logging.warning(f"Connection received {status[:3]} response.")
    return ftp


def download_files(ftp, day):
    base_path = os.path.join(os.getcwd(), "DownloadedFiles")
    download_path = os.path.join(base_path, day.strftime(date_format))

    # setting up local download directory:
    # let's assume every time we download from the server we get the most updated files
    # so any previous file is no longer needed
    if os.path.isdir(download_path):
        logging.info(f"Directory contents purged at {download_path} for day {day.strftime(date_format)}")
        for root, dirs, files in os.walk(download_path):
            for file in files:
                os.remove(os.path.join(root, file))
    else:
        logging.info(f"Setting up download folder at {download_path} for day {day.strftime(date_format)}")
        os.mkdir(download_path)

    # download files
    for file in ftp.nlst():
        # ftp doesn't allow for a straightforward check if an item in the directory is a directory
        # or a file, so we'll assume the files we download are all .txts
        # this step's implementation depends on the file system used at the upload level
        # could be a method call check_valid_file(file), but for the sake of simplicity:
        if ".txt" not in file:
            continue
        ftp.retrbinary("RETR " + file, open(os.path.join(download_path, file), 'wb').write)
        logging.info(f"File {file} downloaded at {datetime.datetime.now()}")


def move_files(day, destination):
    local_path = os.path.join(os.getcwd(), "DownloadedFiles", day.strftime(date_format))
    network_path = os.path.join(destination, day.strftime(date_format))

    # make sure the directory exists on the local network
    # depending on how the internal network is handled, this step may need a different implementation
    # other than that, os should be able to create valid paths for everything
    if not os.path.isdir(network_path):
        os.mkdir(network_path)

    for file in os.listdir(local_path):
        # it is guaranteed all of them are files
        source_path = os.path.join(local_path, file)
        destination_path = os.path.join(network_path, file)
        shutil.move(source_path, destination_path)
        logging.info(f"File {file} moved from {source_path} to {destination_path}.")

    # all files have been moved, we can delete the directory
    os.rmdir(local_path)


def daily_task(day=datetime.date.today()):
    ftp = ftp_connection()
    download_files(ftp, day)
    move_files(day, internal_network_path)
    ftp.close()


def scheduler():
    logging.info(f"\n Running daily file download for {datetime.date.today()} \n")
    schedule.every().day.at("12:30", "Europe/Berlin").do(daily_task)
    while True:
        schedule.run_pending()
        time.sleep(10)

# for testing
daily_task(datetime.date(2023, 10, 7))