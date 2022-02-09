from ftplib import FTP
from datetime import datetime, timedelta
import os
import fnmatch
import logging


def ConnectFtp():
    try:
        ftp = FTP()
        ftp.connect("212.175.180.253")
        ftp.login("ebim", "ebim2012*")
        print("Connected to fpt")
    except:
        raise Exception("Connection to FTP failed!")

    return ftp


def GetFiles(time_diff, path_to_download):
    logging.basicConfig(filename=os.path.join(path_to_download, 'logger.log'), level=logging.INFO)
    today = datetime.now()  # current date and time
    past = today + timedelta(days=-(time_diff))

    end_time = past.strftime("%Y%m%d")

    ftp = ConnectFtp()
    direc = "/G:/fdd_f/meteor.gov.tr/HSAF/NOAA/Validasyon/StationObservation/DailyObservations/"
    ftp.cwd(direc)

    list_of_files = ftp.nlst()

    for i in range(time_diff + 1):
        try:
            c = datetime.strptime(end_time, "%Y%m%d") + timedelta(days=i)
            data_to_download = c.strftime("%Y%m%d")
            for name in list_of_files:
                if fnmatch.fnmatch(name, data_to_download + "*"):
                    with open(os.path.join(path_to_download, name), "wb") as f:
                        ftp.retrbinary("RETR {}".format(name), f.write)
                        logging.info(f"Date {data_to_download} downloaded")
        except:
            logging.info(f"Date {data_to_download} failed")
    ftp.quit()
    print("Observations were downloaded between" + " " + end_time + " " + "and" + " " + data_to_download)
    return end_time


if __name__ == '__main__':
    # time difference as days
    time_diff = 3
    out_path = r"C:\Users\hsaf2\Desktop\DBupdate\Data"
    GetFiles(time_diff, out_path)
