#####################################################################
##
##    Name : source_data_download.py
##    Desc: Downlading source data file
##
#####################################################################
import sys
import os
import urllib.request
import wget

def file_download(url,dest_file_with_path):

    print (url)

    try:
        if not os.path.exists(dest_file_with_path):
            wget.download(url,dest_file_with_path)
    except Exception as err:
        print(err)


if __name__ == '__main__':
    url = sys.argv[1]
    dest_file_with_path=sys.argv[2]
    file_download(url,dest_file_with_path)