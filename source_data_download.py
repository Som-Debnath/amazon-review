#####################################################################
##
##    Name : source_data_download.py
##    Desc: Downlading source data file
##
#####################################################################
import sys
import os
import urllib.request

def file_download(args):
    url=args[1]
    dest_file_with_path=args[2]

    if not dest_file_with_path:
        dest_path='./downloaded.gz'

    try:
        urllib.request.urlretrieve(url,dest_file_with_path)
    except Exception as err:
        print(err)


if __name__ == '__main__':
    file_download(sys.argv)


