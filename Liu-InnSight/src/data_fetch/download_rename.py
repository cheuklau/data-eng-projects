#!/usr/bin/env python

from urllib.request import urlopen
import sys


def download(url, filename):
    """
    Download link content to a file.

    Input:
        - url (str): download link
        - filename (str): name of the file to store output

    Output:
        None
    """

    data = urlopen(url).read()
    f = open(filename, 'wb')
    f.write(data)
    f.close()


def rename(url):
    """
    Renames a download link to a more friendly file format.

    Input:
        - url (str): download link

    Output:
        - filename (str): renamed download link
    """

    # Grab everything after .com
    data_info = url.split(".com/")[1]

    # Split the above result on backslash and join selected elements with underscores
    dataname = data_info.split("/")
    filename = dataname[0] + "_" + dataname[1] + "_" + dataname[2] + "_" + dataname[3] + "_" + dataname[5]
    return filename


if __name__ == '__main__':
    """
    Usage: ./download_rename.py <download links file> <output file>
    """

    file = sys.argv[1]
    save_path = sys.argv[2]
    f = open(file, "r")
    # Loop over all the download links
    for line in f.read().splitlines():
        print(line)
        try:
            # Perform download with renamed output file name
            download(line, save_path + rename(line))
        except Exception as e:
            print(f"Link unavailable with exception: ${e}")
            pass
