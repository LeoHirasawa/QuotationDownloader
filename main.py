# -*- coding: UTF-8 -*-
import types
import urllib2
import json
import sys
import csv
import MySQLdb
import chardet
import time
import datetime
import math
import os
import re
from bs4 import BeautifulSoup

import my_Utils
import my_URL
import my_Time
import DAO
import dataDownloader

# from DAO import SampleDAO



if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding('utf-8')
    dataDownloader.updateStockMinQuotation()
