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

    # step 1: connect to database
    MysqlCursor , MysqlConn = DAO.MysqlConnector(1)

    # step 2:create the basic data database(optional)
    # A stock = 1
    # HK stock = 2
    setNum = dataDownloader.createBasicDatabase(MysqlCursor, MysqlConn, 2)

    # step 3: update Stock Min Quotation
    # shutDownFlag = dataDownloader.updateStockMinQuotation(MysqlCursor , MysqlConn)

    # step 4: update Stock Historical Quotation csv
    shutDownFlag = dataDownloader.updateStockHistoricalQuotation(MysqlCursor, MysqlConn)

    # step 5: shut down this program
    if shutDownFlag == 1:
        print '---------------------Now the stock market is closed, this program will shut down for a moment------------------------'
        DAO.MysqlCloser(MysqlCursor, MysqlConn)
        sys.exit(0)
    if shutDownFlag == 2:
        print '---------------------stock historical quotation CSVs have been downloaded------------------------'
        DAO.MysqlCloser(MysqlCursor, MysqlConn)
        sys.exit(0)