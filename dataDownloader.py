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

import my_Utils
import my_URL
import my_Time
import DAO



def updateStockMinQuotation():
    # step 1: connect to database
    MysqlCursor , MysqlConn = DAO.MysqlConnector(1)

    # step 2: load the stock list html, and grab all the stock data to database
    # stockList = my_Utils.getBasicStockData()
    # setNum = DAO.setStockBasicData(MysqlCursor, MysqlConn, stockList)

    # step 3: grab data per min,and save to each database(loop)
    shutDownFlag = 0
    while shutDownFlag == 0:
        start_id = 1
        end_id = start_id + 500

        while end_id <= 5001:
            nowDate = my_Time.transTimestampToDate(my_Time.getNowTimestamp())
            shutDownFlag = my_Utils.shutDownTimeDetector(nowDate)
            print '-------------------update a mini-batch------------------'
            print '[' + str(nowDate) + ']'
            print str(start_id) + ' to ' + str(end_id)

            stockList = DAO.getStockBasicData(MysqlCursor, MysqlConn, start_id, end_id)
            stockMinDataPair = my_URL.getMinDataFromURL(1, stockList = stockList)
            DAO.setStockMinData(MysqlCursor, MysqlConn, stockMinDataPair)

            start_id = start_id + 500
            end_id = end_id + 500
            time.sleep(5)

        print '---------oneMin stock data got,now waiting for the next minute---------'
        time.sleep(10)

    # step 4, shut down this program:
    print '---------------------Now the stock market is closed, this program will shut down for a moment------------------------'
    DAO.MysqlCloser(MysqlCursor, MysqlConn)
    sys.exit(0)