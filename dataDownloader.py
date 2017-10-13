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



def createBasicDatabase(MysqlCursor, MysqlConn, type):
    # load the stock list html, and grab all the stock data to database
    stockList = my_Utils.getBasicStockData(type)
    setNum = DAO.setStockBasicData(MysqlCursor, MysqlConn, stockList)
    return setNum



def updateStockMinQuotation(MysqlCursor , MysqlConn):
    # grab data per min,and save to each database(loop)
    shutDownFlag = 0
    while shutDownFlag == 0:
        start_id = 1
        end_id = start_id + 1800

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

    return shutDownFlag



def updateStockHistoricalQuotation(MysqlCursor , MysqlConn):
    shutDownFlag = 0
    # step 1: get stockList from database
    start_id = 1
    end_id = start_id + 5000
    stockList = DAO.getStockBasicData(MysqlCursor, MysqlConn, start_id, end_id)
    # step 2: format the code to wangyi format
    formatted_codeList = []
    for stock in stockList:
        formattedCode = my_Utils.stockMarketFormatter(3, code=stock[0], market=stock[2])
        formatted_codeList.append(formattedCode)
    # step 3: get formatted date
    endTimestamp = my_Time.getNowTimestamp()
    startTimestamp = my_Time.getGapTimestamp(endTimestamp, 1000)
    endDateTemp = my_Time.transTimestampToDate(endTimestamp)
    startDateTemp = my_Time.transTimestampToDate(startTimestamp)
    endDate = endDateTemp.split(' ')[0].replace('-', '')
    startDate = startDateTemp.split(' ')[0].replace('-', '')

    # formatted_codeList is always a full list
    # iter_codeList is the list that program need to download in this loop
    # CsvFileLossList will be use at last
    iterTimes = 0
    CsvFileLossList = []
    while iterTimes < 1:
        # step 4: detect which csv is not downloaded
        iter_codeList = my_Utils.CsvFileNotDownloadedDetector(formatted_codeList)
        # step 5: download the csv
        my_URL.DownloadCsv(1, formatted_codeList = iter_codeList, startDate = startDate, endDate = endDate)
        # step 6: detect the loss csv and delete them
        CsvFileLossList = my_Utils.CsvFileLossDetector(formatted_codeList, 1000)
        iterTimes = iterTimes + 1
    # step 7: when,it is all over, update the csv situation to database
    DAO.setCsvTag(MysqlCursor, MysqlConn, CsvFileLossList)
    shutDownFlag = 2
    return shutDownFlag


