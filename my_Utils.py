# -*- coding: UTF-8 -*-
import csv
import os
import datetime
import time
import re
import chardet
import sys
from bs4 import BeautifulSoup

import global_list
import my_URL
import my_Time
import DAO



def isNum(myChar):
    flag = False
    flag = str(myChar).isdigit()
    return flag



def getBasicStockData():
    # get all stock basic data from html file
    if os.path.exists('./files/name_code_list_ch.html') and os.path.getsize('./files/name_code_list_ch.html'):
        print 'grab stock basic info from local data...'
        soup = BeautifulSoup(open('./files/name_code_list_ch.html'), 'html.parser')
        metaHtml = soup.find_all('a', target = '_blank', href = re.compile('http://quote.eastmoney.com/[s(h|z)]'))
        stockList = []
        for data in metaHtml:
            if '<strong>' not in str(data.contents[0]):
                rawStr = data.contents[0]
                tempList = str(rawStr).split('(')
                stockName = tempList[0]
                stockCode = tempList[1].split(')')[0]
                stockMarket = stockMarketFormatter(1, code = stockCode)
                stockList.append((stockCode, stockName, stockMarket))
    else:
        stockList = None
    return stockList



def stockMarketFormatter(type, **kwargs):
    # type:
    # use code(only number) to judge market = 1
    # add market to code = 2
    # separate market and code from code = 3

    code = None
    market = None
    for key in kwargs:
        if key == 'code':
            code = kwargs[key]
        if key == 'market':
            market = kwargs[key]

    if type == 1:
        stockMarket = None
        if code != None:
            judgeChar = code[0:1]
            if judgeChar == '0' or judgeChar == '1' or judgeChar == '3':
                stockMarket = 'SZ'
            if judgeChar == '6' or judgeChar == '9':
                stockMarket = 'SH'
            if judgeChar == '2':
                stockMarket = 'SH/SZ'
            if judgeChar == '4' or judgeChar == '8':
                stockMarket = 'OC'
            if judgeChar == '5' or judgeChar == '7':
                # actually, 5 is fund
                stockMarket = 'NA'
                pass
        else:
            pass
        return stockMarket
    elif type == 2:
        # now the function is only target for sina format
        formattedCode = None
        if code != None and market != None:
            if market == 'SH':
                formattedCode = 'sh' + str(code)
            elif market == 'SZ':
                formattedCode = 'sz' + str(code)
            elif market == 'SH/SZ':
                # not now
                pass
            else:
                pass
        else:
            pass
        return formattedCode
    elif type == 3:
        pass
    else:
        pass
    return None



def crashDetector():
    if global_list.GLOBAL_REQUEST_ERROR_COUNT > 5:
        fileName = "./files/crashLog.txt"
        txt = open(fileName, 'w')
        # txt.truncate()
        txt.write('[' + my_Time.getNowTimestamp() + ']')
        txt.write('more than 5 continuous data grab failed,the net work may crashed!')
        txt.write('formatter terminated!')
        txt.close()
        sys.exit(0)
    return None



def shutDownTimeDetector(nowDate):
    shutDownFlag = 0
    DandT_list = nowDate.split(' ')
    T_list = DandT_list[1].split(':')
    hour = int(T_list[0])
    minute = int(T_list[1])
    if hour == 11 and minute > 35:
        shutDownFlag = 1
    if hour == 15 and minute > 5:
        shutDownFlag = 1
    if hour > 15 or hour < 8:
        shutDownFlag = 1
    return shutDownFlag












# parse the data got from interfaces(json or other format)
def parseDataFromURL(DataType,rawData):
    # DataType:
    # sina Real-time Data(sh/sz),pick up sh/sz-stock name = 1    ------------------------------------    rawData : decodedUrlData
    # sina Real-time Data(hk),pick up hk-stock name = 2    ------------------------------------------    rawData : decodedUrlData
    # sina Real-time Data(sh/sz,hk),pick up stock code,name,real-timeData = 3    --------------------    rawData : decodedUrlData
    if DataType == 1:
        # print 'SH/SZ'
        tempData = rawData.split('="')
        stockName = tempData[1].split(',')[0]
        return stockName
    if DataType == 2:
        # print 'HK'
        tempData = rawData.split('="')
        stockName = tempData[1].split(',')[1]
        return stockName
    if DataType == 3:
        tempData = rawData.split('="')
        code = tempData[0].split('_')[-1]
        name = tempData[1].split(',')[0]
        str1 = tempData[1].split(',')[1]
        parsedData = []
        parsedData.append(code)
        parsedData.append(name)
        parsedData.append(str1)
        return parsedData
    else:
        return None



# parse the report dates to yahoo finance format
def parseReportDateList(reportDateList):
    i = 0
    reportStartDateList = []
    for date in reportDateList:
        i = i + 1
        report_start_date_timestamp = my_Time.transDateToTimestamp(my_Time.parseDate(date))
        reportStartDateList.append(report_start_date_timestamp)
    return reportStartDateList



# detect the loss csv,record and delete them
def CsvFileLossDetector(stockCode_earliestDate_pairs, optionalPair, interval, detectType):
    # interval:
    # daily = 1
    # weekly = 2
    # monthly = 3

    # detectType
    # zj = 1
    # ml = 2

    CsvFileLossList = []
    CsvFileLossPair = {}
    print "Detecting the loss CSVs..."
    print "Deleting the loss CSVs..."
    if detectType == 1:
        for code in stockCode_earliestDate_pairs:
            formattedCode = optionalPair[code]
            rowCount = 0
            csvName = "/home/ntlab607/Downloads/csvFrom_yahoo_zj/" + str(formattedCode) + ".csv"

            if interval == 1:
                days = (my_Time.getNowTimestamp() - stockCode_earliestDate_pairs[code]) / 86400 - 1
                weekNum = days / 7
                hiddenWeek = days % 7
                if hiddenWeek > 0:
                    days = days - (weekNum + 1) * 2
                else:
                    days = days - weekNum * 2
                threshold = days
            elif interval == 2:
                days = (my_Time.getNowTimestamp() - stockCode_earliestDate_pairs[code]) / 86400 - 1
                threshold = days / 7
            elif interval == 3:
                year1, month1 = time.gmtime(my_Time.getNowTimestamp())[0], time.gmtime(my_Time.getNowTimestamp())[1]
                year2, month2 = time.gmtime(stockCode_earliestDate_pairs[code])[0], time.gmtime(stockCode_earliestDate_pairs[code])[1]
                threshold = (year1 - year2) * 12 + (month1 - month2) + 1
            else:
                threshold = 2
            try:
                with open(csvName) as csvFile:
                    f_csv = csv.reader(csvFile)
                    for row in f_csv:
                        rowCount = rowCount + 1
                    if rowCount <= threshold:
                        # the csv is invalid, set it into csv_download_fail table in database
                        CsvFileLossList.append(code)
                        # then delete it
                        os.remove(csvName)
                    else:
                        continue
            except Exception, e:
                print e
                continue
        return CsvFileLossList
    if detectType == 2:
        for code in stockCode_earliestDate_pairs:
            CsvFileLossList = []
            try:
                formatted_codeList = optionalPair[code]
            except Exception, e:
                print e
                continue
            for formatted_code in formatted_codeList:
                rowCount = 0
                csvName = "/home/ntlab607/Downloads/csvFrom_yahoo_ml/" + formatted_code + ".csv"
                if interval == 1:
                    days = (my_Time.getNowTimestamp() - stockCode_earliestDate_pairs[code]) / 86400 - 1
                    weekNum = days / 7
                    hiddenWeek = days % 7
                    if hiddenWeek > 0:
                        days = days - (weekNum + 1) * 2
                    else:
                        days = days - weekNum * 2
                    threshold = days
                elif interval == 2:
                    days = (my_Time.getNowTimestamp() - stockCode_earliestDate_pairs[code]) / 86400 - 1
                    threshold = days / 7
                elif interval == 3:
                    year1, month1 = time.gmtime(my_Time.getNowTimestamp())[0], time.gmtime(my_Time.getNowTimestamp())[1]
                    year2, month2 = time.gmtime(stockCode_earliestDate_pairs[code])[0], \
                                    time.gmtime(stockCode_earliestDate_pairs[code])[1]
                    threshold = (year1 - year2) * 12 + (month1 - month2) + 1
                else:
                    threshold = 2

                try:
                    with open(csvName) as csvFile:
                        f_csv = csv.reader(csvFile)
                        for row in f_csv:
                            rowCount = rowCount + 1
                        if rowCount <= threshold:
                            # the csv is invalid, set it into csv_download_fail table in database
                            CsvFileLossList.append(formatted_code)
                            # then delete it
                            os.remove(csvName)
                        else:
                            continue
                except Exception, e:
                    continue
            CsvFileLossPair[code] = CsvFileLossList
        return CsvFileLossPair
    else:
        return None


