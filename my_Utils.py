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



def getBasicStockData(type):
    # get all stock basic data from html file
    stockList = []
    if type == 1:
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
    elif type == 2:
        if os.path.exists('./files/name_code_list_hk.html') and os.path.getsize('./files/name_code_list_hk.html'):
            print 'grab stock basic info from local data...'
            soup = BeautifulSoup(open('./files/name_code_list_hk.html'), 'html.parser')
            metaHtml = soup.find_all('a', target = '_blank', href = re.compile('http://quote.eastmoney.com/hk'))
            stockList = []
            for data in metaHtml:
                if '<strong>' not in str(data.contents[0]):
                    rawStr = data.contents[0]
                    tempList = str(rawStr).split(')')
                    stockName = tempList[1]
                    stockCode = tempList[0].split('(')[1]
                    stockMarket = 'HK'
                    stockList.append((stockCode, stockName, stockMarket))
    else:
        stockList = None
    return stockList



def stockMarketFormatter(type, **kwargs):
    # type:
    # use code(only number) to judge market = 1
    # add market to code = 2 (sina format)
    # add market to code = 3 (wangyi format)
    # separate market and code from code = 4

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
        formattedCode = None
        if code != None and market != None:
            if market == 'SH':
                formattedCode = '0' + str(code)
            elif market == 'SZ':
                formattedCode = '1' + str(code)
            elif market == 'SH/SZ':
                # more task on it
                formattedCode = '2' + str(code)
            elif market == 'NA':
                formattedCode = '2' + str(code)
            else:
                pass
        else:
            pass
        return formattedCode
    elif type == 4:
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
    if hour == 11 and minute > 40:
        shutDownFlag = 1
    if hour == 15 and minute > 10:
        shutDownFlag = 1
    if hour > 15 or hour < 8:
        shutDownFlag = 1
    return shutDownFlag



# detect the loss csv,record and delete them
def CsvFileLossDetector(formatted_codeList, days):
    CsvFileLossList = []
    print "Detecting and deleting the loss CSVs..."
    for code in formatted_codeList:
        if str(code[0]) != '2':
            rowCount = 0
            csvName = "./csvs/" + str(code)[1:] + ".csv"

            weekNum = days / 7
            hiddenDays = days % 7
            if hiddenDays > 2:
                threshold = (hiddenDays - 2) + weekNum * 5 + 1
            else:
                threshold = weekNum * 5 + 1

            # all stock stop about 30 days every year
            # and some stock has been delisted(or maybe the stock just be listed lately),so line will be very few
            threshold = threshold - days / 10 - days / 5

            try:
                with open(csvName) as csvFile:
                    f_csv = csv.reader(csvFile)
                    for row in f_csv:
                        rowCount = rowCount + 1
                    if rowCount < threshold:
                        # the csv is invalid, set it into csv_download_fail table in database
                        CsvFileLossList.append(code[1:])
                        # then delete it
                        print 'csv '+ csvName +' data is wrong ,deleted'
                        os.remove(csvName)
                    else:
                        continue
            except Exception, e:
                print e
                continue
        else:
            CsvFileLossList.append(code[1:])
    return CsvFileLossList



def CsvFileNotDownloadedDetector(formatted_codeList):
    notDownloaded_codeList = []
    for code in formatted_codeList:
        if str(code)[0] != '2':
            csvName = "./csvs/" + str(code)[1:] + ".csv"
            if os.path.exists(csvName):
                continue
            else:
                notDownloaded_codeList.append(code)
    return notDownloaded_codeList




