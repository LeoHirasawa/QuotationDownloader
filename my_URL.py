# -*- coding: UTF-8 -*-
import urllib2
import requests
import time
import yaml
import re
import datetime
import os
import io
import exceptions
import csv

import global_list
import my_Utils
import my_Time



def getMinDataFromURL(type, **kwargs):
    # type:
    # get sina Real-time Data(sh/sz/sb/hk) = 1    --------------------    metaData : stockCodeList

    stockList = None
    for key in kwargs:
        if key == 'stockList':
            stockList = kwargs[key]

    if type == 1:
        stockMinDataPair = {}
        if stockList != None:
            codeQueue = ''
            for stock in stockList:
                formattedCode = my_Utils.stockMarketFormatter(2, code = stock[0], market = stock[2])
                codeQueue = codeQueue + str(formattedCode) + ','
            codeQueue = codeQueue[:-1]
            urlbase = "http://hq.sinajs.cn/list="
            url = urlbase + codeQueue
            urlData = None
            req_header = {
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.101 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.8',
                'Accept-Encoding': 'gzip, deflate',
            }

            tryTime = 1
            while tryTime < 5 and tryTime > 0:
                print 'Grabbing stock quotation data from sina...'
                try:
                    urlData = requests.get(url, headers=req_header, timeout=5)
                    tryTime = -1
                except Exception, e:
                    print e
                    tryTime = tryTime + 1
            if tryTime < 0:
                urlData.encoding = 'gbk'
                decodedUrlData = urlData.text
                global_list.GLOBAL_REQUEST_ERROR_COUNT = 0
            else:
                decodedUrlData = None
                global_list.GLOBAL_REQUEST_ERROR_COUNT = global_list.GLOBAL_REQUEST_ERROR_COUNT + 1
                my_Utils.crashDetector()

            if decodedUrlData != None:
                stockQuotationList = str(decodedUrlData).replace('\n','').replace('"','').split(';')
                for stockQuotation in stockQuotationList:
                    stockParts = stockQuotation.split('=')
                    if len(stockParts) > 1:
                        code = filter(my_Utils.isNum, stockParts[0])
                        oneStockQuotationList = stockParts[1].split(',')
                        if len(oneStockQuotationList) > 0 and oneStockQuotationList[0] != '':
                            stockMinDataPair[code] = oneStockQuotationList
                    else:
                        pass
            else:
                pass
        else:
            pass
        return stockMinDataPair
    else:
        pass
    return None
















# Download historical data csv from yahoo
def DownloadCsv(csvType, stockCode_earliestDate_pairs, optionalPair, interval):
    # csvType:
    # Download historical data csv(sh/sz) from wangyi = 1    ---------------------------- Date format:20170815
    # Download historical data csv(sh/sz/hk/us) from yahoo finance = 2    ---------------------------- Date format:1503046806(timestamp)

    # interval:
    # daily = 1
    # weekly = 2
    # monthly = 3

    # endDate is optional,you can give a None for default

    print "Starting download CSVs..."
    global csv
    if csvType == 1:
        DownloadedNum = 0
        # WangyiCsvUrlBase = "http://quotes.money.163.com/service/chddata.html?code="
        # for code in formatted_codeList:
        #     addCode = WangyiCsvUrlBase + '0' + code[2:]
        #     addDate = addCode + "&start=" + "&end=" + endDate
        #     WangyiCsvUrl = addDate + "&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER;TCAP;MCAP"
        #     try:
        #         csv = urllib2.urlopen(WangyiCsvUrl).read()
        #         csvName = "/home/ntlab607/Downloads/csvFrom_Wangyi/" + code + ".csv"
        #         with open(csvName, "wb") as csvFile:
        #             csvFile.write(csv)
        #         csvFile.close()
        #         print "one CSV saved"
        #         DownloadedNum = DownloadedNum + 1
        #         time.sleep(10)
        #     except Exception, e:
        #         print e
        return DownloadedNum
    elif csvType == 2:
        DownloadedNum = 0
        FailNum = 0
        failCodeList = []
        yahooUrlBase_part1 = "https://query1.finance.yahoo.com/v7/finance/download/"
        yahooUrlBase_part2 = "&events=history&crumb="
        crumb = loadToken()['crumb']

        req_header = {
            'User-Agent' : 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.101 Safari/537.36',
            'Accept':'*/*',
            'Accept-Language':'en-US,en;q=0.8',
            # 'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
            'Accept-Encoding':'gzip, deflate, br',
            # 'Host': 'streamerapi.finance.yahoo.com',
            # 'Connection':'close',
            # 'Origin':'https://finance.yahoo.com',
            # 'Referer':'https://finance.yahoo.com/__streamer-worker-0.3.62.js'  # if still fail ,add host and other key-values
        }

        if interval == 1:
            gap = "1d"
        elif interval == 2:
            gap = "1wk"
        else:
            gap = "1mo"

        for code in stockCode_earliestDate_pairs:
            timestamp = stockCode_earliestDate_pairs[code]
            formatted_code = optionalPair[code]
            csvName = "/home/ntlab607/Downloads/csvFrom_yahoo_zj/" + formatted_code + ".csv"
            if os.path.exists(csvName):
                print "CSV file " + csvName + " exist...continue"
                DownloadedNum = DownloadedNum + 1
                continue
            else:
                if timestamp != None:
                    startDate = str(timestamp)
                    endDate = str(my_Time.getNowTimestamp())
                    yahooUrl = yahooUrlBase_part1 + formatted_code + "?period1=" + startDate + "&period2=" + endDate + "&interval=" + gap + yahooUrlBase_part2 + crumb
                    print yahooUrl
                    time.sleep(2)
                    try:
                        data = requests.get(yahooUrl, cookies = {'B': loadToken()['cookie']}, timeout = 5)
                    except Exception, e:
                        print e
                        FailNum = FailNum + 1
                        failCodeList.append(code)
                        continue
                    csvContent = data.text
                    with open(csvName, "wb") as csvFile:
                        csvFile.write(csvContent)
                    csvFile.close()
                    print "one yahoo_CSV saved"
                    DownloadedNum = DownloadedNum + 1
                else:
                    FailNum = FailNum + 1
                    continue
        return DownloadedNum, FailNum, failCodeList
    elif csvType == 3:
        DownloadedNum = 0
        FailNum = 0
        oneStockSeveralTryFlag = 0
        failCodePair = {}
        yahooUrlBase_part1 = "https://query1.finance.yahoo.com/v7/finance/download/"
        yahooUrlBase_part2 = "&events=history&crumb="
        crumb = loadToken()['crumb']

        req_header = {
            'User-Agent' : 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.101 Safari/537.36',
            'Accept':'*/*',
            'Accept-Language':'en-US,en;q=0.8',
            # 'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
            'Accept-Encoding':'gzip, deflate, br',
            # 'Host': 'streamerapi.finance.yahoo.com',
            # 'Connection':'close',
            # 'Origin':'https://finance.yahoo.com',
            # 'Referer':'https://finance.yahoo.com/__streamer-worker-0.3.62.js'  # if still fail ,add host and other key-values
        }

        if interval == 1:
            gap = "1d"
        elif interval == 2:
            gap = "1wk"
        else:
            gap = "1mo"

        for code in stockCode_earliestDate_pairs:
            failCodeList = []
            errorFlag = 0
            try:
                # when iter,the code will not exist in the optionalPair
                formatted_codeList = optionalPair[code]
            except Exception, e:
                print e
                continue
            timestamp = stockCode_earliestDate_pairs[code]
            maxNum = len(formatted_codeList)
            validFileNum = 0
            for formatted_code in formatted_codeList:
                csvName = "/home/ntlab607/Downloads/csvFrom_yahoo_ml/" + formatted_code + ".csv"
                if os.path.exists(csvName):
                    # have issues here..if all files exist, how to add DownloadedNum?
                    print "CSV file " + csvName + " exist...continue"
                    validFileNum = validFileNum + 1
                    if validFileNum >= maxNum:
                        DownloadedNum = DownloadedNum + 1
                    continue
                else:
                    if timestamp != None:
                        startDate = str(timestamp)
                        endDate = str(my_Time.getNowTimestamp())
                        yahooUrl = yahooUrlBase_part1 + formatted_code + "?period1=" + startDate + "&period2=" + endDate + "&interval=" + gap + yahooUrlBase_part2 + crumb
                        print yahooUrl
                        time.sleep(2)
                        try:
                            data = requests.get(yahooUrl, cookies = {'B': loadToken()['cookie']}, timeout = 5)
                        except Exception, e:
                            print e
                            # if fail once, it will be added to failList
                            FailNum = FailNum + 1
                            # bloombergCode
                            failCodeList.append(code)
                            break
                        if errorFlag == 0:
                            csvContent = data.text
                            with open(csvName, "wb") as csvFile:
                                csvFile.write(csvContent)
                            csvFile.close()
                            print "one yahoo_CSV saved"
                    else:
                        FailNum = FailNum + 1
                        failCodeList.append(code)
                        break
                validFileNum = validFileNum + 1
                if validFileNum >= maxNum:
                    DownloadedNum = DownloadedNum + 1

            if len(failCodeList) > 0:
                failCodePair[code] = failCodeList
        return DownloadedNum, FailNum, failCodePair
    else:
        return None



# get cookie and crumb from APPL page or disk
def loadToken():
    print "Loading token..."
    # force = overwrite disk data
    refreshDays = 30  # refresh cookie every 'x' days
    dateTimeFormat = "%Y%m%d %H:%M:%S"

    # set destinatioin file
    dataDir = os.path.expanduser('~') + '/twpData'
    dataFile = os.path.join(dataDir, 'yahoo_cookie.yml')

    try:
        # load file from disk
        data = yaml.load(open(dataFile, 'r'))
        age = (datetime.datetime.now() - datetime.datetime.strptime(data['timestamp'], dateTimeFormat)).days
        assert age < refreshDays, 'cookie too old'
    except Exception, e:
        # file not found
        if not os.path.exists(dataDir):
            os.mkdir(dataDir)
        data = getToken(dataFile) # try to get token online
    return data



# get cookie and crumb from yahoo
def getToken(fName = None):
    crumb = None
    dateTimeFormat = "%Y%m%d %H:%M:%S"
    url = 'https://uk.finance.yahoo.com/quote/AAPL/history' # url for a ticker symbol, with a download link
    r = requests.get(url) # download page
    txt = r.text  # extract html
    cookie = r.cookies['B']  # the cooke we're looking for is named 'B'
    pattern = re.compile('.*"CrumbStore":\{"crumb":"(?P<crumb>[^"]+)"\}')
    for line in txt.splitlines():
        m = pattern.match(line)
        if m is not None:
            crumb = m.groupdict()['crumb']

    assert r.status_code == 200  # check for successful download
    # save to disk
    data = {'crumb': crumb, 'cookie': cookie, 'timestamp': datetime.datetime.now().strftime(dateTimeFormat)}
    if fName is not None:  # save to file
        with open(fName, 'w') as fid:
            yaml.dump(data, fid)
    return data







# new functions
def getHtmlData(rawData, Type):
    if Type == 1:
        url = "https://finance.yahoo.com/quote/" + rawData
        req_header = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.101 Safari/537.36',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
        }
        data = None
        tryTime = 1
        while tryTime < 5 and tryTime > 0:
            try:
                print 'grab data from yahoo...'
                data = requests.get(url, headers = req_header, timeout = 5)
                tryTime = -1
            except Exception, e:
                print e
                tryTime = tryTime + 1
        if tryTime < 0:
            txtContent = data.text
        else:
            txtContent = None
        return txtContent
    if Type == 2:
        tempList = str(rawData).split('-')
        if len(tempList) == 2:
            name = tempList[0]
            market = tempList[1][0:1]# -H, -A, -US, -B
        else:
            return None, None, None
        if market == 'H':
            url = "http://quote.eastmoney.com/hk/HStock_list.html"
            formatted_name = name
            market = 'HK'
        elif market == 'US':
            url = "http://quote.eastmoney.com/usstocklist.html"
            return None, None, None
        elif market == 'B':
            url = "http://quote.eastmoney.com/stocklist.html"
            formatted_name = name + 'B'
            market = 'CH'
        elif market == 'A':
            url = "http://quote.eastmoney.com/stocklist.html"
            formatted_name = name
            market = 'CH'
        else:
            return None, None, None

        # check if data exist in local
        if market == 'HK':
            if os.path.exists('./name_code_list_hk.txt') and os.path.getsize('./name_code_list_hk.txt'):
                print 'grab code from local data...'
                name_code_list_hk = open('./name_code_list_hk.txt', 'r')
                txtContent = name_code_list_hk.read()
                name_code_list_hk.close()
                return txtContent, formatted_name, market
        if market == 'CH':
            if os.path.exists('./name_code_list_ch.txt') and os.path.getsize('./name_code_list_ch.txt'):
                print 'grab code from local data...'
                name_code_list_ch = open('./name_code_list_ch.txt', 'r')
                txtContent = name_code_list_ch.read()
                name_code_list_ch.close()
                return txtContent, formatted_name, market

        data = None
        tryTime = 1
        while tryTime < 5 and tryTime > 0:
            try:
                print 'grab data from eastmoney...'
                data = requests.get(url, timeout=5)
                tryTime = -1
            except Exception, e:
                print e
                tryTime = tryTime + 1
        if tryTime < 0:
            data.encoding = 'gbk'
            txtContent = data.text

            fileName = None
            if market == 'HK':
                if os.path.exists('./name_code_list_hk.txt') and os.path.getsize('./name_code_list_hk.txt'):
                    print 'code_name data file exist.'
                else:
                    fileName = "./name_code_list_hk.txt"
                txt = open(fileName, 'w')
                txt.write(txtContent)
                txt.close()
            if market == 'CH':
                if os.path.exists('./name_code_list_ch.txt') and os.path.getsize('./name_code_list_ch.txt'):
                    print 'code_name data file exist.'
                else:
                    fileName = "./name_code_list_ch.txt"
                txt = open(fileName, 'w')
                txt.write(txtContent)
                txt.close()

        else:
            txtContent = None
        return txtContent, formatted_name, market



def get_close_price_data(formatted_code, start_timestamp, end_timestamp):
    yahooUrlBase_part1 = "https://finance.yahoo.com/quote/"
    yahooUrlBase_part2 = "&interval=1d&filter=history&frequency=1d"
    content = None
    # "https://finance.yahoo.com/quote/002508.SZ/history?period1=1502726400&period2=1504368000&interval=1d&filter=history&frequency=1d"

    req_header = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.101 Safari/537.36',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.8',
        # 'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
        'Accept-Encoding': 'gzip, deflate, br',
        # 'Host': 'streamerapi.finance.yahoo.com',
        # 'Connection':'close',
        # 'Origin':'https://finance.yahoo.com',
        # 'Referer':'https://finance.yahoo.com/__streamer-worker-0.3.62.js'  # if still fail ,add host and other key-values
    }

    yahooUrl = yahooUrlBase_part1 + str(formatted_code) + "/history?period1=" + str(start_timestamp) + "&period2=" + str(end_timestamp) + yahooUrlBase_part2
    print yahooUrl

    tryTime = 1
    while tryTime < 5 and tryTime > 0:
        try:
            print 'grab close_price data from yahoo...'
            content = requests.get(yahooUrl, cookies={'B': loadToken()['cookie']}, timeout=5)
            tryTime = -1
        except Exception, e:
            print e
            tryTime = tryTime + 1
    if tryTime < 0:
        return content.text
    else:
        return None















# Backup:
#
#
#
#
#
#
#
#
#
# # use urllib2 to get data from interface(json or other format,don't use now)
# def registerUrl(stockCodeList):
#     try:
#         urlbase = "http://hq.sinajs.cn/list="
#         for stockCode in stockCodeList:
#             url = urlbase + stockCode
#             # print url
#             urlData = urllib2.urlopen(url).read()
#             urlData = urlData.decode('GBK')
#             # print urlData
#             parsedData = praserSinaStockData(urlData)#return as a list---->[code,name,str1]
#             stockDataDict[parsedData[0]] = parsedData#push into dict---->key:code value:list
#             # stocksData.append(parsedData)
#         print "url over"
#         return stockDataDict
#     except Exception, e:
#         print e
#
#
#
# # parse JSON data(don't use now)
# def praseJsonFile(jsonData):
#     value = json.loads(jsonData)
#     rootlist = value.keys()
#     print rootlist
#     print duan
#     for rootkey in rootlist:
#         print rootkey
#     print duan
#     subvalue = value[rootkey]
#     print subvalue
#     print duan
#     for subkey in subvalue:
#         print subkey, subvalue[subkey]
#
#
#
# # parser the data from sina finance(don't use now)
# def praserSinaStockData(sinaStockData):
#     tempData = sinaStockData.split('="')
#     # print tempData
#     code = tempData[0].split('_')[-1]
#     name = tempData[1].split(',')[0]
#     str1 = tempData[1].split(',')[1]
#     # print code,name,str1
#     parsedData = []
#     parsedData.append(code)
#     parsedData.append(name)
#     parsedData.append(str1)
#     # print parsedData
#     return parsedData
#
#
#
# # write sina data to file(daliy data,don't use now)
# def writesinaFile(fileData,stockCodeList):
#     csvFile = file("/home/ntlab607/Documents/stock.csv", "ab+")
#     writer = csv.writer(csvFile)
#     for code in stockCodeList:
#         tempList = fileData.get(code)
#         writer.writerow([tempList[0],tempList[1],tempList[2]])
#     csvFile.close()