# -*- coding: UTF-8 -*-
import MySQLdb
import csv

import my_Time
import my_Utils



# connect to database
def MysqlConnector(Database):
    # Database
    # main = 1
    if Database == 1:
        conn = MySQLdb.connect(
            host='localhost',
            user='root',
            passwd='root',
            db='stock_analyze',
            charset='utf8'
        )
        cur = conn.cursor()
        return cur,conn
    elif Database == 2:
        conn = MySQLdb.connect(
            host='localhost',
            user='root',
            passwd='root',
            db='formatted_nlp',
            charset='utf8'
        )
        cur = conn.cursor()
        return cur,conn
    else:
        return None



# close connection
def MysqlCloser(MysqlCursor,MysqlConn):
    MysqlCursor.close()
    MysqlConn.close()



def tableDropper(MysqlCursor , MysqlConn, stockList):
    for stock in stockList:
        code = stock[0]
        sql_0 = "DROP TABLE IF EXISTS stock_analyze.stock_quotations_" + str(code)
        try:
            n = MysqlCursor.execute(sql_0)
            MysqlConn.commit()
        except Exception, e:
            print e



# set stock basic data to database
def setStockBasicData(MysqlCursor , MysqlConn, stockList):
    # sql_0 = "DROP TABLE IF EXISTS stock_analyze.stock_basic_info"
    # try:
    #     n = MysqlCursor.execute(sql_0)
    #     MysqlConn.commit()
    # except Exception, e:
    #     print e
    # sql_0 = "CREATE TABLE stock_analyze.stock_basic_info" \
    #               "(id INT NOT NULL AUTO_INCREMENT," \
    #               "code VARCHAR(255) NULL," \
    #               "name VARCHAR(255) NULL," \
    #               "market VARCHAR(255) NULL," \
    #               "csv VARCHAR(255) NULL," \
    #               "PRIMARY KEY (id))"
    # try:
    #     n = MysqlCursor.execute(sql_0)
    #     MysqlConn.commit()
    # except Exception, e:
    #     print e

    setNum = 0
    for stock in stockList:
        print '---set a new stock to database---' + ' code = ' + stock[0] + ' name = ' + stock[1] + ' market = ' + stock[2]
        sql = "INSERT INTO stock_analyze.stock_basic_info(code, name, market) " \
              "VALUES ('%s', '%s', '%s')" % (stock[0], stock[1], stock[2])
        try:
            n = MysqlCursor.execute(sql)
            if n != 0:
                setNum = setNum + 1
        except Exception, e:
            print e
            continue

        # if stock[2] != None:
        #     print '---create a new table for it---'
        #     sql_1 = "CREATE TABLE stock_analyze.stock_quotations_" + str(stock[0]) + \
        #           "(id INT NOT NULL AUTO_INCREMENT," \
        #           "today_open VARCHAR(255) NULL," \
        #           "yesterday_open VARCHAR(255) NULL," \
        #           "now_price VARCHAR(255) NULL," \
        #           "today_high VARCHAR(255) NULL," \
        #           "today_low VARCHAR(255) NULL," \
        #           "buy_1 VARCHAR(255) NULL," \
        #           "sale_1 VARCHAR(255) NULL," \
        #           "volume VARCHAR(255) NULL," \
        #           "volume_price VARCHAR(255) NULL," \
        #           "buy_1_hand VARCHAR(255) NULL," \
        #           "buy_1_price VARCHAR(255) NULL," \
        #           "buy_2_hand VARCHAR(255) NULL," \
        #           "buy_2_price VARCHAR(255) NULL," \
        #           "buy_3_hand VARCHAR(255) NULL," \
        #           "buy_3_price VARCHAR(255) NULL," \
        #           "buy_4_hand VARCHAR(255) NULL," \
        #           "buy_4_price VARCHAR(255) NULL," \
        #           "buy_5_hand VARCHAR(255) NULL," \
        #           "buy_5_price VARCHAR(255) NULL," \
        #           "sale_1_hand VARCHAR(255) NULL," \
        #           "sale_1_price VARCHAR(255) NULL," \
        #           "sale_2_hand VARCHAR(255) NULL," \
        #           "sale_2_price VARCHAR(255) NULL," \
        #           "sale_3_hand VARCHAR(255) NULL," \
        #           "sale_3_price VARCHAR(255) NULL," \
        #           "sale_4_hand VARCHAR(255) NULL," \
        #           "sale_4_price VARCHAR(255) NULL," \
        #           "sale_5_hand VARCHAR(255) NULL," \
        #           "sale_5_price VARCHAR(255) NULL," \
        #           "date VARCHAR(255) NULL," \
        #           "time VARCHAR(255) NULL," \
        #           "PRIMARY KEY (id))"
        #     try:
        #         n = MysqlCursor.execute(sql_1)
        #         if n != 0:
        #             setNum = setNum + 1
        #     except Exception, e:
        #         print e
        #         continue
    try:
        MysqlConn.commit()
    except Exception, e:
        print e
    return setNum



# get stock basic data from database
def getStockBasicData(MysqlCursor , MysqlConn, start_id, end_id):
    stockList = []
    sql = "SELECT code, name, market FROM stock_analyze.stock_basic_info WHERE id >= '%s' AND id <= '%s'" % (start_id, end_id)
    try:
        MysqlCursor.execute(sql)
        result = MysqlCursor.fetchall()
        for row in result:
            stockList.append((row[0], row[1], row[2]))
    except Exception, e:
        print e
    return stockList



def setStockMinData(MysqlCursor, MysqlConn, stockMinDataPair):
    setNum = 0
    print '---updating stock one min quotation to database---'
    for code in stockMinDataPair:
        oneStockQuotationList = stockMinDataPair[code]
        table_name = 'stock_quotations_' + str(code)
        try:
            sql = "INSERT INTO stock_analyze." + table_name + \
                  "(today_open, yesterday_open, now_price, today_high, " \
                  "today_low, buy_1, sale_1, volume, volume_price, buy_1_hand, " \
                  "buy_1_price, buy_2_hand, buy_2_price, buy_3_hand, buy_3_price, " \
                  "buy_4_hand, buy_4_price, buy_5_hand, buy_5_price, sale_1_hand, " \
                  "sale_1_price, sale_2_hand, sale_2_price, sale_3_hand, sale_3_price, " \
                  "sale_4_hand, sale_4_price, sale_5_hand, sale_5_price, date, time) VALUES " \
                  "('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', " \
                  "'%s', '%s', '%s','%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', " \
                  "'%s', '%s', '%s', '%s', '%s', '%s', '%s')" % \
                  (oneStockQuotationList[1], oneStockQuotationList[2], oneStockQuotationList[3],
                   oneStockQuotationList[4], oneStockQuotationList[5], oneStockQuotationList[6],
                   oneStockQuotationList[7],oneStockQuotationList[8], oneStockQuotationList[9],
                   oneStockQuotationList[10], oneStockQuotationList[11], oneStockQuotationList[12],
                   oneStockQuotationList[13], oneStockQuotationList[14], oneStockQuotationList[15],
                   oneStockQuotationList[16], oneStockQuotationList[17], oneStockQuotationList[18],
                   oneStockQuotationList[19], oneStockQuotationList[20], oneStockQuotationList[21],
                   oneStockQuotationList[22], oneStockQuotationList[23], oneStockQuotationList[24],
                   oneStockQuotationList[25], oneStockQuotationList[26], oneStockQuotationList[27],
                   oneStockQuotationList[28], oneStockQuotationList[29], oneStockQuotationList[30],
                   oneStockQuotationList[31])

            n = MysqlCursor.execute(sql)
            if n != 0:
                setNum = setNum + 1
        except Exception, e:
            print e
            continue
    try:
        MysqlConn.commit()
        print '---updated ' + str(setNum) + ' stock one min quotation to database---'
    except Exception, e:
        print e
    return setNum



def setCsvTag(MysqlCursor, MysqlConn, CsvFileLossList):
    print 'update csv situation to database...'

    sql_0 = "UPDATE stock_analyze.stock_basic_info SET csv = '%s'" % ('valid')
    try:
        n = MysqlCursor.execute(sql_0)
    except Exception, e:
        print e
    MysqlConn.commit()

    setNum = 0
    for code in CsvFileLossList:
        sql = "UPDATE stock_analyze.stock_basic_info SET csv = '%s' WHERE code = '%s'" % ('invalid', code)
        try:
            n = MysqlCursor.execute(sql)
            if n > 0:
                setNum = setNum + 1
        except Exception, e:
            print e
            continue
    MysqlConn.commit()

    return setNum

























# read csv data
def csvSaver(MysqlCursor, MysqlConn, csvType, optionalPairs):
    # csvType:
    # csv from yahoo finance = 1

    if csvType == 1:
        dirBase = "/home/ntlab607/Downloads/csvFrom_yahoo_zj/"
        for code in optionalPairs:
            formattedCode = optionalPairs[code]
            market = None
            tempList = []
            csvDir = dirBase + formattedCode + '.csv'
            sql_0 = "SELECT market FROM formatted_nlp.zj_info_stock WHERE code = '%s'" % (code)
            try:
                MysqlCursor.execute(sql_0)
                result = MysqlCursor.fetchall()
                for row in result:
                    market = row[0]
            except Exception, e:
                print e
            try:
                myCsvReader = csv.reader(open(csvDir))
                for row in myCsvReader:
                    tempList.append(row)
                print tempList
                flag = 0
                for line in tempList:
                    if flag == 1:
                        sql = "INSERT INTO formatted_nlp.zj_info_stock_actual(code, market, time, open_price, max_price, min_price, close_price, volume) VALUES('%s','%s','%s','%s','%s','%s','%s','%s')" % (code, market, line[0], line[1], line[2], line[3], line[4], line[6])
                        try:
                            n = MysqlCursor.execute(sql)
                            MysqlConn.commit()
                        except Exception, e:
                            print e
                            continue
                    else:
                        flag = 1
            except Exception, e:
                print e
                continue
        return None
    elif csvType == 2:
        dirBase = "/home/ntlab607/Downloads/csvFrom_yahoo_ml/"
        for code in optionalPairs:
            # formattedCode = my_Utils.stockCodeFormatOne(3,code)
            formattedCodeList = optionalPairs[code]
            for formattedCode in formattedCodeList:
                tempList = []
                csvDir = dirBase + formattedCode + '.csv'
                try:
                    myCsvReader = csv.reader(open(csvDir))
                    for row in myCsvReader:
                        tempList.append(row)
                    print tempList
                    flag = 0
                    for line in tempList:
                        if flag == 1:
                            sql = "INSERT INTO share_nlp.ml_info_stock_actual(time,stock_code,close_price,max_price,min_price,open_price,volume) VALUES('%s','%s','%s','%s','%s','%s','%s')" % (line[0], formattedCode, line[4], line[2], line[3], line[1], line[6])
                            try:
                                n = MysqlCursor.execute(sql)
                                MysqlConn.commit()
                            except Exception, e:
                                print e
                                continue
                        else:
                            flag = 1
                except Exception, e:
                    print e
                    continue
        return None
    else:
        return None