# -*- coding: UTF-8 -*-
import time
import datetime


# trans YMDT to Timestamp
def transDateToTimestamp(parsedDate):
    t = datetime.datetime(int(parsedDate[0]),int(parsedDate[1]),int(parsedDate[2]),int(parsedDate[3]))
    timestamp = long(time.mktime(t.timetuple()))
    return timestamp



# trans timestamp to date
def transTimestampToDate(timestamp):
    x = time.localtime(timestamp)
    date = time.strftime('%Y-%m-%d %H:%M:%S', x)
    return date



# get now timestamp
def getNowTimestamp():
    timestamp = long(time.time())
    return timestamp



# trans timestamp to datetime format and get delta timestamp
def getGapTimestamp(timestamp, gap):

    date = datetime.datetime.fromtimestamp(timestamp)
    start_date = date + datetime.timedelta(days = -gap)
    start_timestamp = int(time.mktime(start_date.timetuple()))
    return start_timestamp



# parse Date Data from DataBase to Y/M/D/T
def parseDate(DateStr):
    newDateStr = str(DateStr).replace(' ', '')
    judgeChar = (newDateStr[0:1])
    if str.isdigit(str(judgeChar)):
        # Chinese format Date
        tempLanguage = str(newDateStr).split("年")
        tempY = tempLanguage[0]
        newTempLanguage = tempLanguage[1].split("月")
        tempM = newTempLanguage[0]
        tempD = newTempLanguage[1].split("日")[0]
        Year, Month, Day = tempY, tempM, tempD
        if int(Month) < 10:
            Month = '0' + Month
        if int(Day) < 10:
            Day = '0' + Day
        # print Year + '\n' + Month + '\n' + Day + '\n'
    else:
        # English format Date
        tempDateStr = list(newDateStr)
        listM = []
        listDandY = []
        flag = 0
        for char in tempDateStr:
            if str.isdigit(str(char)):
                flag = 1
            if flag == 0:
                listM.append(char)
            else:
                listDandY.append(char)
        strM = ''.join(listM)
        strDandY = ''.join(listDandY)




        if strM == 'January' or strM == 'JANUARY':
            tempM = '01'
        if strM == 'February' or strM == 'FEBRUARY':
            tempM = '02'
        if strM == 'March' or strM == 'MARCH':
            tempM = '03'
        if strM == 'April' or strM == 'APRIL':
            tempM = '04'
        if strM == 'May' or strM == 'MAY':
            tempM = '05'
        if strM == 'June' or strM == 'JUNE':
            tempM = '06'
        if strM == 'July' or strM == 'JULY':
            tempM = '07'
        if strM == 'August' or strM == 'AUGUST':
            tempM = '08'
        if strM == 'September' or strM == 'SEPTEMBER':
            tempM = '09'
        if strM == 'October' or strM == 'OCTOBER':
            tempM = '10'
        if strM == 'November' or strM == 'NOVEMBER':
            tempM = '11'
        if strM == 'December' or strM == 'DECEMBER':
            tempM = '12'

        tempDandY = strDandY.split(',')
        tempD = tempDandY[0]
        tempY = tempDandY[1]
        Year, Month, Day = tempY, tempM, tempD
        if int(Day) < 10:
            Day = '0' + Day
        # print Year + '\n' + Month + '\n' + Day + '\n'

    Time = '23'# 23:00pm
    parsedDate = []
    parsedDate.append(Year)
    parsedDate.append(Month)
    parsedDate.append(Day)
    parsedDate.append(Time)
    return parsedDate