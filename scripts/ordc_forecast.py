import csv
import os
import scipy.stats
from time import strftime
from datetime import datetime, timezone
import argparse 

import pandas as pd
import urllib
import sqlalchemy

global VOLL
global MINCONTLEVEL
global RTORPA_MU_COEF
global RTORPA_SIGMA_COEF
global RTOFFPA_RATIO

#will be reset at beginning of 2022
#Updated on 1/5/202 ERCOT WMS meeting
#VOLL is changed from $9000 to $5000
#MINCONTLEVEL is changed from 2000mw to 3000mw
VOLL = 5000
MINCONTLEVEL = 3000
RTORPA_MU_COEF = 0.5
RTORPA_SIGMA_COEF = 0.707
RTOFFPA_RATIO = 0.5

#def main():
def ordc_forecast(markettype):
    global ORDCTYPE
    
    # Initialize parser 
    '''parser = argparse.ArgumentParser() 
  
    # Adding optional argument 
    parser.add_argument("-m", "--Market", help = "DA or HA market type") 
  
    # Read arguments from command line 
    args = parser.parse_args() 
    
    ORDCTYPE = 'HA'  # default value
    if args.Market: 
        ORDCTYPE = args.Market''' 
    
    ORDCTYPE = markettype

    global rtoffpaDict
    global rtorpaDict
    rtoffpaDict = {}
    rtorpaDict = {}

    global ordcForecastBias

    ordcForecastBias = calculate_ordc_reserve_capacity_bias()
    
    get_reserve_capacity_forecast()

    lolp_distribution_input()

    for t in range(0, nPeriod):
        #default lolp parameters
        mu = 884.42
        sigma = 1208.18

        lmp = 0
        datetimeStr = periodList[t]
        (mu, sigma) = lolp_distribution_params(datetimeStr)

        rtolcap = rtolcapDict[datetimeStr]
        rtoffcap = rtoffcapDict[datetimeStr]

        (rtoffpaDict[datetimeStr], rtorpaDict[datetimeStr]) = rt_price_adder(rtolcap, rtoffcap, lmp, mu, sigma)

    #print(rtoffpaDict, rtorpaDict)
    ordc_price_adder_output()


def season_month_map(monthVal):
    if monthVal in [12, 1, 2]:
        return 'WINTER'
    elif monthVal in [3, 4, 5]:
        return 'SPRING'
    elif monthVal in [6, 7, 8]:
        return 'SUMMER'
    elif monthVal in [9, 10, 11]:
        return 'FALL'
    else:
        print('Invalid month number!')
        return ''


def lolp_distribution_params(datetimeStr):
    datetimeList = datetimeStr.split(' ')
    dateList = datetimeList[0].split('/')
    timeList = datetimeList[1].split(':')
    
    dateStrOutput= "%02d/%02d" % (int(dateList[0]), int(dateList[1]))
    timeStrOutput = int(timeList[0]) + 1

    seasonStr = season_month_map(int(dateList[0]))

    for n in range(0, nLOLPDist):
        #if dateStrOutput >= startdateDict[n] and dateStrOutput <= enddateDict[n]:
        if seasonStr == seasonDict[n]:
            if timeStrOutput >= starthourDict[n] and timeStrOutput <= endhourDict[n]:
                mu = meanDict[n]
                sigma = stdDict[n]
                break    

    return (mu, sigma)
# end of lolp_distribution_params()
 

def ordc_price_adder_output():
    '''output ORDC price adder to csv file'''
    
    datetimeFormat = "%m/%d/%Y %H:%M"
    curUTCTime = datetime.utcnow()
    curLocalTime = curUTCTime.replace(tzinfo=timezone.utc).astimezone(tz=None)

    allRows = []
    for t in range(0, nPeriod):
        datetimeStr = periodList[t]
        outRow = {}
        outRow['DateTime'] = periodList[t][:-2]
        outRow['RTORPA'] = rtorpaDict[datetimeStr]
        outRow['RTOFFPA'] = rtoffpaDict[datetimeStr]
        outRow['DSTFlag'] = periodList[t][-1:]
        outRow['Load'] = loadDict[datetimeStr]
        outRow['Wind'] = windgenDict[datetimeStr]
        outRow['Solar'] = solargenDict[datetimeStr]
        outRow['NetLoad'] = netloadDict[datetimeStr]
        outRow['ThermalCapacity'] = thermalcapDict[datetimeStr]
        outRow['TieFlow'] = tieflowDict[datetimeStr]
        outRow['Outages'] = outageDict[datetimeStr]
        outRow['LMP'] = 0.0
        outRow['ForecastRunDateTime'] = curLocalTime.strftime(datetimeFormat)
        outRow['ForecastBias'] = ordcForecastBias
        outRow['ForecastType'] = ORDCTYPE

        allRows.append(outRow)


    fileStr = os.path.abspath(os.getcwd())
    fileStr = os.path.join(fileStr, "ordc_price_adder.csv")
    cnt = 1
    while is_open(fileStr):
        print('%s has been opened already!' % fileStr)
        fileStr = os.path.abspath(os.getcwd())
        fileName = 'ordc_price_adder_' + str(cnt) + '.csv'
        fileStr = os.path.join(fileStr, fileName)
        cnt += 1
        print('Write ORDC price adder to new output file: %s' % fileStr)
        #exit()

    with open(fileStr, "w", newline='') as outFile:
        fieldnames = ['DateTime', 'RTORPA', 'RTOFFPA', 'DSTFlag', 'Load', 'Wind', 'Solar', 'NetLoad',  \
                      'ThermalCapacity', 'TieFlow', 'Outages', 'LMP', 'ForecastRunDateTime', 'ForecastBias', 'ForecastType']
        filewriter = csv.DictWriter(outFile, fieldnames=fieldnames)
        filewriter.writeheader()
        filewriter.writerows(allRows)  #prepare all rows first, then write to file once
    
    outFile.close()

    output_csv_to_db(fileStr) # write csv output file to SQL DB


    fileStr = os.path.abspath(os.getcwd())
    fileStr = os.path.join(fileStr, "ordc_price_adder_all.csv")
    cnt = 1
    while is_open(fileStr):
        print('%s has been opened already!' % fileStr)
        fileStr = os.path.abspath(os.getcwd())
        fileName = 'ordc_price_adder_all_' + str(cnt) + '.csv'
        fileStr = os.path.join(fileStr, fileName)
        cnt += 1
        print('Write ORDC price adder to new output file: %s' % fileStr)
        #exit()
    
    with open(fileStr, "a", newline='') as outFile:
        fieldnames = ['DateTime', 'RTORPA', 'RTOFFPA', 'DSTFlag', 'Load', 'Wind', 'Solar', 'NetLoad',  \
                      'ThermalCapacity', 'TieFlow', 'Outages', 'LMP', 'ForecastRunDateTime', 'ForecastBias', 'ForecastType']
        filewriter = csv.DictWriter(outFile, fieldnames=fieldnames)
        if outFile.tell() == 0:
            filewriter.writeheader()
        filewriter.writerows(allRows)  #acumulated ORDC forecast results

    outFile.close()
# end of ordc_price_adder_output()

def calculate_ordc_reserve_capacity_bias():
    '''read ORDC forecast initial condition to calculate ORDC reserve capacity forecast bias,
    which will be applied to the ORDC price adder calculation later'''

    fileStr = os.path.abspath(os.getcwd())
    fileStr = os.path.join(fileStr, "zip_dir", "ordc_forecast_initial_condition.csv")
    if not os.path.exists(fileStr):
        print("ordc_forecast_initial_condition.csv file does not exist, please double check!")
        exit()

    inFile = open(fileStr, 'r', newline='')
    filereader = csv.DictReader(inFile)

    for row in filereader:
        netload = float(row['Load']) - float(row['Wind']) - float(row['Solar'])
        # row['TieFlow']: + means export(load), - means import(gen)
        rttotcap = float(row['ThermalCapacity']) + float(row['PUNs']) + (-1.0 * float(row['TieFlow'])) 
        rttotcap -= netload
        rttotcap -= (float(row['Outages'])) 

        forecastBias = rttotcap - float(row['ERCOTORDCCapacity'])

    return (round(forecastBias, 2))
#end of calculate_ordc_reserve_capacity_bias()


def get_reserve_capacity_forecast():
    '''read system real time forecast and calcualte system reserve capacity for ORDC calcualtion'''

    fileStr = os.path.abspath(os.getcwd())
    fileStr = os.path.join(fileStr, "zip_dir", "reserve_capacity_forecast_input.csv")
    if not os.path.exists(fileStr):
        print("reserve_capacity_forecast_input.csv file does not exist, please double check!")
        exit()

    inFile = open(fileStr, 'r', newline='')
    filereader = csv.DictReader(inFile)
    global rtolcapDict
    global rtoffcapDict
    global rttotcapDict

    global netloadDict
    global loadDict
    global windgenDict
    global solargenDict
    global thermalcapDict
    global tieflowDict
    global outageDict

    global periodList
    global nPeriod

    rtolcapDict = {}
    rtoffcapDict = {}
    rttotcapDict = {}

    netloadDict = {}
    loadDict = {}
    windgenDict = {}
    solargenDict = {}
    thermalcapDict = {}
    tieflowDict = {}
    outageDict = {}

    periodList = []
    
  #use DateTime + DSTFlag as key
    for row in filereader:
        datetimeStr = date_format_change(row['DateTime'])
        datetimeStr += ' ' + row['DSTFlag']
 
        #save input data bleow for output later
        loadDict[datetimeStr] = round(float(row['Load']), 2)
        windgenDict[datetimeStr] = round(float(row['Wind']), 2)
        solargenDict[datetimeStr] = round(float(row['Solar']), 2)
        thermalcapDict[datetimeStr] = round(float(row['ThermalCapacity']), 2)
        tieflowDict[datetimeStr] = round(float(row['TieFlow']), 2)
        outageDict[datetimeStr] = round(float(row['Outages']), 2)

        netloadDict[datetimeStr] = float(row['Load']) - float(row['Wind']) - float(row['Solar'])
        netloadDict[datetimeStr] = round(netloadDict[datetimeStr], 2)
        # row['TieFlow']: + means export(load), - means import(gen)
        rttotcapDict[datetimeStr] = float(row['ThermalCapacity']) + float(row['PUNs']) + (-1.0 * float(row['TieFlow'])) 
        rttotcapDict[datetimeStr] -= netloadDict[datetimeStr]
        rttotcapDict[datetimeStr] -= float(row['Outages'])
        rttotcapDict[datetimeStr] -= ordcForecastBias
        rttotcapDict[datetimeStr] = round(rttotcapDict[datetimeStr], 2)

        rtolcapDict[datetimeStr] = rttotcapDict[datetimeStr]
        rtoffcapDict[datetimeStr] = 0  #need rtoffcap value, right now set as 0
        
        periodList.append(datetimeStr) 
 
    periodList.sort()
    nPeriod = len(periodList)

    return
#end of get_reserve_capacity_forecast()


def lolp_distribution_input():
    '''read LOLP distribution by season and TOD block'''

    fileStr = os.path.abspath(os.getcwd())
    fileStr = os.path.join(fileStr, "zip_dir", "lolp_distribution.csv")
    if not os.path.exists(fileStr):
        print("lolp_distribution.csv file does not exist, default lolp parameters will be used!")
        #exit()

    inFile = open(fileStr, 'r', newline='')
    filereader = csv.DictReader(inFile)
    global seasonDict
    global startdateDict
    global enddateDict
    global starthourDict
    global endhourDict
    global meanDict
    global stdDict

    global nLOLPDist
 
    seasonDict = {}
    startdateDict = {}
    enddateDict = {}
    starthourDict = {}
    endhourDict = {}
    meanDict = {}
    stdDict = {}
    
  #ignore year if the case is out of bount, only need to consider season and time of day
    cnt = 0
    for row in filereader:
        dateList = row['Start Date'].split('/')
        dateStrOutput = "%02d/%02d" % (int(dateList[0]), int(dateList[1]))
        startdateDict[cnt] = dateStrOutput

        dateList = row['End Date'].split('/')
        dateStrOutput = "%02d/%02d" % (int(dateList[0]), int(dateList[1]))
        enddateDict[cnt] = dateStrOutput

        seasonDict[cnt] = row['Season'].upper()
        starthourDict[cnt] = int(row['Start Hour'])
        endhourDict[cnt]  = int(row['End Hour'])
        meanDict[cnt] = float(row['Mean'].replace(',', ''))
        stdDict[cnt] = float(row['Standard Deviation'].replace(',', ''))

        cnt += 1
    nLOLPDist = cnt
    return
#end of lolp_distribution_input()


def rt_price_adder(rtolcap, rtoffcap, lmp, mu, sigma):
    ''' calculate real time online/offline reserve price addrer for different reserve capacity level'''

    rtoffpa = 0
    rtorpa = 0

    # calculate real time offline reserve price adder
    lolp = calculate_lolp(rtolcap + rtoffcap, MINCONTLEVEL, mu, sigma)
    rtoffpa = RTOFFPA_RATIO * max((VOLL - lmp), 0) * lolp

    # calculate real time online reserve price adder
    lolp = calculate_lolp(rtolcap, MINCONTLEVEL, RTORPA_MU_COEF * mu, RTORPA_SIGMA_COEF * sigma)
    rtorpa = rtoffpa + (1.0 - RTOFFPA_RATIO) * max((VOLL - lmp), 0) * lolp

    return(round(rtoffpa, 2), round(rtorpa,2))
# end of rt_price_adder()


def calculate_lolp(reserveAmount, minContingencyLevel, mean, std):
    '''calculate lolp for a given resere capacity amount with defined mean and standard deviation value of normal distribution,
    the mean and standard deviation vlaues vary for different season and time of day'''

    if (reserveAmount <= minContingencyLevel):
        lolp = 1.0
    else:
        lolp = 1.0 - scipy.stats.norm.cdf(reserveAmount - minContingencyLevel, mean, std)
    return (lolp)
# end of calculate_lolp()


# fill missing leading 0 in date time string to avoid periodList ordering issue
def date_format_change(datetimeStrInput):
    '''input date time string format without leading 0 (MM/DD/YYYY HH:MM)
    ---> output date time string format with leading 0'''

    datetimeList = datetimeStrInput.split(' ')
    dateList = datetimeList[0].split('/')
    timeList = datetimeList[1].split(':')
    datetimeStrOutput = "%02d/%02d/%04d %02d:%02d" % (int(dateList[0]), int(dateList[1]), int(dateList[2]), \
                                                        int(timeList[0]), int(timeList[1]))
        
    return datetimeStrOutput
# end of date_format_change(datetimeStrInput):


# Only works on Windows
def is_open(file_name):
    if os.path.exists(file_name):
        try:
            os.rename(file_name, file_name) #can't rename an open file so an error will be thrown
            return False
        except:
            return True
    #raise 'file is not existing'


def output_csv_to_db(csvFile):
    ercotdbserver = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};"
                                            "SERVER=brptemp.database.windows.net;"
                                            "DATABASE=ErcotMarketData;"
                                            "UID=brp_admin;"
                                            "PWD=Bro@dRe@chP0wer")
    dbengine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect={}".format(ercotdbserver))

    inFile = open(csvFile, 'r', newline='')
    df = pd.read_csv(inFile, delimiter = ",")
    #change the format to datetime
    df['DateTime'] = pd.to_datetime(df['DateTime'], format = '%m/%d/%Y %H:%M')
    df['ForecastRunDateTime'] = pd.to_datetime(df['ForecastRunDateTime'], format = '%m/%d/%Y %H:%M')

    #check to make sure the table looks okay
    print(df.head())
    df.to_sql('ordc_forecast', dbengine, if_exists='replace')
    #df.to_sql('ordc_forecast', con=dbengine,index=False,method=None,if_exists="append")


#ordc_forecast(markettype)
'''if __name__ == "__main__":
    main()'''
