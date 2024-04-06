library(httr)
library(data.table)
library(rjson)
library(stringr)
library(utils)
library(ggplot2)
library(RODBC)
library(DBI)
library(odbc)
library(patchwork)
library(lubridate)
library(lpSolve)
library(RCurl)
library(XML)
rm(list = ls())

#### update ancillary procurements ####

updateProcurements <- function(){
  data <- fromJSON(file  = vault.get_secret('ercot-sa-url')+"/misapp/servlets/IceDocListJsonWS?reportTypeId=12316")
  # content <- content(data)

  data_available <- rbindlist(lapply(1:length(data$ListDocsByRptTypeRes$DocumentList),function(x){
    result <- as.data.table(data$ListDocsByRptTypeRes$DocumentList[[x]]$Document)
  }), fill = TRUE)

  data_available <- as.data.table(data_available)

  data_available[, type := str_sub(FriendlyName, -3,-1)]

  #csv can probably be removed
  csv_to_pull <- as.list(paste0(vault.get_secret('ercot-sa-url')+"/misdownload/servlets/mirDownload?doclookupId=",
                                data_available[type == "csv", DocID]))

  table <- data_available[type == "csv", c("DocID", "ConstructedName")]
  table <- table[, zipFile := paste0(vault.get_secret('ercot-sa-url')+"/misdownload/servlets/mirDownload?doclookupId=",
                                     DocID)]
  table[, file := str_remove(ConstructedName, ".zip")]
  table[, file := str_replace(file, '_','.')]

  for(i in 1:nrow(table)){
    if(i ==1){
      setwd(tempdir())
      dest <- "temp.zip"
      download.file(paste0(table[i,zipFile]),destfile = dest,mode='wb')
      unzip('temp.zip')
      csv <- list.files(pattern = '*.csv')
      df<- as.data.table(read.csv(csv))
      removeFiles <- file.remove(list.files())
      print(i)
    }
    if(i>1){
      setwd(tempdir())
      dest <- "temp.zip"
      download.file(paste0(table[i,zipFile]),destfile = dest,mode='wb')
      unzip('temp.zip')
      csv <- list.files(pattern = '*.csv')
      temp <- as.data.table(read.csv(csv))
      df <- rbind(df, temp)
      removeFiles <- file.remove(list.files())
      print(i)
    }
  }

  df <- na.omit(df)
  newData <- unique(df)

  #### write data to SQL table ####

  ErcotMarketData <-dbConnect(odbc(),
                              Driver = "ODBC Driver 17 for SQL Server",
                              Server = "brptemp.database.windows.net",
                              Port = 1433,
                              UID = "brp_admin",
                              PWD = "Bro@dRe@chP0wer",
                              Database = "ErcotMarketData")

  currentDates <- dbGetQuery(ErcotMarketData, 'SELECT distinct [DeliveryDate] FROM [dbo].[AS_Procurement_Plan]')
  removal <- as.vector(currentDates$DeliveryDate)

  setkey(newData, DeliveryDate)
  upload <- newData[!removal]

  dbAppendTable(ErcotMarketData,'AS_Procurement_Plan', upload, row.names=NULL)
  
}

updateProcurements()

#### get the proxy days from Yes Energy ####


# user = 'dl_trading@broadreachpower.com'
# password = 'joshallen'
# 
# query <- getURL("https://services.yesenergy.com/PS/rest/collection/date/9258275",userpwd = paste0(user,":",password))
# content <- readHTMLTable(query)
# 
# proxy_days <- rbindlist(lapply(1:length(content),function(x){
#   result <- as.data.table(content[[x]])}), fill = TRUE)
# proxy_days[, date := as.Date(DAY, tz = 'America/Chicago', format = '%m/%d/%Y %H:%M:%S')]
# 
# 
# nearest <- proxy_days[, 'date']


#### get the proxy days from BRP proxy days ####

ErcotMarketData <-dbConnect(odbc(),
                            Driver = "ODBC Driver 17 for SQL Server",
                            Server = "brptemp.database.windows.net",
                            Port = 1433,
                            UID = "brp_admin",
                            PWD = "Bro@dRe@chP0wer",
                            Database = "ErcotMarketData")

proxy_days <- as.data.table(dbGetQuery(ErcotMarketData, paste0("SELECT [ND] FROM [dbo].[proxy_date_ranking]")))

proxy_days[, date := as.Date(ND, tz = 'America/Chicago', format = '%m/%d/%Y')]

nearest <- proxy_days[, 'date']

#### get the clearing prices ####

start <- '2019-01-01'
end <- as.Date(Sys.Date() - days(1), tz = 'America/Chicago')

queryParameters<- list(agglevel = 'hour',
                       startdate = start,
                       enddate = end,
                       timezone = 'CST',
                       items = 'ASM_DA_RRS:10000756298,ASM_DA_REGDOWN:10000756298,ASM_DA_REGUP:10000756298,ASM_DA_NONSPIN:10000756298')

user = 'dl_trading@broadreachpower.com'
password = 'joshallen'

query <- GET("https://services.yesenergy.com/PS/rest/timeseries/multiple.json?", query = queryParameters,
             authenticate(user,password))
content <- content(query)

closeDay <- rbindlist(lapply(1:length(content),function(x){
  result <- as.data.table(content[[x]])}), fill = TRUE)

setnames(closeDay, old = c('ERCOT (ASM_DA_RRS)', 'ERCOT (ASM_DA_REGDOWN)', 'ERCOT (ASM_DA_NONSPIN)', 'ERCOT (ASM_DA_REGUP)'),
         new = c('RRS','REGDN','REGUP','NSPIN'))

closeDay[, datetime := as.POSIXct(DATETIME, tz = 'America/Chicago', format = '%m/%d/%Y %H:%M:%S')]
closeDay[, datetime := datetime - hours(1)]
closeDay[, date := as.Date(datetime, tz = 'America/Chicago')]
closeDay <- closeDay[date %in% nearest$date,]

closeDay <- closeDay[, c('datetime','RRS','REGDN','REGUP','NSPIN')]
closeDay[, RRS := as.numeric(RRS)]
closeDay[, REGDN := as.numeric(REGDN)]
closeDay[, REGUP := as.numeric(REGUP)]
closeDay[, NSPIN := as.numeric(NSPIN)]


#### HERE BEGINS THE ANALYSIS ####

#start by pulling the curves for the previous week

ErcotMarketData <-dbConnect(odbc(),
                            Driver = "ODBC Driver 17 for SQL Server",
                            Server = "brptemp.database.windows.net",
                            Port = 1433,
                            UID = "brp_admin",
                            PWD = "Bro@dRe@chP0wer",
                            Database = "ErcotMarketData")

datestring <- function(x, count){
  output <- paste0("'",format(x[1,date],"%m/%d/%Y"),"'")
  for(i in 2:count){
    output <- paste0(output,",'",format(x[i,date],"%m/%d/%Y"),"'")
  }
  return(output)
}

closeDays <- datestring(proxy_days, 15)

asCurves <- dbGetQuery(ErcotMarketData, paste0("SELECT * FROM [dbo].[AGG_AS_Curves] where DeliveryDate in (",closeDays,")"))
asCurves <- as.data.table(asCurves)
asCurves[, Quantity := as.numeric(Quantity)]
asCurves[, Price := as.numeric(Price)]

#create the AS curves table

# asCurves[,DeliveryDate := as.Date(DeliveryDate,format = "%m/%d/%Y")]
asCurves[, HE := as.numeric(substr(HourEnding,1,2))]

asCurves[, type := ifelse(substr(AncillaryType,1,3) == "RRS", "RRS", 
                          ifelse(AncillaryType == "OFFNS", "NSPIN",
                                 ifelse(AncillaryType == "ONNS", "NSPIN", AncillaryType)))]

asCurves <- asCurves[AncillaryType %in% c('REGDN', 'REGUP', 'RRSGN','ONNS')]

marginalMW <- function(x) {
  x <- x[order(DeliveryDate,HourEnding,AncillaryType,Price),]
  uniquePairs <- unique(x[,c("DeliveryDate","HourEnding","AncillaryType")])
  for(i in 1:nrow(uniquePairs)){
    x[DeliveryDate == uniquePairs[i,"DeliveryDate"] & HourEnding == uniquePairs[i,"HourEnding"]
      & AncillaryType == uniquePairs[i,"AncillaryType"],
      marginalMW := ifelse(is.na(Quantity - shift(Quantity, n = 1, type = "lag")),
                           Quantity,
                           Quantity - shift(Quantity, n = 1, type = "lag"))]}
  return(x)
}

asCurves <- marginalMW(asCurves)


#### get entire MW offered for each AS product 

totalMW <- function(x) {
  uniquePairs <- unique(x[,c("DeliveryDate","HourEnding","type")])
  x <- x[order(DeliveryDate,HourEnding,type,Price),]
  for(i in 1:nrow(uniquePairs)){
    x[DeliveryDate == uniquePairs[i,"DeliveryDate"] & HourEnding == uniquePairs[i,"HourEnding"]
      & type == uniquePairs[i,"type"],
      totalMW := cumsum(marginalMW)]}
  return(x)
}

asCurves <- totalMW(asCurves)  

#convert the date in asCurves to datetime

asCurves[, DeliveryDate := as.Date(DeliveryDate, tz = 'America/Chicago',format = "%m/%d/%Y")]

#get the prices for that given day and format the data




asPrices <- closeDay

asPrices[, DeliveryDate := as.Date(datetime, tz = 'America/Chicago')]
asPrices[, HE := hour(datetime)+1]
asPrices <- melt(asPrices, measure.vars = c("RRS","REGUP","REGDN","NSPIN"), variable.name = "type", value.name = "MCPC")



#merge the tables together

df <- merge(asCurves, asPrices, by = c("DeliveryDate", "HE","type"))

# mark where the clearing price matches the bid prices and pull quantity

#get the closest prices to the clearing price

df[, dist := abs(Price-MCPC)]

distanceDF <- df[,.(dist = min(dist)), by = c("DeliveryDate", "HourEnding","type")]
clearedDF <- df[distanceDF, on = c("DeliveryDate", "HourEnding","type","dist")]

#filter out situations where the marginal MW bid is 0
maxClears <- clearedDF[, .(maxQ = max(totalMW)), by = c("DeliveryDate", "HourEnding","type")] #maxQ stands for the max of the quantity

clearedVolumes <- clearedDF[maxClears, on = c("DeliveryDate", "HourEnding","type")]
clearedVolumes <- clearedVolumes[maxQ == totalMW & marginalMW >0,]

volume <- clearedVolumes[, c("DeliveryDate", "HourEnding", "type", "AncillaryType",
                             "Price", "Quantity","marginalMW","totalMW", "MCPC")]

#pull in as plan

asPlan <- dbGetQuery(ErcotMarketData, paste0("SELECT * FROM [dbo].[AS_Procurement_Plan] where DeliveryDate in (",closeDays,")"))

asPlan <- as.data.table(asPlan)
names(asPlan)[names(asPlan) == c("DeliveryDate", "HourEnding","AncillaryType","Quantity","DSTFlag")] <- c("DeliveryDate", 
                                                                                                          "HourEnding", 
                                                                                                          "type", 
                                                                                                          "PlannedQuantity",
                                                                                                          "DSTFLAG")
asPlan[, DeliveryDate := as.Date(DeliveryDate, format = "%m/%d/%Y")]
asPlan[, PlannedQuantity := as.numeric(PlannedQuantity)]
asPlan <- unique(asPlan)

comparison <- merge(volume, asPlan, by = c("DeliveryDate","HourEnding","type"))

comparison[, difference := totalMW-PlannedQuantity]
comparison[, HE := as.numeric(substr(HourEnding,1,2))]


#pull asplan for current day
start <- floor_date(Sys.Date())+days(1)
day <- format(start, '%m/%d/%Y')
currentASPlan <- dbGetQuery(ErcotMarketData, paste0("SELECT * FROM [dbo].[AS_Procurement_Plan] where DeliveryDate = '",day,"'"))

currentASPlan <- as.data.table(currentASPlan)
currentASPlan[, Quantity := as.numeric(Quantity)]
currentASPlan[, type := AncillaryType]
currentASPlan <- currentASPlan[,!'DeliveryDate']
names(currentASPlan)[names(currentASPlan) == "Quantity"] <- 'currentProcurement'
currentASPlan <- currentASPlan[,.(currentProcurement = max(currentProcurement)), by = c('HourEnding','type','AncillaryType')]

comparison <- merge(comparison, currentASPlan, by = c('HourEnding', 'type'))
# removing the scalar on the difference because prices are too high
# comparison[, adder := difference*(currentProcurement/PlannedQuantity)]

comparison[, adder := difference*(currentProcurement/PlannedQuantity)]

# this will calculate the marginal cost for each hour of the day
# the comparison stack is the aggregated supply stack from the proxy day
# scalars are the procurement amount multiplied by the average scalar
# the as plan is the procurement levels produced by ERCOT (be sure to aggregate the different markets to type)
#THINGS TO CLEAN UP
# have the scalar be on a daily basis rather than an average of all the days

asCalculator <- function(Comparison_Stack, scalars, as_plan){
  for(i in 1:nrow(scalars)){
    temp <- Comparison_Stack[HourEnding == scalars[i,HourEnding] & type == scalars[i,type],] #grab the hourly supply stack
    procurement <- as_plan[HourEnding == scalars[i,HourEnding] & type == scalars[i,type], currentProcurement] + scalars[i, adder] # determine the adjusted procurement amount
    # if(max(temp$totalMW) < procurement){
    #   procurement <- as_plan[HourEnding == scalars[i,HourEnding] & type == scalars[i,type], Quantity] + (scalars[i, adder]-scalars[i,stdev])
    #   print(paste0("First St Dev on ",scalars[i, HourEnding],scalars[i, type]))
    #   if(max(temp$totalMW) < procurement){
    #     procurement <- as_plan[HourEnding == scalars[i,HourEnding] & type == scalars[i,type], Quantity] + (scalars[i, adder]-(2*scalars[i,stdev]))
    #     print(paste0("Second St Dev on ",scalars[i, HourEnding],scalars[i, type]))
    #     }
    # }
    offerQuantity <- as.matrix(temp$marginalMW) # organize the bid quantities
    opt <- as.matrix(temp$Price) # matrix of the prices
    countBids <- as.numeric(length(offerQuantity)) #count of the length of the matrix to optimize across
    master <- as.matrix(rbind(rep(1, as.numeric(length(offerQuantity))),
                              diag(1,countBids))) # identity matrix for the limits 
    limits <- as.matrix(rbind(procurement, offerQuantity))
    dir <- as.matrix(rbind(as.matrix("="),
                           as.matrix(rep("<=", countBids))))
    solution <- lp(direction = "min", objective.in = opt, const.dir = dir,
                   const.mat = master, const.rhs = limits, compute.sens = TRUE)
    scalars[i, baseForecast := solution$duals[1]]
    scalars[i, procurementAmount := procurement]
    scalars[i, scheduledProcurement := as_plan$currentProcurement[i]]
  }
  return(scalars)
}


multiplier <- comparison[, .(adder = mean(adder), stdev = sd(adder)), by = c("DeliveryDate","HourEnding","type")]

topNdays <- function(x){
  output <- data.table()
  # output <- asCalculator(asCurves[DeliveryDate == nearest[1,date],], multiplier, currentASPlan)
  # output[, compDay := nearest[1,date]]
  # print(nearest[1,date])
  
  for(i in 1:x){
    if (nrow(asCurves[DeliveryDate == nearest[i,date],]) == 0 | nrow(multiplier[DeliveryDate == nearest[i,date],])==0) {
      print(paste0("SKIPPED ",nearest[i,date]))
      next
    }
    temp <- asCalculator(asCurves[DeliveryDate == nearest[i,date],], multiplier[DeliveryDate == nearest[i,date],], currentASPlan)
    temp[, compDay := nearest[i,date]]
    output <- rbind(output, temp)
    print(nearest[i,date])
  }
  output <- output[baseForecast != 0,]
  final <- dcast(output, HourEnding + compDay ~ type, value.var = "baseForecast", fun.aggregate = mean)
  return(final)
}

test <- topNdays(15)

price <- test[,.(RRS = mean(RRS, na.rm =TRUE), REGDN = mean(REGDN, na.rm =TRUE),
                 REGUP = mean(REGUP, na.rm =TRUE), NSPIN = mean(NSPIN, na.rm = TRUE)),
              by = "HourEnding"]

ggplot(test[REGDN < 500 & compDay != '2021-02-20',], aes(x = HourEnding,y = REGDN)) + geom_boxplot() +
  labs(x = "Hour Ending",
       y = "Price ($/MWHr)",
       title = "REGDN")

ggplot(test[REGUP < 500 & compDay != '2021-02-20',], aes(x = HourEnding,y = REGUP)) + geom_boxplot() +
  labs(x = "Hour Ending",
       y = "Price ($/MWHr)",
       title = "REGUP")

ggplot(test[RRS < 500 & compDay != '2021-02-20',], aes(x = HourEnding,y = RRS)) + geom_boxplot() +
  labs(x = "Hour Ending",
       y = "Price ($/MWHr)",
       title = "RRS")

ggplot(test[NSPIN < 500,], aes(x = HourEnding,y = NSPIN)) + geom_boxplot() +
  labs(x = "Hour Ending",
       y = "Price ($/MWHr)",
       title = "NSPIN")

price[, DSTFlag := "N"]


end <- start + hours(23)
forecastDays <- seq.POSIXt(start, end,by = 'hour')

price[, OperatingDate := format(forecastDays, "%m/%d/%Y %H:%M")]


upload <- price[, c("OperatingDate", "NSPIN", "REGDN","RRS","REGUP","DSTFlag")]
upload[, NSPIN := round(NSPIN, 2)]
upload[, REGDN := round(REGDN, 2)]
upload[, RRS := round(RRS, 2)]
upload[, REGUP := round(REGUP, 2)]

upload[, NSPIN := ifelse(is.na(NSPIN) | NSPIN > RRS,RRS*.7,NSPIN)]


print(head(nearest))

setwd("C:/Users/CaseyKopp/Broad Reach Power LLC/BRP - Documents/Cap and Trade/Market Research/Python_programs/pbuc_results_mix_genscape_lmp_ercot_as_mcp/AS price forecast")
# write.csv(upload, paste0("as_forecast",year(forecastDay),month(forecastDay),day(forecastDay),".csv"),
# row.names = FALSE,quote=FALSE)
write.csv(upload, "jaguar_as_mcp.csv",
          row.names = FALSE,quote=FALSE)

#### write the forecast to SQL database
forSQL <- melt(upload, id.vars = c("OperatingDate", "DSTFlag"), measure.vars = c("NSPIN", "REGDN","REGUP","RRS"),
               variable.name = "forecast", value.name = "value")
forSQL <- as.data.table(forSQL)
forSQL[,OperatingDate := as.POSIXct(OperatingDate, tz="UTC", format = "%m/%d/%Y %H:%M")]

apx <-dbConnect(odbc(),
                Driver = "ODBC Driver 17 for SQL Server",
                Server = get_secret("database-server"),
                Port = 1433,
                UID = get_secret("database-username"),
                PWD = get_secret("database-password"),,
                Database = "APX")

dbWriteTable(apx, "temp_as_forecast", forSQL, overwrite = TRUE)
dbExecute(apx, "as_forecast_move")

#### add a scenarios table ####

long.table <- test
long.table[NSPIN == 'NaN', NSPIN := NA]
long.table[RRS == 'NaN', RRS := NA]
long.table[REGDN == 'NaN', REGDN := NA]
long.table[REGUP == 'NaN', REGUP := NA]
long.table[,OperatingDate := as.POSIXct(day, tz="America/Chicago", format = "%m/%d/%Y")]
long.table[, OperatingDate := as.POSIXct(day, tz="America/Chicago", format = "%m/%d/%Y")+(hours(as.numeric(substr(HourEnding,1,2))-1))]
long.table[, comp_day := as.Date(compDay)]

ScenarioID <- data.table(comp_day = unique(long.table$comp_day),
                         scenario = c(1:length(unique(long.table$compDay))))

long.table <- merge(long.table, ScenarioID, by = c('comp_day'))

# get the unique ids
ercot <-dbConnect(odbc(),
                Driver = "ODBC Driver 17 for SQL Server",
                Server = get_secret("database-server"),
                Port = 1433,
                UID = get_secret("database-username"),
                PWD = get_secret("database-password"),,
                Database = "MarketData")

IDs <- as.data.table(dbGetQuery(ercot, 'Select * from dbo.timeseries_metadata where ID >= 100002 and ID <= 100046'))


# turn the table into long form
ts.upload <- long.table[, c('OperatingDate', 'RRS', 'NSPIN','REGUP','REGDN', 'scenario')]

ts.upload <- melt(ts.upload, id.vars = c('OperatingDate','scenario'),
             measure.vars = c('RRS','NSPIN','REGUP','REGDN'),
             variable.name = 'type',
             value.name = 'value')

setnames(ts.upload, old = c('OperatingDate'),
         new = 'datetime_local')

#create the utc time 
ts.upload[, datetime_utc := with_tz(datetime_local, tz = 'UTC')]
ts.upload[, Name := paste0(type, ' Forecast: Scenario ', scenario)]

ts.upload <- merge(ts.upload, IDs[, c('ID','Name')], by = c('Name'))

# get the update)time in utc
ts.upload[, update_time := as.POSIXct(Sys.time(), tz = 'America/Chicago')]
ts.upload[, update_time := with_tz(update_time, tz = 'UTC')]

ts.upload[, update_time := with_tz(update_time, tz = 'UTC')]

#get the local time to show up as CPT
ts.upload[, datetime_local := as.POSIXct(datetime_utc - hours(as.numeric(as.POSIXct(format(datetime_utc, '%Y-%m-%d %H:%M:%S')) - as.POSIXct(datetime_utc, 'UTC'))), tz = 'UTC')]

ts.upload <- ts.upload[,c('ID','datetime_utc','datetime_local', 'update_time', 'value')]

dbAppendTable(ercot, 'timeseries', ts.upload)





