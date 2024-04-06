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
rm(list = ls())


data <- fromJSON(file  = "https://sa.ercot.com/misapp/servlets/IceDocListJsonWS?reportTypeId=13051")
# content <- content(data)

data_available <- rbindlist(lapply(1:length(data$ListDocsByRptTypeRes$DocumentList),function(x){
  result <- as.data.table(data$ListDocsByRptTypeRes$DocumentList[[x]]$Document)
  }), fill = TRUE)

data_available <- as.data.table(data_available)

data_available[, type := str_sub(FriendlyName, -3,-1)]

#csv can probably be removed
# csv_to_pull <- as.list(paste0("https://sa.ercot.com/misdownload/servlets/mirDownload?doclookupId=",
                           # data_available[type == "csv", DocID]))

table <- data_available[, c("DocID", "ConstructedName")] #change from the AS procurement: removed the type==csv filter
table <- table[, zipFile := paste0("https://sa.ercot.com/misdownload/servlets/mirDownload?doclookupId=",
                              DocID)]
table[, file := str_remove(ConstructedName, ".zip")]
table[, file := str_replace(file, '_','.')]


# list.files(pattern = paste0('60d_DAM_EnergyBids-',"*"))

ERCOTtoDT <- function(csvName,sqlTable){
  for(i in 1:nrow(table)){
    if(i ==1){
      setwd(tempdir())
      dest <- "temp.zip"
      download.file(paste0(table[i,zipFile]),destfile = dest,mode='wb')
      unzip('temp.zip')
      csv <- list.files(pattern = paste0(csvName,'*'))
      df<- as.data.table(read.csv(csv))
      removeFiles <- file.remove(list.files())
      print(i)
    }
    if(i > 1){
      setwd(tempdir())
      dest <- "temp.zip"
      download.file(paste0(table[i,zipFile]),destfile = dest,mode='wb')
      unzip('temp.zip')
      csv <- list.files(pattern = paste0(csvName,'*'))
      temp <- as.data.table(read.csv(csv))
      df <- rbind(df, temp)
      removeFiles <- file.remove(list.files())
      print(i)}
  }
  
  #replace periods with spaces
  
  colnames(df) <- str_replace_all(colnames(df), '\\.'," ")

  newData <- unique(df)
  newData <- as.data.table(newData)
  newData[,`Delivery Date` := as.POSIXct(newData$`Delivery Date`, tz="UTC", "%m/%d/%Y")]
  newData <- newData[!is.na(`Delivery Date`)]
  #### write data to SQL table ####
  
  ErcotMarketData <-dbConnect(odbc(),
                              Driver = "ODBC Driver 17 for SQL Server",
                              Server = "brptemp.database.windows.net",
                              Port = 1433,
                              UID = "brp_admin",
                              PWD = "Bro@dRe@chP0wer",
                              Database = "ErcotMarketData")
  
  currentDates <- dbGetQuery(ErcotMarketData, 
                             paste0('SELECT distinct [Delivery Date] FROM [dbo].[',sqlTable,']'))
  removal <- currentDates$`Delivery Date`
  
  setkey(newData, 'Delivery Date')
  upload <- newData[! `Delivery Date` %in% removal]
  blocks <- ceiling(nrow(upload)/50000)
  for(i in 1:blocks){
    start <- (i-1)*50000
    end <- ifelse((i*50000)<nrow(upload),i*50000,nrow(upload))
    dbAppendTable(ErcotMarketData,sqlTable, upload[start:end,], row.names=NULL)
  }
  print("Mission Acccomplished")
  return(upload)
}






GenASOffers <- ERCOTtoDT('60d_DAM_Generation_Resource_ASOffers-','60dDAMGenerationResourceASOffers')

LoadASOffers <- ERCOTtoDT('60d_DAM_Load_Resource_ASOffers-','60dDAMLoadResourceASOffers')

DAMEnergyBids <- ERCOTtoDT('60d_DAM_EnergyBids-','60dDAMEnergyBids')

DAMEnergyBidAwards <- ERCOTtoDT('60d_DAM_EnergyBidAwards-','60dDAMEnergyBidAwards')

DAMEnergyOnlyOfferAwards <- ERCOTtoDT('60d_DAM_EnergyOnlyOfferAwards-','60dDAMEnergyOnlyOfferAwards')

DAMEnergyOnlyOffers <- ERCOTtoDT('60d_DAM_EnergyOnlyOffers-','60dDAMEnergyOnlyOffers')

DAMGenResourceData <- ERCOTtoDT('60d_DAM_Gen_Resource_Data-','60dDAMGenResourceData')

DAMLoadResourceData <- ERCOTtoDT('60d_DAM_Load_Resource_Data-','60dDAMLoadResourceData')

DAMLoadResourceData <- ERCOTtoDT('60d_DAM_QSE_Self_Arranged_AS-','60dDAMQSESelfArrangedAS')




# #replace periods with spaces
# newColNames <- str_replace_all(colnames(df), '\\.'," ")
# 
# newData <- unique(df)
# newData <- as.data.table(newData)
# newData[,`Delivery Date` := as.POSIXct(newData$`Delivery Date`, tz="EST", "%m/%d/%Y")]
# newData <- newData[!is.na(`Delivery Date`)]
# 
# #### write data to SQL table ####
# 
# 
# 
# 
# ErcotMarketData <-dbConnect(odbc(),
#                             Driver = "ODBC Driver 17 for SQL Server",
#                             Server = "brptemp.database.windows.net",
#                             Port = 1433,
#                             UID = "brp_admin",
#                             PWD = "Bro@dRe@chP0wer",
#                             Database = "ErcotMarketData")
# 
# currentDates <- dbGetQuery(ErcotMarketData, 'SELECT distinct [Delivery Date] FROM [dbo].[60dDAMGenerationResourceASOffers]')
# removal <- currentDates$`Delivery Date`
# 
# setkey(newData, 'Delivery Date')
# upload <- newData[! `Delivery Date` %in% removal]
# 
# dbAppendTable(ErcotMarketData,'60dDAMGenerationResourceASOffers', upload, row.names=NULL)
# print("Mission Acccomplished")

# test <- '60d_DAM_Gen_Resource_Data'
# # dest <- "C:/Users/CaseyKopp/Documents/R Programs/ERCOT Downloads/temp.zip"
# download.file(paste0(table[1,zipFile]),destfile = dest,mode='wb')
# setwd('C:/Users/CaseyKopp/Documents/R Programs/ERCOT Downloads/')
# unzip('temp.zip')
# csv <- list.files(pattern = paste0(test,'*'))
# df<- as.data.table(read.csv(csv))
# removeFiles <- file.remove(list.files())
# print(i)
