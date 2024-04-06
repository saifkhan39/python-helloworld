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


data <- fromJSON(file  = "https://sa.ercot.com/misapp/servlets/IceDocListJsonWS?reportTypeId=13052")
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

ErcotMarketData <-dbConnect(odbc(),
                            Driver = "ODBC Driver 17 for SQL Server",
                            Server = "brptemp.database.windows.net",
                            Port = 1433,
                            UID = "brp_admin",
                            PWD = "Bro@dRe@chP0wer",
                            Database = "ErcotMarketData")

currentDates <- dbGetQuery(ErcotMarketData,
                           paste0('SELECT distinct [SCED Time Stamp] FROM [dbo].[','60d_RT_Gen_Data',']'))
removal <- currentDates$`SCED Time Stamp`

ERCOTtoDT <- function(csvName,sqlTable){
  for(i in 1:nrow(table)){
      tryCatch({
        setwd(tempdir())
        dest <- "temp.zip"
        download.file(paste0(table[i,zipFile]),destfile = dest,mode='wb')
        unzip('temp.zip')
        csv <- list.files(pattern = paste0(csvName,'*'))
        df<- as.data.table(read.csv(csv))
        removeFiles <- file.remove(list.files(pattern = '*.csv'))
        print(i)
        #replace periods with spaces
        
        colnames(df) <- str_replace_all(colnames(df), '\\.'," ")
        
        newData <- unique(df)
        newData <- as.data.table(newData)
        newData[,`SCED Time Stamp` := as.POSIXct(newData$`SCED Time Stamp`, tz="UTC", "%m/%d/%Y %H:%M:%S")]
        newData[,`SCED Time Stamp` := round_date(newData$`SCED Time Stamp`, unit='second')]
        newData <- newData[!is.na(`SCED Time Stamp`)]
        
        #### write data to SQL table ####
        
        ErcotMarketData <-dbConnect(odbc(),
                                    Driver = "ODBC Driver 17 for SQL Server",
                                    Server = "brptemp.database.windows.net",
                                    Port = 1433,
                                    UID = "brp_admin",
                                    PWD = "Bro@dRe@chP0wer",
                                    Database = "ErcotMarketData")
        
        setkey(newData, 'SCED Time Stamp')
        # upload <- newData
        upload <- newData[! `SCED Time Stamp` %in% removal]
        blocks <- ceiling(nrow(upload)/10000)
        if(blocks > 0){
          for(i in 1:blocks){
            start <- (i-1)*10000
            end <- ifelse((i*10000)<nrow(upload),i*10000,nrow(upload))
            tryCatch({dbAppendTable(ErcotMarketData,sqlTable, upload[start:end,], row.names=NULL)
              print(paste0("Mission Acccomplished ",i))},
              error=function(e){cat("ERROR :",conditionMessage(e),"\n")})
          }
        }
        else{print('Already Uploaded')}
      },
      error=function(e){cat("ERROR :",conditionMessage(e),"\n")},
      warning=function(w){cat("WARNING: ",conditionMessage(w),"\n")})
  }
}

ERCOTtoDT('60d_SCED_Gen_Resource_Data','60d_RT_Gen_Data')




