library(httr)
library(data.table)
library(rjson)
library(stringr)
library(utilities)
library(filesstrings)
library(rlist)
library(utils)
library(readxl)
library(lubridate)
library(RODBC)
library(DBI)
library(odbc)
library(patchwork)
library(ggplot2)

rm(list = ls())


DST <- "YES"

ErcotMarketData <-dbConnect(odbc(),
                            Driver = "ODBC Driver 17 for SQL Server",
                            Server = get_secret("database-server"),
                            Port = 1433,
                            UID = get_secret("database-username"),
                            PWD = get_secret("database-password"),
                            Database = "ErcotMarketData")

deployments <- dbGetQuery(ErcotMarketData,
                           "SELECT TOP (500000) [id]
                           ,[dtm_created]
                           ,[dtm_updated]
                           ,[source]
                           ,[dtm]
                           ,[value]
                           ,[data_id]
                           ,[field_id]
                           FROM [dbo].[spider_isosystemdatavalue]
                           where field_id in (30,31,32,33)
                          order by id desc")

id <- data.table(
  field_id = c(30,31,32,33),
  name = c('DRU',
           'DRD',
           'RU',
           'RD')
)

df <- merge(deployments,id, by = 'field_id')
df <- as.data.table(df)
df[, datetime := as.POSIXct(dtm)]
df[, hb := floor_date(datetime, 'hour')]

df <- dcast(df, hb ~ name, value.var = 'value', fun = mean)

df[, 'Reg Down Exhaustion' := DRD/RD]
df[, 'Reg Up Exhaustion' := DRU/RU]


if(DST == "YES"){
  Yesterday <- Sys.Date()-days(1)+hours(5)
  Today <- Sys.Date() + hours(5)
  hist <- df[hb >= (Today - days(7)) & hb < Today,]
  hist[,cst := hb - hours(5)]
  hist[, HourEnding := hour(cst)+1]
} else{
  Yesterday <- Sys.Date()-days(1)+hours(6)
  Today <- Sys.Date() + hours(6)
  hist <- df[hb >= (Today - days(7)) & hb < Today,]
  hist[,cst := hb - hours(6)]
  hist[, HourEnding := hour(cst)+1]
}

# upload <- df[hb >= Today - days(13) & hb < Today,]

maxday <- dbGetQuery(ErcotMarketData, 'SELECT max(hb) FROM [dbo].[Regulation_Deployments]')

upload <- df[hb > maxday[1,1] & hb < Today]

upload[, updateTime := as.POSIXct(Sys.time())]

dbAppendTable(ErcotMarketData, "Regulation_Deployments", upload, row.names = NULL)

reg_fx <- hist[, .('Exhaustion_RU' = mean(`Reg Up Exhaustion`), 'Exhaustion_RD' = mean(`Reg Down Exhaustion`)),
              by = 'HourEnding']

reg_fx[, Date := Today + days(1) - hours(ifelse(DST == "YES", 5, 6))]

reg_fx <- reg_fx[,c('Date', 'HourEnding', 'Exhaustion_RU', 'Exhaustion_RD')]

reg_fx[, datetime := Date + hours(HourEnding - 1)]

dbAppendTable(ErcotMarketData, "Reg_Deployment_Forecast", reg_fx, row.names = NULL)





