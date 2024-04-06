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
rm(list = ls())

apx <-dbConnect(odbc(),
                Driver = "ODBC Driver 17 for SQL Server",
                Server = get_secret("database-server"),
                Port = 1433,
                UID = get_secret("database-username"),
                PWD = get_secret("database-password"),
                Database = "APX")

df <- as.data.table(dbReadTable(apx, 'temp_netload'))


colnames(df)[1] <- 'datetime'
df[, hour := hour(datetime)+1]

studyDay <- as.Date(Sys.Date()+days(1), tz = "America/New_York")

forecastDay <- df[datetime >= studyDay & datetime < studyDay+days(1), c("hour", "datetime", "fcast", "netload","TROCAPACITY")]

colnames(forecastDay)[1:5] <- c("hour","datetime_fx", "demandFx", "netloadFx", "outageFx")

dist <- merge(df[datetime < studyDay-days(1),], forecastDay, by = "hour")

dist[, dist := sqrt((SYSTEMDEMAND - demandFx)^2 + (netloadFx - actnetload)^2 + (outageFx-TROCAPACITY)^2)]
dist[, day := as.Date(datetime)]


closeDays <- dist[, .(dist = sum(dist)), by = 'day']
closeDays <- closeDays[order(dist, decreasing = FALSE),]

top10 <- closeDays[1:5, c('day','dist')]

proxyDays <- dist[top10, c('datetime','hour','day', 'HB_NORTH.SPP', 'HB_HOUSTON.SPP', 'HB_HUBAVG.SPP', 'HB_WEST.SPP', 'HB_SOUTH.SPP'), on = 'day']


degree <-12
# plot <- ggplot(data = proxyDays, aes(hour,HB_HUBAVG.SPP)) + geom_point() + geom_smooth(method = 'lm',formula = y ~ poly(x,degree))
# plot


HUBAVGmodel <- lm(HB_HUBAVG.SPP ~ poly(hour, degree), proxyDays)
summary(HUBAVGmodel)

forecastDay[, HB_HUBAVG := HUBAVGmodel$fitted.values[1:24]]

NORTHmodel <- lm(HB_NORTH.SPP ~ poly(hour, degree), proxyDays)
forecastDay[, HB_NORTH := NORTHmodel$fitted.values[1:24]]

SOUTHmodel <- lm(HB_SOUTH.SPP ~ poly(hour, degree), proxyDays)
forecastDay[, HB_SOUTH := SOUTHmodel$fitted.values[1:24]]

WESTmodel <- lm(HB_WEST.SPP ~ poly(hour, degree), proxyDays)
forecastDay[, HB_WEST := WESTmodel$fitted.values[1:24]]

HOUmodel <- lm(HB_HOUSTON.SPP ~ poly(hour, degree), proxyDays)
forecastDay[, HB_HOUSTON := HOUmodel$fitted.values[1:24]]


upload <- forecastDay[,c('datetime_fx', 'HB_HUBAVG', 'HB_NORTH', 'HB_SOUTH', 'HB_WEST', 'HB_HOUSTON')]

colnames(upload)[1] <- 'datetime'

upload <- melt(upload, id.vars = 'datetime', measure.vars = c('HB_HUBAVG', 'HB_NORTH', 'HB_SOUTH', 'HB_WEST', 'HB_HOUSTON'), variable.name = 'location',
               value.name = 'value')

# ErcotMarketData <-dbConnect(odbc(),
#                             Driver = "ODBC Driver 17 for SQL Server",
#                             Server = get_secret("database-server"),
#                             Port = 1433,
#                             UID = get_secret("database-username"),
#                             PWD = get_secret("database-password"),
#                             Database = "ErcotMarketData")
# 
# dbAppendTable(ErcotMarketData, 'Hub_Forecasts', upload)
