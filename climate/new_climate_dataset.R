rm(list = ls())
library(RPostgreSQL) # Need to explore RJDBC to connect with vertica
library(data.table)
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv,user="teamuser",password="",
                 dbname="team_2.0_production",port="5444",
                 host="data.team.sdsc.edu")

cl_3_data <- dbSendQuery(con,"SELECT * FROM dqa_climate3")
data_3 <- fetch(cl_3_data, n = -1)
#data_3["protocol"] <- 3 # Mark records as climate protocol 3

#** Get Clamate 2.0 data from climate_samples table 
cl_2_data <- dbSendQuery(con,"SELECT * FROM dqa_climate2_analysis")
data_2 <- fetch(cl_2_data, n = -1)
# Get data_2 to match the structure of data_3
# Create new columns
data_2$Observation <- paste(data_2$ObservationDate,data_2$ObservationTime)
setnames(data_2,"Id","ReferenceID")
setnames(data_2,"TotalSolarRadiation","Sensor1_TotalSolarRadiation")
setnames(data_2,"Precipitation","Rainfall")
setnames(data_2,"DryTemperature","Sensor1_AvgTemp")
setnames(data_2,"RelativeHumidity","Sensor1_RelativeHumidity")
setnames(data_2,"Site Name","TEAMSiteName")
data_2["RecordID"]<- NA
data_2["MinimumBatteryVoltage"]<- NA
data_2["Sensor1_TempStdDeviation"]<- NA
data_2["Sensor2_AvgTemp"]<- NA
data_2["Sensor2_TempStdDeviation"]<- NA
data_2["Sensor2_RelativeHumidity"]<- NA
data_2["Sensor3_AvgTemp"]<- NA
data_2["Sensor3_TempStdDeviation"]<- NA
data_2["Sensor3_RelativeHumidity"]<- NA
data_2["Sensor1_AvgSolarRadiation"]<- NA
data_2["Sensor1_SolarRadiationStdDeviation"]<- NA
data_2["Sensor2_AvgSolarRadiation"]<- NA
data_2["Sensor2_SolarRadiationStdDeviation"]<- NA
data_2["Sensor2_TotalSolarRadiation"]<- NA
data_2["SerialNumber"]<- NA
data_2["ProgramName"]<- NA
data_2["OperatingSystem"]<- NA
data_2["Tachometer_RPM"]<- NA
# Get rid of these two coolumns
data_2$ObservationDate <- NULL
data_2$ObservationTime <- NULL
data_2_new <-data.frame(cbind(data_2$ReferenceID, data_2$Observation, data_2$RecordID, data_2$MinimumBatteryVoltage,
                              data_2$Sensor1_AvgTemp, data_2$Sensor1_TempStdDeviation, data_2$Sensor1_RelativeHumidity,
                              data_2$Sensor2_AvgTemp, data_2$Sensor2_TempStdDeviation, data_2$Sensor2_RelativeHumidity,
                              data_2$Sensor3_AvgTemp, data_2$Sensor3_TempStdDeviation, data_2$Sensor3_RelativeHumidity,
                              data_2$Sensor1_AvgSolarRadiation, data_2$Sensor1_SolarRadiationStdDeviation,data_2$Sensor1_TotalSolarRadiation,
                              data_2$Sensor2_AvgSolarRadiation, data_2$Sensor2_SolarRadiationStdDeviation,data_2$Sensor2_TotalSolarRadiation,
                              data_2$Rainfall,data_2$SerialNumber,data_2$ProgramName,data_2$OperatingSystem,
                              data_2$Tachometer_RPM, data_2$ProtocolVersion, data_2$SamplingUnitName, data_2$Latitude,
                              data_2$Longitude, data_2$TEAMSiteName))
colnames(data_2_new)<-c('ReferenceID','Observation','RecordID','MinimumBatteryVoltage','Sensor1_AvgTemp','Sensor1_TempStdDeviation',
                        'Sensor1_RelativeHumidity','Sensor2_AvgTemp','Sensor2_TempStdDeviation','Sensor2_RelativeHumidity',
                        'Sensor3_AvgTemp','Sensor3_TempStdDeviation','Sensor3_RelativeHumidity','Sensor1_AvgSolarRadiation',
                        'Sensor1_SolarRadiationStdDeviation','Sensor1_TotalSolarRadiation','Sensor2_AvgSolarRadiation','Sensor2_SolarRadiationStdDeviation',
                        'Sensor2_TotalSolarRadiation','Rainfall','SerialNumber','ProgramName','OperatingSystem',
                        'Tachometer_RPM','ProtocolVersion','SamplingUnitName','Latitude',
                        'Longitude','TEAMSiteName')

# Combine climate 2.0 and 3.0 dataasets 
cl_data_all = rbind(data_3,data_2_new)
sysdate = Sys.Date()
filename= paste("cl_data_new",sysdate,".gzip",sep="")
save(cl_data_all, file=filename,compress="gzip")