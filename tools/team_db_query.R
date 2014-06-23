# team_db_query.r
#
f.teamdb.query <- function(dataset) {
  # Database query function of TEAM production database. Returns a R Object for
  # each dataset and saves to the workspace. The function contains views or pre-
  # built queries. These should not be changed. Do not call this function 
  # repeatedly during script development. Instead run the function and load
  # the R object. This will minimize hits on the database. 
  # Example usage: ctdata <- f.teamdb.query("camera trap")
  # Args:
  # Dataset: The dataset you want. Acceptable args: "vegetation", "climate",
  # "camera trap", "camera trap adv". 
  # Returns:
  # A dataframe or list with the desired datasets. 
  # Camera trap datasets: cam_trap_data and cam_trap_data_adv R object and dataframe. 
  # Camera trap adv will include much more camera trap metadata and image EXIF 
  # information.
  # Climate dataset: Returns a list with two dataframes in it.
  # cl_data: All climate 2.0 and 3.0 data
  # cl_temp_maxmin: The temperature maxmin associated with each TEAM Site.
  #   This is used for quality control 
  # Vegetation dataset: Returns a list wtih two dataframes in it:
  # tree: The entire tree dataset
  # liana: The entire liana dataset
  library(data.table)
  library(lubridate)
  library(RPostgreSQL) # Need to explore RJDBC to connect with vertica
  drv <- dbDriver("PostgreSQL")
  con <- dbConnect(drv,user="readonlyuser",password="rwanda*2014",
                   dbname="team_2.0_production",port="5444",
                   host="data.team.sdsc.edu")
  
  if (dataset == "camera trap adv") {
    cam_trap_query <- dbSendQuery(con,"select * from dqa_ct_advanced")
    cam_trap_data_adv <- fetch(cam_trap_query, n = -1)
    # Setup the filename with the Date. 
    sysdate = Sys.Date()
    filename= paste("ct_data_adv",sysdate,".gzip",sep="")
    save(cam_trap_data_adv, file=filename,compress="gzip")
    return(cam_trap_data_adv) # Return dataframe with CT data
    
    
  } else if (dataset == "camera trap") {
    cam_trap_query <- dbSendQuery(con,"select * from dqa_ct_ordered")
    cam_trap_data <- fetch(cam_trap_query, n = -1)
    # Setup the filename with the Date. 
    sysdate = Sys.Date()
    filename= paste("ct_data",sysdate,".gzip",sep="")
    save(cam_trap_data, file=filename,compress="gzip")
    return(cam_trap_data) # Return dataframe with CT data
    
  } else if (dataset == "climate") {
    cl_3_data <- dbSendQuery(con,"SELECT * FROM dqa_climate3")
    data_3 <- fetch(cl_3_data, n = -1)
    #data_3["protocol"] <- 3 # Mark records as climate protocol 3
    
    #** Get Clamate 2.0 data from climate_samples table 
    cl_2_data <- dbSendQuery(con,"SELECT * FROM dqa_climate2_analysis")
    data_2 <- fetch(cl_2_data, n = -1)
    # Get data_2 to match the structure of data_3
    # Create new columns
    data_2$collected_at <- paste(data_2$collected_at,data_2$collected_time)
    data_2$Observation <-data_2$collected_at
    setnames(data_2,"Id","ReferenceID")
    setnames(data_2,"TotalSolarRadiation","Sensor1_TotalSolarRadiation")
    setnames(data_2,"Precipitation","Rainfall")
    setnames(data_2,"DryTemperature","Sensor1_AvgTemp")
    setnames(data_2,"RelativeHumidity","Sensor1_RelativeHumidity")
    setnames(data_2,"SiteName","TEAMSiteName")
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
    #data_2$ObservationDate <- NULL
    #data_2$ObservationTime <- NULL
    data_2_new <-data.frame(cbind(data_2$ReferenceID, data_2$Observation, data_2$RecordID, data_2$MinimumBatteryVoltage,
                                  data_2$Sensor1_AvgTemp, data_2$Sensor1_TempStdDeviation, data_2$Sensor1_RelativeHumidity,
                                  data_2$Sensor2_AvgTemp, data_2$Sensor2_TempStdDeviation, data_2$Sensor2_RelativeHumidity,
                                  data_2$Sensor3_AvgTemp, data_2$Sensor3_TempStdDeviation, data_2$Sensor3_RelativeHumidity,
                                  data_2$Sensor1_AvgSolarRadiation, data_2$Sensor1_SolarRadiationStdDeviation,data_2$Sensor1_TotalSolarRadiation,
                                  data_2$Sensor2_AvgSolarRadiation, data_2$Sensor2_SolarRadiationStdDeviation,data_2$Sensor2_TotalSolarRadiation,
                                  data_2$Rainfall,data_2$SerialNumber,data_2$ProgramName,data_2$OperatingSystem,
                                  data_2$Tachometer_RPM, data_2$ProtocolVersion, data_2$SamplingUnitName, data_2$Latitude,
                                  data_2$Longitude, data_2$TEAMSiteName))
    colnames(data_2_new)<-c('ReferenceID','Observation','RecordID','MinimumBatteryVoltage',
                            'Sensor1_AvgTemp','Sensor1_TempStdDeviation','Sensor1_RelativeHumidity',
                            'Sensor2_AvgTemp','Sensor2_TempStdDeviation','Sensor2_RelativeHumidity',
                            'Sensor3_AvgTemp','Sensor3_TempStdDeviation','Sensor3_RelativeHumidity',
                            'Sensor1_AvgSolarRadiation','Sensor1_SolarRadiationStdDeviation','Sensor1_TotalSolarRadiation',
                            'Sensor2_AvgSolarRadiation','Sensor2_SolarRadiationStdDeviation','Sensor2_TotalSolarRadiation',
                            'Rainfall','SerialNumber','ProgramName','OperatingSystem',
                            'Tachometer_RPM','ProtocolVersion','SamplingUnitName','Latitude',
                            'Longitude','TEAMSiteName')
    ####
    data_2_new$Observation <-ymd_hms(data_2_new$Observation)
    # Combine climate 2.0 and 3.0 dataasets 
    cl_data_all = rbind(data_3,data_2_new)
    result <- cl_data_all
    sysdate <- Sys.Date()
    filename <- paste("cl_data_new",sysdate,".gzip",sep="")
    save(result, file=filename,compress="gzip")
    return(result)
    
  } else if (dataset == "vegetation") {
    # Get tree data
    tree <- dbSendQuery(con,"select * from dqa_tree")
    tree_data <- fetch(tree, n = -1)
    # Get liana data
    lianas <- dbSendQuery(con,"select * from dqa_liana")
    lianas_data <- fetch(lianas, n = -1)
    result <- list(tree = tree_data, liana = lianas_data)
    # Setup the filename with the Date. 
    sysdate = Sys.Date()
    filename= paste("veg_data",sysdate,".gzip",sep="")
    save(result, file=filename,compress="gzip")
    return(result)
  } else {
    print(paste("Incorrect dataset name. See the function instructions."))
  }
  
}
