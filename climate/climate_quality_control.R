rm(list = ls())
load("cl_data_new2014-04-24.gzip")


# Add columns to store Pass/Fails from Tests
cl_data_all$StepRH1 <-NA
cl_data_all$StepRH2 <-NA
cl_data_all$StepRH3 <-NA
cl_data_all$StepSR1 <-NA
cl_data_all$StepSR2 <-NA
cl_data_all$StepT1 <-NA
cl_data_all$StepT2 <-NA
cl_data_all$StepT3 <-NA
cl_data_all$StepT4 <-NA
cl_data_all$StepT5 <-NA
cl_data_all$Sensor1_RelativeHumidity <-as.numeric(cl_data_all$Sensor1_RelativeHumidity)
cl_data_all$Sensor2_RelativeHumidity <-as.numeric(cl_data_all$Sensor2_RelativeHumidity)
cl_data_all$Sensor3_RelativeHumidity <-as.numeric(cl_data_all$Sensor3_RelativeHumidity)
cl_data_all$Sensor1_AvgTemp <-as.numeric(cl_data_all$Sensor1_AvgTemp)
cl_data_all$Sensor2_AvgTemp <-as.numeric(cl_data_all$Sensor2_AvgTemp)
cl_data_all$Sensor3_AvgTemp <-as.numeric(cl_data_all$Sensor3_AvgTemp)
cl_data_all$Sensor1_TotalSolarRadiation <-as.numeric(cl_data_all$Sensor1_TotalSolarRadiation)
cl_data_all$Sensor2_TotalSolarRadiation <-as.numeric(cl_data_all$Sensor2_TotalSolarRadiation)






# Get a unique list of sites and loop through them
unique_sites <- unique(cl_data_all$TEAMSiteName)
for (i in 1:1) { #length(unqiue(cl_data_all$TEAMSiteName)) {
  site_data = cl_data_all[which(cl_data_all$TEAMSiteName == unique_sites[i]),]
  unique_stations <- unique(paste(site_data$SamplingUnitName))
  # get a unique list of stations..handle sites that have more than one station
  for (j in 1:1) { #lengh(unique_stations)) {
    # if more than 1 do something
    station_data = site_data
    # Perform tests on a particular climate station data.
    for (k in 1:nrow(station_data)) {
      # Find records 30 minutes,1 hour, 2hour, 3hour, 6hour and 12 hours ahead.
      new_date = station_data$Observation[k] + 60*30
      new_date_hour = station_data$Observation[k] + 60*60
      new_date_2hour = station_data$Observation[k] + 2*60*60
      new_date_3hour = station_data$Observation[k] + 3*60*60
      new_date_6hour = station_data$Observation[k] + 6*60*60
      new_date_12hour = station_data$Observation[k] + 12*60*60
      # Create thirty min index and check to make sure it exists..
      thirty_min_index <-which(station_data$Observation == new_date) 
      if (length(thirty_min_index) == 0) {
        thirty_min_index <- 'a'
      }
      sixty_min_index <- which(station_data$Observation == new_date_hour)
      if (length(sixty_min_index) == 0) {
        sixty_min_index <- 'a'
      }
      two_hour_index <- which(station_data$Observation == new_date_2hour)
      if (length(two_hour_index) == 0) {
        two_hour_index <- 'a'
      }
      three_hour_index <- which(station_data$Observation == new_date_3hour)
      if (length(three_hour_index) == 0) {
        three_hour_index <- 'a'
      }
      six_hour_index <- which(station_data$Observation == new_date_6hour)
      if (length(six_hour_index) == 0) {
        six_hour_index <- 'a'
      }
      twelve_hour_index <- which(station_data$Observation == new_date_12hour)
      if (length(twelve_hour_index) == 0) {
        twelve_hour_index <- 'a'
      }
      
      
      # Sensor 1
      # RH - a value of 1 is equal to fail.
      if(is.numeric(station_data$Sensor1_RelativeHumidity[k]) & is.numeric(station_data$Sensor1_RelativeHumidity[thirty_min_index])) {
        station_data$StepRH1[k] <- ifelse (station_data$Sensor1_RelativeHumidity[thirty_min_index] - 
                                             station_data$Sensor1_RelativeHumidity[k] > 45,1,0)
      }
      # Solar radiation 
      # What are our units?
      if(is.numeric(station_data$Sensor1_TotalSolarRadiation[k]) & is.numeric(station_data$Sensor1_TotalSolarRadiation[sixty_min_index])) {
        station_data$StepSR1[k] <- ifelse (station_data$Sensor1_TotalSolarRadiation[sixty_min_index] - 
                                             station_data$Sensor1_TotalSolarRadiation[k] >= 0 & station_data$Sensor1_TotalSolarRadiation[sixty_min_index] - 
                                             station_data$Sensor1_TotalSolarRadiation[k] < 555,1,0)
      }
      # Temperature
      # 1 hour
      if(is.numeric(station_data$Sensor1_AvgTemp[k]) & is.numeric(station_data$Sensor1_AvgTemp[sixty_min_index])) {
        station_data$StepT1[k] <- ifelse (station_data$Sensor1_AvgTemp[sixty_min_index] - 
                                            station_data$Sensor1_AvgTemp[k] >= 4,1,0)
      }
      # 2hour
      if(is.numeric(station_data$Sensor1_AvgTemp[k]) & is.numeric(station_data$Sensor1_AvgTemp[two_hour_index])) {
        station_data$StepT2[k] <- ifelse (station_data$Sensor1_AvgTemp[two_hour_index] - 
                                            station_data$Sensor1_AvgTemp[k] >= 7,1,0)
      }
      # 3hour
      if(is.numeric(station_data$Sensor1_AvgTemp[k]) & is.numeric(station_data$Sensor1_AvgTemp[three_hour_index])) {
        station_data$StepT3[k] <- ifelse (station_data$Sensor1_AvgTemp[three_hour_index] - 
                                            station_data$Sensor1_AvgTemp[k] >= 9,1,0)
      }
      #6 hour
      if(is.numeric(station_data$Sensor1_AvgTemp[k]) & is.numeric(station_data$Sensor1_AvgTemp[six_hour_index])) {
        station_data$StepT4[k] <- ifelse (station_data$Sensor1_AvgTemp[six_hour_index] - 
                                            station_data$Sensor1_AvgTemp[k] >= 15,1,0)
      }
      #12 hour
      if(is.numeric(station_data$Sensor1_AvgTemp[k]) & is.numeric(station_data$Sensor1_AvgTemp[twelve_hour_index])) {
        station_data$StepT5[k] <- ifelse (station_data$Sensor1_AvgTemp[twelve_hour_index] - 
                                            station_data$Sensor1_AvgTemp[k] >= 25,1,0)
      }
      ###############
      # Sensor 2
      # RH
      if(is.numeric(station_data$Sensor2_RelativeHumidity[k]) & is.numeric(station_data$Sensor2_RelativeHumidity[thirty_min_index])) {
        station_data$StepRH2[k] <- ifelse (station_data$Sensor2_RelativeHumidity[thirty_min_index] - 
                                             station_data$Sensor2_RelativeHumidity[k] > 45,1,0)
      }
      # Sensor 3
      # RH
      if(is.numeric(station_data$Sensor3_RelativeHumidity[k]) & is.numeric(station_data$Sensor3_RelativeHumidity[thirty_min_index])) {
        station_data$StepRH3[k] <- ifelse (station_data$Sensor3_RelativeHumidity[thirty_min_index] - 
                                             station_data$Sensor3_RelativeHumidity[k] > 45,1,0)
      }
    }
    
  }
}
