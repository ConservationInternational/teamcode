###################################
# CleanClimateData.r    9/25/2013
# This scripts cleans the climate data based upon a set of 
# rules to ensure the best climate data are made available for 
# the WPI.The file uses tables "sites_climate_rules", 
#
rm(list = ls())
library(RPostgreSQL)
###################################
# Get TEAM Site climate temperature thresholds
# from sites_climate_rules, cl_data_record, & climate_samples
# Database connection
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv,user="readonlyuser",password="rwanda*2014",dbname="team_2.0_production",port="5444",host="data.team.sdsc.edu")
#** Get climate 3.0 Data from cl_datalog_record - ADD WHERE statement to look for "WHERE cl_datalog_record.climate_review = f" to ensure we are only getting records that haven't been cleaned.
cl_3_data <- dbSendQuery(con,"select collection_sampling_units.id, collection_sampling_units.sampling_unit_id, collections.site_id, cl_datalog_record.id, cl_datalog_record.collection_sampling_unit_id, cl_datalog_record.collected_at::timestamp without time zone, cl_datalog_record.batt_volt_min, cl_datalog_record.airtc_avg, cl_datalog_record.airtc_2_avg, cl_datalog_record.rain_mm_tot,cl_datalog_record.temp1_keep,cl_datalog_record.temp2_keep, cl_datalog_record.temp_clean,cl_datalog_record.precip_clean,cl_datalog_record.climate_review from collection_sampling_units,collections, cl_datalog_record where collection_sampling_units.collection_id = collections.id and collection_sampling_units.id = cl_datalog_record.collection_sampling_unit_id")
data_3 <- fetch(cl_3_data, n = -1)
data_3["protocol"] <- 3 # Mark records as climate protocol 3

#** Get Cliamte 2.0 data from climate_samples table 
cl_2_data <- dbSendQuery(con,"select collection_sampling_units.id, collection_sampling_units.sampling_unit_id, collections.site_id, climate_samples.id, climate_samples.collection_sampling_unit_id, climate_samples.collected_at, climate_samples.collected_time, climate_samples.dry_temperature as airtc_avg, climate_samples.precipitation as rain_mm_tot, climate_samples.temp1_keep,climate_samples.temp2_keep, climate_samples.temp_clean, climate_samples.precip_clean, climate_samples.climate_review from collection_sampling_units, collections, climate_samples where collection_sampling_units.collection_id = collections.id and collection_sampling_units.id = climate_samples.collection_sampling_unit_id")
data_2 <- fetch(cl_2_data, n = -1)
data_2["protocol"] <- 2 # Mark records as climate protocol 2
data_2["airtc_2_avg"]<- NA # Add a new column to match what is in climate 3 data
data_2$collected_at <- paste(data_2$collected_at,data_2$collected_time)
colnames(data_2)[7]<- "batt_volt_min"
data_2$batt_volt_min <- 0 # There are no battery values so climate 2.0 so make 0 and make column type = 'num'
data_2_new <-cbind(data_2[1:8],data_2[16],data_2[9:15]) # Rearrange columns so two datasets can be easily combined. 

# Combine climate 2.0 and 3.0 dataasets 
data_all = rbind(data_3,data_2_new)
data_all$temp1_keep <- 0 # Set these columns to zero to more easily determine which temp values to keep
data_all$temp2_keep <- 0
#  SQL to query from sites_climate_rules
cl_rules <- dbSendQuery(con,"select * from sites_climate_rules")
cl_temp_maxmin <- fetch(cl_rules, n = -1)

###################################
# Look for Duplication Values
#   Check for duplicated values and remove them from the main dataframe
#   Creates a separate dataframe for excluded data
#  ** Need to revisit this and see what the problem is with the duplicates. Removing ~485K dups

# Put dups in a df
dups = data_all[which(duplicated(paste(data_all$collected_at,data_all$protocol,data_all$site_id,data_all$sampling_unit_id,sep=""))==TRUE),]
# Create a dup index
dup_index <- which(duplicated(paste(data_all$collected_at,data_all$protocol,data_all$site_id,data_all$sampling_unit_id,sep="")))
# Get rid of the dups out of data_all
data_all <- data_all[-dup_index,]

# **Future: add some code to grab information from calibration and verification tool tables
#
# Selects temp data that fits quality requirements. Assigns 1 for data to keep and
# 0 for data to exclude.
#   - Add a check for oscillations that could indicate problem with fan
#   - Find out at which point the fan speed/batt_volt affects temperature readings. E.g., batt_volt = 10.11
#
#** Differentiate between climate 2.0 w/o battery and climate 3.0 with battery
# Determine which sites we have climate data for
p_unique_sites <- data.frame(site_id = unique(data_all$site_id))
# get the maxmins for these sites
maxmin_sites <- subset(cl_temp_maxmin,cl_temp_maxmin$site_id %in% p_unique_sites$site_id)

for (i in 1:nrow(maxmin_sites)) {
  #** Run this for climate 2.0 and then for 3.0 and check counts...make sure there are aren't measurements that are same for 2.0 and 3.0
  
  # Climate 2.0 data
  # Establish site_index -- Need to update data_all as 
  site_index <- which(data_all$site_id == maxmin_sites$site_id[i] & data_all$protocol == 2)
  #site_data <- data_all[which(data_all$site_id == maxmin_sites$site_id[i] & data_all$protocol == 2),]
  temp1_want <- which(data_all$airtc_avg[site_index] >= cl_temp_maxmin$temp_min[i] & data_all$airtc_avg[site_index] <= cl_temp_maxmin$temp_max[i]) 
  data_all$temp1_keep[site_index[temp1_want]]<-1
  
  # Climate 3.0 data
  site_index <- which(data_all$site_id == maxmin_sites$site_id[i] & data_all$protocol == 3)
  
  temp1_want <- which(data_all$batt[site_index] >= 11.2 & data_all$airtc_avg[site_index] >= cl_temp_maxmin$temp_min[i] & data_all$airtc_avg[site_index] <= cl_temp_maxmin$temp_max[i])
  temp2_want <- which(data_all$batt[site_index] >= 11.2 & data_all$airtc_2_avg[site_index] >= cl_temp_maxmin$temp_min[i] & data_all$airtc_2_avg[site_index] <= cl_temp_maxmin$temp_max[i])

  data_all$temp1_keep[site_index[temp1_want]]<-1
  data_all$temp2_keep[site_index[temp2_want]]<-1
}
#
# Transform into one temp value that can be used in data products
data_all$both <- data_all$temp1_keep + data_all$temp2_keep # Possible values equal 2,1,0
# Determine temp_clean when 1 temperature value present
# Include temp values where there is one temperate value
# First situations where temp1_keep = 0  so include airtc_2_avg
temp1_index <- which(data_all$both == 1 & data_all$temp1_keep == 0)
data_all$temp_clean[which(data_all$both == 1 & data_all$temp1_keep == 0)] <- data_all$airtc_2_avg[which(data_all$both == 1 & data_all$temp1_keep == 0)]
# Next situations where temp2_keep = 0  so include airtc_avg
temp2_index <-which(data_all$both == 1 & data_all$temp2_keep == 0)
data_all$temp_clean[which(data_all$both == 1 & data_all$temp2_keep == 0)] <- data_all$airtc_avg[which(data_all$both == 1 & data_all$temp2_keep == 0)]


# Determine temp_clean when 2 temperature values present
# # Want temp values that are less than 1 degree apart. Get their mean.
# If more than one degree apart exclude them
#df= data_all[which(data_all$both == 2 & (abs(data_all$airtc_avg - data_all$airtc_2_avg) <= 1)),]
two_temp_index = which(data_all$both == 2 & (abs(data_all$airtc_avg - data_all$airtc_2_avg) <= 1))
# Add the temperatue values that are less than 1 degree apart to temp_clean
data_all$temp_clean[two_temp_index] = rowMeans(data_all[two_temp_index,c('airtc_avg','airtc_2_avg')]) # Get the mean of airtc_avg and airtc_2_avg 


# Determine all rain values that are > 0 and put them into data_$all$precip_clean
# Create index of all rain values > 0
rain_want_index <- which(data_all$rain_mm_tot >= 0)
# Put these values in data_all$precip_clean
data_all$precip_clean[rain_want_index] <-data_all$rain_mm_tot[rain_want_index]

# Change all NAs to NULL
data_all$temp_clean[which(is.na(data_all$temp_clean))] <- "NULL" # This converts the column to a char
data_all$precip_clean[which(is.na(data_all$precip_clean))] <- "NULL" # This converts the column to a char

# split data back into protocol 2 and 3 to submit to climate_samples and cl_datalog_record
cl_samples = data_all[which(data_all$protocol == 2),]
cl_data_log = data_all[which(data_all$protocol == 3),]
# SET CLIMATE REVIEW to TRUE? Do this after ready and have all duplicates figured out.

# Save the dataframe for both climate 2.0 and 3.0 to temporary tables
a <- dbWriteTable(con,"cl_temp_datalog",cl_data_log)
b <- dbWriteTable(con,"cl_temp_samples",cl_samples)

#** Running these updates manually from within PG. Need to incorporate into R script here. 
# Update climate 3.0 -- cl_datalog_record
UPDATE cl_datalog_record AS a SET
temp1_keep = b.temp1_keep, temp2_keep = b.temp2_keep,temp_clean = b.temp_clean::float, precip_clean = b.precip_clean::float,
climate_review = b.climate_review FROM cl_temp_datalog AS b WHERE a.id = b."id.1"

# Update Climate 2.0 -- climate_samples
UPDATE climate_samples AS a SET
temp1_keep = b.temp1_keep, temp2_keep = b.temp2_keep,temp_clean = b.temp_clean::float, precip_clean = b.precip_clean::float,
climate_review = b.climate_review FROM cl_temp_samples AS b WHERE a.id = b."id.1"

# Clean up and drop temporary tables
drop_sql = paste("drop table cl_temp_data_log,cl_temp_samples")
#a <-dbSendQuery(con,drop_sql) 
###########################
# END SCRIPT
###########################
# SQl to do updates - determined too slow to update database in this manner
#for (i in 1:nrow(cl_samples)) {
#  update_sql <- paste("UPDATE climate_samples SET precip_clean=",cl_samples$precip_clean[i],",temp_clean=",cl_samples$temp_clean[i]," where id=",cl_samples$id.1[i])
#  a <-dbSendQuery(con,update_sql)      
#}
#for (b in 994:nrow(cl_data_log)) {
# update_sql <- paste("UPDATE cl_datalog_record SET precip_clean=",cl_data_log$precip_clean[b],",temp_clean=",cl_data_log$temp_clean[b]," where id=",cl_data_log$id.1[b])
# dbSendQuery(con,update_sql) 
#  print(b)
#}
###
# Also too slow while preparing larger db prepartion statement
#update_sql <-vector()#361246
#for (b in 1:nrow(cl_data_log)) {
# update_sql <- paste("UPDATE cl_datalog_record SET precip_clean=",cl_data_log$precip_clean[b],",temp_clean=",cl_data_log$temp_clean[b]," where id=",cl_data_log$id.1[b])
#}
#dbSendQuery(con,update_sql) 


##################
# #Write Results to following tables ##
# cl_datalog_record
#     - temp1_keep
#     - temp2_keep
#     - temp_clean
#     - precip_clean
#     - climate_review
#####
# climate_samples
#     - temp1_keep
#     - temp2_keep
#     - temp_clean
#     - precip_clean
#     - climate_review
#####################
## Determine information to go back into climate_samples & cl_datalog_record
## SQL to insert new values into these columns. use data_all$id.1 but first subset by protocol. ID's in
## in climate_samples and cl_datalog_record are NOT unique between tables.
## Close databse connections
