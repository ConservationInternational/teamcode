# team_db_query.r
#
#Hello! This is different from the master version.
f.teamdb.query <- function(dataset) {
  # Database query function of TEAM production database. Returns a R Object for
  # each dataset and saves to the workspace. The function contains views or pre-
  # built queries. These should not be changed. Do not call this function 
  # repeatedly during script development. Instead run the function and load
  # the R object. This will minimize hits on the database. 
  # Example usage: ctdata <- f.teamdb.query("camera trap")
  # Args:
    # Dataset: The dataset you want. Acceptable args: "vegetation", "climate",
    # "camera trap"
  # Returns:
    # A dataframe or list with the desired datasets. 
      # Camera trap dataset: cam_trap_data R object and dataframe
      # Climate dataset: Returns a list with two dataframes in it.
        # cl_data: All climate 2.0 and 3.0 data
        # cl_temp_maxmin: The temperature maxmin associated with each TEAM Site.
        #   This is used for quality control 
      # Vegetation dataset: Returns a list wtih two dataframes in it:
        # tree: The entire tree dataset
        # liana: The entire liana dataset
  
  library(RPostgreSQL) # Need to explore RJDBC to connect with vertica
  drv <- dbDriver("PostgreSQL")
  con <- dbConnect(drv,user="readonlyuser",password="rwanda*2014",
                   dbname="team_2.0_production",port="5444",
                   host="data.team.sdsc.edu")
  
  if (dataset == "camera trap") {
    cam_trap_query <- dbSendQuery(con,"select * from dqa_ct_ordered")
    cam_trap_data <- fetch(cam_trap_query, n = -1)
    # Setup the filename with the Date. 
    sysdate = Sys.Date()
    filename= paste("ct_data",sysdate,".gzip",sep="")
    save(cam_trap_data, file=filename,compress="gzip")
    return(cam_trap_data) # Return dataframe with CT data
    
    
  } else if (dataset == "climate") {
    cl_3_data <- dbSendQuery(con,"select collection_sampling_units.id, collection_sampling_units.sampling_unit_id, collections.site_id, cl_datalog_record.id, cl_datalog_record.collection_sampling_unit_id, cl_datalog_record.collected_at::timestamp without time zone, cl_datalog_record.batt_volt_min, cl_datalog_record.airtc_avg, cl_datalog_record.airtc_2_avg, cl_datalog_record.rain_mm_tot,cl_datalog_record.temp1_keep,cl_datalog_record.temp2_keep, cl_datalog_record.temp_clean,cl_datalog_record.precip_clean,cl_datalog_record.climate_review from collection_sampling_units,collections, cl_datalog_record where collection_sampling_units.collection_id = collections.id and collection_sampling_units.id = cl_datalog_record.collection_sampling_unit_id")
    data_3 <- fetch(cl_3_data, n = -1)
    data_3["protocol"] <- 3 # Mark records as climate protocol 3
    
    #** Get Clamate 2.0 data from climate_samples table 
    cl_2_data <- dbSendQuery(con,"select collection_sampling_units.id, collection_sampling_units.sampling_unit_id, collections.site_id, climate_samples.id, climate_samples.collection_sampling_unit_id, climate_samples.collected_at, climate_samples.collected_time, climate_samples.dry_temperature as airtc_avg, climate_samples.precipitation as rain_mm_tot, climate_samples.temp1_keep,climate_samples.temp2_keep, climate_samples.temp_clean, climate_samples.precip_clean, climate_samples.climate_review from collection_sampling_units, collections, climate_samples where collection_sampling_units.collection_id = collections.id and collection_sampling_units.id = climate_samples.collection_sampling_unit_id")
    data_2 <- fetch(cl_2_data, n = -1)
    data_2["protocol"] <- 2 # Mark records as climate protocol 2
    data_2["airtc_2_avg"]<- NA # Add a new column to match what is in climate 3 data
    data_2$collected_at <- paste(data_2$collected_at,data_2$collected_time)
    colnames(data_2)[7]<- "batt_volt_min"
    data_2$batt_volt_min <- 0 # There are no battery values so climate 2.0 so make 0 and make column type = 'num'
    data_2_new <-cbind(data_2[1:8],data_2[16],data_2[9:15]) # Rearrange columns so two datasets can be easily combined. 
    
    # Combine climate 2.0 and 3.0 dataasets 
    cl_data_all = rbind(data_3,data_2_new)
    
    #  SQL to query from sites_climate_rules
    cl_rules <- dbSendQuery(con,"select * from sites_climate_rules")
    cl_temp_maxmin <- fetch(cl_rules, n = -1)
    result <- list(cl_data = cl_data_all, cl_temp_maxmin = cl_temp_maxmin)
    # Setup the filename with the Date. 
    sysdate = Sys.Date()
    filename= paste("cl_data",sysdate,".gzip",sep="")
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
