# TV_editor_events.r
# 

rm(list = ls())
require(lubridate)
require(ggplot2)
library(RPostgreSQL) # Need to explore RJDBC to connect with vertica

# SQL to update tv_photo table with events_all column. This colun will store
# all the events. event_id is what is used by the Taxonomic Editor app and can
# be assigned to experts.

# Load R Object created directly from DB or replace by query to database.
load("ct_data2014-04-02.gzip")
# Small Dataset
#animals = cam_trap_data[which(cam_trap_data$Photo.Type == 'Animal' & cam_trap_data$Site.Name == 'Volcán Barva'),]


# Order data by: Smapling Period, Sampling unit name and Photo Taken Time
## Temp code to to generate data for one site
order_data <- f.order.data(animals) # Small dataset for testing
#order_data <- f.order.data(cam_trap_data)
# Seperate into events
#CAUTION: this function removes records that are NOT images (e.g. Sampling Date records)
data1<-f.separate.events(order_data,5) #the entire grp column  is what makes it unique
# Save data1 so don't need to rerun separate events.
#save(data1,file="data1.gzip",compress="gzip")
# Create small subset for viewing
view_data <- data.frame(data1$Site.Name,data1$Sampling.Period,data1$Sampling.Unit.Name,data1$Photo.Taken.Time,data1$grp)

drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv,user="teamuser",password="",
                 dbname="team_2.0_devel",port="5444",
                 host="data.team.sdsc.edu")

# Will need to explore whether we want to include already reviewed images/events.

for (i in 1:nrow(data1)) {
  update_sql <- paste("UPDATE tv_photo SET events_all=",shQuote(data1$grp[i])," where id=",data1$ID[i])
  a <-dbSendQuery(con,update_sql)  
  i
}

# Start from loading ct_groups or run from the beginning
#load("ct_groups")
# Create a new column with Family, Genus, Species
data1$sp_all <- paste(data1$Family,data1$Genus,data1$Species)
# Crate a new column to store the random sample of events that will be used in 
# taxonomic editor
data1[,'event'] <- NA 
sites <- unique(data1$Site.Name)
# Create a dataframe to store the tv_photo.ID and the event_id's to be used in the taxonomic app.
final_event_df<- data.frame(final_event=character(0))
for (i in 1:length(unique(data1$Site.Name))) {
  site_index <- which(data1$Site.Name == sites[i]) 
  length(site_index)
  temp_sp_unique <- unique(paste(data1$Family[site_index],data1$Genus[site_index],data1$Species[site_index]))
  for (j in 1: length(temp_sp_unique)){ 
    #Data by TEAM Site by species
    site_data <- data1[site_index,]
    sp_by_site <- site_data[which(temp_sp_unique[j] == site_data$sp_all),]
    # Get events, determine if there are 50 or more and insert into database
    num_events = length(unique(sp_by_site$grp))
    # Get a list of unique events to select from if more than 50 or assign
    # to the editor if less than 50
    unique_events = unique(sp_by_site$grp)
    if (num_events > 50) {
      # Get a random sample w/o replacement
      event_index <- sample(1:num_events,50 , replace=F)
      final_event <- unique_events[event_index]
    } else {
      final_event <- unique(sp_by_site$grp)
    }
    final_event_df <-rbind(final_event_df,data.frame(final_event=final_event))
  }
}
# SQL to insert the final events into the tv_photo table. This will add the event
# to the event_id as selected in the final_event_df dataframe.
for (i in 1:nrow(final_event_df)) {
  update_sql <- paste("UPDATE tv_photo SET event_id=",
                      shQuote(final_event_df$final_event[i])," where events_all=",
                      shQuote(final_event_df$final_event[i]))
  a <-dbSendQuery(con,update_sql)      
}
Sys.time()
