# TV_editor_events.r
# 

rm(list = ls())
require(lubridate)
require(ggplot2)

# SQL to update tv_photo table with events_all column. This colun will store
# all the events. event_id is what is used by the Taxonomic Editor app and can
# be assigned to experts.
#ALTER TABLE tv_photo ADD COLUMN events_all VARCHAR(50);


# WRITE CODE TO QUERY DATBASE fro team_db_query.r
# Load R Object created directly from DB
load("../../ct_data2014-02-27.gzip")
# Small Dataset
animals = cam_trap_data[which(cam_trap_data$Photo.Type == 'Animal' & cam_trap_data$Site.Name == 'Volcán Barva'),]


## Jorge's code
# Order data by: Smapling Period, Sampling unit name and Photo Taken Time
## Temp code to to generate data for one site
order_data <- f.order.data(animals) # Small dataset for testing
#order_data <- f.order.data(cam_trap_data)
# Seperate into events
#CAUTION: this function removes records that are NOT images (e.g. Sampling Date records)
data1<-f.separate.events(order_data,5) #the entire grp column is what makes it unique
# Create small subset for viewing
view_data <- data.frame(data1$Site.Name,data1$Sampling.Period,data1$Sampling.Unit.Name,data1$Photo.Taken.Time,data1$grp)

# Will need to explore whether we want to include already reviewed images/events.

# UPDATE DATABASE WITH ALL THE EVENT IDS....

# Start from loading ct_groups or run from the beginning
load("ct_groups")
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
    # Update DATABASE statement for tv_photo.event_id OR BUILD a final list and
    # then insert into the database
    }
}
# SQL TO UPDATE BASED ON MATCHING event_id for selected events


### Jimmy's code
animals_subset <- data.frame(animals$Site.Name,animals$Sampling.Unit.Name,animals$Sampling.Period,animals$Photo.Taken.Time,paste(animals$Genus,animals$Species))
colnames(animals_subset) <- c("Site.Name", "Samp.Unit","Samp.Period", "Photo.Taken.Time","Genus.Species")

order_dataset <- animals_subset[order(animals_subset$Samp.Period,animals_subset$Samp.Unit,animals_subset$Genus.Species,animals_subset$Photo.Taken.Time),]
order_dataset$diff <- c(diff(order_dataset$Photo.Taken.Time), 0)

event_data <- f.group.events(order_dataset)


 

#First order the data frame by sampling unit name and time:
#indx<-order(cam_trap_data$Site.name,cam_trap_data$Sampling.Unit.Name,cam_trap_data$Photo.Taken.Time )
#x <- order(cam_trap_data$Sampling.Unit.Name,cam_trap_data$Photo.Taken.Time)

#a <- which(cam_trap_data$Sampling.Unit.Name == 'CT-BBS-1-01')

#sort(unique(paste(cam_trap_data$Sampling.Period,cam_trap_data$Sampling.Unit.Name)))
#Code f.separate.events(data,thresh) classifies photos in a separate series of events 
#that are thresh mins apart. 
#this generates a numeric code for each event (column "event"). If different species are photographed < thres min away, 
#then they will have the same code. But that is OK, since species will be analyzed separately for abundance,
#occupancy etc.
#This creates a unique group id in the last column.
#CAUTION: this function removes records that are NOT images (e.g. Sampling Date records)
data1<-f.separate.events(data,5)
#species <- unique(data$bin)
#events.by.species2 <- by(data=data, INDICES=data$bin, FUN=list)
#CREATE LIST OF EVENTS BY SPECIES
events.by.species <- tapply(X=data$grp, INDEX=data$bin, FUN=list)
#CREATE EMPTY DATA FRAME TO COUNT NUMBER OF EVENTS PER SPECIES
num.events <- data.frame()
#COUNTS THE NUMBER OF EVENTS PER SPECIES
for(i in 1:length(events.by.species)) {
  num.events[i, 'num.events'] <- length(events.by.species[[i]])
}
#ADD SPECIES NAMES TO THE DATA FRAME WITH THE EVENT COUNT
num.events$species <- names(events.by.species)

#UNIDENTIFIABLE IMAGES DO NOT HAVE SPECIES IN THEM SO FILL IN EMPTY SPACE WITH UNIDENTIFIABLE
num.events$species[1] <- 'Unidentifiable'

#RENAME THE ROWS WITH THE SPECIES NAMES (NOT REALLY NECESSARY)
#rownames(num.events) <- names(events.by.species)
#SORT THE DATA FRAME BY THE NUMBER OF EVENTS PER SPECIES
num.events<-num.events[order(num.events$num.events, decreasing=T), ]

#ASSIGN A FACTOR VALUE SO GRAPH RESULTS CAN BE ORDERED
num.events$factor <- factor(num.events$species, as.character(num.events$species))

#CONVERT THE NUMBER OF EVENTS TO LOG NUMBER OF EVENTS TO HELP VISUALIZE
num.events$logs <- log(num.events$num.events)

#PLOT THE DATA WITHOUT LOG VALUES (GREEN = 1000, BLUE = 500)
ggplot(data=num.events, aes(x = factor, y = num.events)) + geom_bar(stat = 'identity') +
  theme(axis.text.x = element_text(angle = 90, hjust = 1, vjust = 0.25)) + ggtitle(as.character(data$Site.Name[1])) +
  geom_hline(yintercept=1000, color='green') + geom_hline(yintercept=500, color='blue') #+
  #geom_hline(yintercept=250, color = 'red') + geom_hline(yintercept=125, color = 'yellow')

#PLOT THE DATA WITH LOG VALUES (GREEN = 100, BLUE = 50, RED = 25)
ggplot(data=num.events, aes(x=factor, y=logs)) + geom_bar(stat='identity') +
  theme(axis.text.x=element_text(angle=90, hjust=1, vjust=0.25)) + ggtitle(as.character(data$Site.Name[1])) +
  geom_hline(yintercept=4.60517, color='green') + geom_hline(yintercept=3.912023, color='blue') +
  geom_hline(yintercept=3.218876, color='red')

###############################################
#CREATE EMPTY DATA FRAME TO COUNT NUMBER OF EVENTS PER SPECIES
num.events <- data.frame()
#BREAK DOWN DATA BY SITE
data.by.site <- by(data=data, INDICES=data$Site.Name, FUN=list)
for(j in 1:length(data.by.site)) {
  events.by.species[[j]] <- tapply(X=data.by.site[[j]]$grp, INDEX=data.by.site[[j]]$bin, FUN=list)
}
#COUNTS THE NUMBER OF EVENTS PER SPECIES
for(i in 1:length(events.by.species)) {
  num.events[i, 'num.events'] <- length(events.by.species[[i]])
}