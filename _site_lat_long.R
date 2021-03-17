
# Adapted from script developed by Ian Fellows (ymx4@cdc.gov)
# https://github.com/ICPI/Denominators/blob/master/YODA/R/datim_geo.R


ldpkg <- dget("ldpkg.R")

ldpkg(c("tidyverse",
"readr",
"zoo",
"tidytext",
"rjson",
"jsonlite",
"data.table",
"tidyverse",
"geojsonio",
"geojson",
"sf",
"httr"))


# Code to Merge MER data with COVID time series 
library(tidyverse)
library(readr)
library(zoo)
library(tidytext)
library(rjson)
library(jsonlite)
library(data.table)
library(tidyverse)
library(geojsonio)
library(geojson)
library(sf)
library(httr)


library(devtools)
install_github(repo = "https://github.com/pepfar-datim/datapackr.git", ref = "master")
library(datapackr)


# country <- "Botswana"

datim_logged_in <<- loginToDATIM()
# https://www.datim.org/

datim_get_locations <- function(country){
  
  # countries <-
  #   datapackr::api_call("organisationUnits") %>%
  #   datapackr::api_filter(field = "organisationUnitGroups.id",
  #                         operation = "eq",
  #                         match = "cNzfcPWEGSH") %>%
  #   datapackr::api_fields(fields = "id,name") 
  

    datim_countries <- api_call("organisationUnits") %>%
    datapackr::api_filter(field = "organisationUnitGroups.name",
                          operation = "eq",
                          match = "Country") %>% 
    # api_filter("organisationUnits.name:eq:Country") %>%
    api_get()

  country_uid <- datim_countries[tolower(datim_countries$displayName) == tolower(country),1]
  
  locations <- datapackr::api_call("organisationUnits") %>%
    datapackr::api_fields("id,geometry,organisationUnitGroups,displayName") %>%
    datapackr::api_filter(field = "ancestors.id",
                          operation = "eq",
                          match = country_uid) %>% 
    datapackr::api_get()
  
  unit_groups <- api_call("organisationUnitGroups") %>%
    api_get()  
  
  ug <- sapply(locations$organisationUnitGroups, function(x) x[[1]][1])
  locations$unit_group_1 <- unit_groups$displayName[match(ug, unit_groups$id)]
  ug <- sapply(locations$organisationUnitGroups, function(x) x[[1]][2])
  locations$unit_group_2 <- unit_groups$displayName[match(ug, unit_groups$id)]
  ug <- sapply(locations$organisationUnitGroups, function(x) x[[1]][3])
  locations$unit_group_3 <- unit_groups$displayName[match(ug, unit_groups$id)]
  
  locations$latitude <- NA
  locations$longitude <- NA
  for(i in 1:nrow(locations)){
    coord <- locations$geometry.coordinates[[i]]
    coord_type <- locations$geometry.type[[i]]
    if(is.null(coord) || is.na(coord) || is.na(coord_type))
      next
    if(is.list(coord) && coord_type != "MultiPolygon"){
      #message("Coercing to MultiPolygon")
      coord_type <- "MultiPolygon"
    }
    if(!is.list(coord) && coord_type == "MultiPolygon"){
      #message("Coercing away from MultiPolygon")
      if(length(dim(coord)) == 3)
        coord_type <- "Polygon"
      else
        coord_type <- "Point"
    }
    if(coord_type == "Polygon"){
      if(length(dim(coord)) == 3) 
        cc <- coord[1, , ]
      coord <- cc %>% 
        as.data.frame() %>% 
        geojsonio::geojson_list(lon = "V1",lat="V2") %>% 
        geojsonio::geojson_sf() %>% 
        st_coordinates() %>% 
        st_linestring() %>% 
        st_centroid() %>% 
        st_coordinates() %>%
        as.numeric()
    }else if(coord_type == "MultiPolygon"){
      coord <- rlang::flatten(coord)
      c1 <- list()
      for(j in 1:length(coord)){
        cc <- coord[[j]]
        if(length(dim(cc)) == 3) 
          cc <- cc[1, , ]
        #tryCatch({
        c1[[j]] <- cc %>% as.data.frame() %>% 
          geojsonio::geojson_list(lon = "V1",lat="V2") %>% 
          geojsonio::geojson_sf() %>% 
          st_coordinates() %>% 
          st_linestring() %>% 
          st_centroid() %>% 
          st_coordinates() %>%
          as.numeric()
        #}, error= function(xx) browser())
      }
      coord <- c(median(as.numeric(as.data.frame(c1)[1,])),
                 median(as.numeric(as.data.frame(c1)[2,])))
    }else if(coord_type != "Point"){
      warning(paste("Unknown geometry",coord_type))
      next
    }
      
    locations$longitude[i] <- coord[1]
    locations$latitude[i] <- coord[2]
  }
  locations
}


# dat_analysis <- datim

site_locations <- function(dat_analysis, locations){
  locations <- locations %>% filter(!is.na(latitude), !is.na(longitude))
  da_locations <- dat_analysis %>% 
    group_by(orgunituid, 
      sitename,
      facilityuid,
      psnuuid,
      communityuid) %>% 
    summarise(n=n()) %>% select(-n)
  da_locations$longitude <- NA
  da_locations$latitude <- NA
  da_locations$location_mode <- NA
  
  # Find best location match
  for(i in 1:nrow(da_locations)){
    s <- locations[locations$id == da_locations$facilityuid[i],]
    if(is.na(da_locations$facilityuid[i]))
      next
    if(nrow(s) > 0){
      if(nrow(s)!= 1) browser()
      da_locations$longitude[i] <- s$longitude
      da_locations$latitude[i] <- s$latitude
      da_locations$location_mode[i] <- "facilityuid"
      next
    }
    s <- locations[locations$displayName == da_locations$sitename[i],]
    if(nrow(s) == 1){
      da_locations$longitude[i] <- s$longitude
      da_locations$latitude[i] <- s$latitude
      da_locations$location_mode[i] <- "sitename"
      next
    }
    s <- locations[locations$id == da_locations$communityuid[i],]
    if(nrow(s) > 0){
      da_locations$longitude[i] <- s$longitude
      da_locations$latitude[i] <- s$latitude
      da_locations$location_mode[i] <- "communityuid"
      next
    }
    s <- locations[locations$id == da_locations$psnuuid[i],]
    if(nrow(s) > 0){
      da_locations$longitude[i] <- s$longitude
      da_locations$latitude[i] <- s$latitude
      da_locations$location_mode[i] <- "psnuuid"
      next
    }
  }
  
  # if no location data on psnu/community/facility use median of known locations in the area
  da_locations_comp <- na.omit(as.data.frame(da_locations))
  for(i in 1:nrow(da_locations)){
    if(!is.na(da_locations$latitude[i]))
      next
    s <- da_locations_comp[da_locations_comp$communityuid == da_locations$communityuid[i],]
    if(nrow(s) > 0){
      da_locations$longitude[i] <- median(s$longitude)
      da_locations$latitude[i] <- median(s$latitude)
      da_locations$location_mode[i] <- "median_communityuid"
      next
    }
    
    s <- da_locations_comp[da_locations_comp$psnuuid == da_locations$psnuuid[i],]
    if(nrow(s) > 0){
      da_locations$longitude[i] <- median(s$longitude)
      da_locations$latitude[i] <- median(s$latitude)
      da_locations$location_mode[i] <- "median_psnuuid"
      next
    }
  }
  
  # fall back to country median
  miss <- is.na(da_locations$latitude)
  if(any(miss)){
    da_locations$longitude[miss] <- median(da_locations_comp$longitude)
    da_locations$latitude[miss] <- median(da_locations_comp$latitude)
    da_locations$location_mode[miss] <- "median_country"
  }
  da_locations
}





# Creating the 'not in' function
`%ni%` <- Negate(`%in%`) 


# File paths
FY<-"20"
quarter<-"Q4"
devprod<-"PROD"
msd_period<-"Postclean"
#dir<-"C:/Users/ouo8/OneDrive - CDC/MEDAB/Country Dashboards/Q3 Dashboards/"

# Date ranges
start_date <- as.Date("2019-10-01")
end_date   <- as.Date(Sys.Date())

start_datex <- "2019-10-01"
end_datex   <- as.character(Sys.Date())



#filepaths
main.dir<- ifelse(dir.exists(paste0("C:/Users/",
  Sys.getenv("USERNAME"),"/CDC/CPME - Country_Dashboards/","FY",FY,"/",
                                    quarter,"/")),
                  paste0("C:/Users/",
                    Sys.getenv("USERNAME"),"/CDC/CPME - Country_Dashboards/","FY",FY,"/",
                         quarter,"/"),
                  "Directory does not exist")

codedir<-paste(main.dir,devprod,"Code/",sep="/")
outmer <- paste(main.dir,devprod,"Output/",sep="/")
ffolder <- paste(main.dir,"MSDs",msd_period,sep="/")

msdlist <- list.files(ffolder, pattern="Site_IM")
msd_name<-msdlist[3]

input<-ffolder

#indicators Looking only at Treatment and Prevention indicators
indlist<-c(
  # "HTS_TST",
  # "HTS_TST_POS",
  "TX_CURR",
  "TX_ML",
  "TX_NET_NEW",
  "TX_PVLS",
  "TX_NEW",
  "TX_RTT",
  # "SC_ARVDISP",
  # "SC_STOCK",
  # "SC_CURR",
  "PrEP_NEW",
  "PrEP_CURR"  )


#get column names
msd_name_full <- paste(input,msd_name,sep="/")
coviddir <- "C:/Users/lrz5/CDC/CPME - Country_Dashboards/MER_COVID_analysis/RawData"

# unzip(msd_name_full, 
#   exdir = coviddir)

zipname <- unzip(msd_name_full, list=T) %>% .$Name

foo <- fread(file=paste(coviddir,zipname,sep="/"), nrows = 0)
foonames <- tolower(names(foo))
# colvecx <- as.vector(ifelse(grepl("qtr|targets|cumulative", foonames), "d", "c"))
colvecx <- as.vector(ifelse(grepl("qtr|targets|cumulative", foonames), "double", "character"))
colvec <- paste(colvecx, collapse = '')

oufiles <- msdlist

# Function to pull in site-level dataset
coord_pull <- function(k) {
  
msd_namex <- paste(input, k, sep="/")

# unzip(msd_namex, 
#   exdir = coviddir)

zipnamex <- unzip(msd_namex, list=T) %>% .$Name

#pull in MSD, filter to just the indicators and standarddisaggs we want, and pivot to wide by indicator and long by period
datim <- fread(file=paste(coviddir,zipnamex,sep="/"), colClasses=colvecx)
  
countrynames <- unique(datim$countryname)

locx <- purrr::map(.x = countrynames, .f = ~datim_get_locations(.x))
locatx <- dplyr::bind_rows(locx)

# polygonx <- locatx %>% filter(geometry.type=="Polygon")

# Creating final data frame
flocx <- site_locations(datim, locatx)

return(flocx)
}

location_list1 <- purrr::map(.x = oufiles[1:14], .f = ~coord_pull(.x))
location_list2 <- purrr::map(.x = oufiles[15:18], .f = ~coord_pull(.x))
location_list3 <- purrr::map(.x = oufiles[20:21], .f = ~coord_pull(.x))
location_list4 <- purrr::map(.x = oufiles[22], .f = ~coord_pull(.x))
location_list5 <- purrr::map(.x = oufiles[23], .f = ~coord_pull(.x))
location_list6 <- purrr::map(.x = oufiles[24:26], .f = ~coord_pull(.x))
location_list7 <- purrr::map(.x = oufiles[27], .f = ~coord_pull(.x))
location_list8 <- purrr::map(.x = oufiles[28], .f = ~coord_pull(.x))
location_list9 <- purrr::map(.x = oufiles[19], .f = ~coord_pull(.x))


lat_long_df <- dplyr::bind_rows(location_list1,
                                location_list2,
                                location_list3,
                                location_list4,
                                location_list5,
                                # location_list6,
                                location_list7,
                                location_list8,
                                location_list9)
                                
  
# Save an object to a file
saveRDS(lat_long_df, file = "latlong.rds")
# Restore the object
testx <- readRDS(file = "latlong.rds")

# Pulling in COVID-related time-series data
# COVID code file path
covid.dir<- ifelse(dir.exists(paste0("C:/Users/",
  Sys.getenv("USERNAME"),
  "/CDC/CPME - Country_Dashboards/MER_COVID_analysis/")),
                  paste0("C:/Users/",
                    Sys.getenv("USERNAME"),
                    "/CDC/CPME - Country_Dashboards/MER_COVID_analysis/"),
                  "Directory does not exist")
outdir <- paste0(covid.dir, "output/")


# Output lat_long data
fwrite(lat_long_df, paste0(outdir,"lat_long.txt"),na="")



