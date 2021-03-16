# Script pulls in various COVID related data streams and integrates them with MER data
# Data streams include:
# MER (DATIM)
# MER Narratives (DATIM)
# COVID cases and deaths (WHO|JHU)
# COVID mitigation measures (WHO)
# COVID stringency index (Oxford)
# Latitude and Longitude data for PEPFAR facilites (DATIM)
#
# Script developed by: Imran Mujawar (lrz5@cdc.gov)
# Date: 03/16/2021



# Code to Merge MER data with COVID time series 

ldpkg <- dget("ldpkg.R")

ldpkg(c(
"tidyverse",
"readr",
"zoo",
"tidytext",
"rjson",
"jsonlite",
"data.table"
))

library(tidyverse)
library(readr)
library(zoo)
library(tidytext)
library(rjson)
library(jsonlite)
library(data.table)

# Creating the 'not in' function
`%ni%` <- Negate(`%in%`) 


# File paths
FY<-"21"
quarter<-"Q1"
devprod<-"PROD"
msd_period<-"Preclean"
datimtype <- "Postfrozen"
#dir<-"C:/Users/ouo8/OneDrive - CDC/MEDAB/Country Dashboards/Q3 Dashboards/"

# Date ranges
start_date <- as.Date("2019-10-01")
end_date   <- as.Date(Sys.Date())

start_datex <- "2019-10-01"
end_datex   <- as.character(Sys.Date())



#filepaths
main.dir<- ifelse(dir.exists(paste0("C:/Users/",Sys.getenv("USERNAME"),"/CDC/CPME - Country_Dashboards/","FY",FY,"/",
                                    quarter,"/")),
                  paste0("C:/Users/",Sys.getenv("USERNAME"),"/CDC/CPME - Country_Dashboards/","FY",FY,"/",
                         quarter,"/"),
                  "Directory does not exist")

# codedir<-paste(main.dir,devprod,"Code/",sep="/")
# outmer <- paste(main.dir,devprod,"Output/",sep="/")
ffolder <- paste(main.dir,"MSDs",msd_period, datimtype, sep="/")

msdlist <- list.files(ffolder, pattern="Site_IM")
msd_name<-msdlist[1]

input<-ffolder

# Pulling in COVID-related time-series data
# COVID code file path
covid.dir<- ifelse(dir.exists(paste0("C:/Users/",
  Sys.getenv("USERNAME"),
  "/CDC/CPME - Country_Dashboards/MER_COVID_analysis/")),
                  paste0("C:/Users/",
                    Sys.getenv("USERNAME"),
                    "/CDC/CPME - Country_Dashboards/MER_COVID_analysis/"),
                  "Directory does not exist")

rfunctions.dir <- paste0(covid.dir, "covid_functions/ITF_Power_BI-master/Rfunctions/") 
  

#read in packages needed to run code
source(paste0(rfunctions.dir,"packages_for_Power_BI.R"))



#indicators
indlist<-c(
  "HTS_TST",
  "HTS_TST_POS",
  "TX_CURR",
  "TX_ML",
  "TX_NET_NEW",
  "TX_PVLS",
  "TX_NEW",
  "TX_RTT",
  "SC_ARVDISP",
  "SC_STOCK",
  "SC_CURR",
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
mer_pull <- function(k) {
  
msd_namex <- paste(input, k, sep="/")

zipnamex <- unzip(msd_namex, list=T) %>% .$Name

#pull in MSD, filter to just the indicators and standarddisaggs we want, and pivot to wide by indicator and long by period
if(file.exists(paste(coviddir,zipnamex,sep="/"))==T){
  datim <- fread(file=paste(coviddir,zipnamex,sep="/"), colClasses=colvecx)
  } else {
unzip(msd_namex,
  exdir = coviddir)

datim <- fread(file=paste(coviddir,zipnamex,sep="/"), colClasses=colvecx)
    } 

  
# fread(paste0("unzip -cq ",myVar,"file.zip"))
  
# datimx <- datim %>%
#   filter(fiscal_year %in% c("2019","2020")) %>% 
#   filter(trendssemifine %ni% c("Coarse"))

datimx <- datim %>% 
  filter(fiscal_year %in% c("2019","2020", "2021")) %>% 
  filter(trendssemifine %ni% c("Coarse"))

# Getting age-sex disaggregates 
ouim1 <- datimx %>% 
  filter(indicator %in% indlist) %>% 
  select(-source_name) %>%
  pivot_longer(cols = targets:cumulative, names_to = "period") %>%  
  filter(!is.na(value)) %>%
  group_by_if(is.character) %>%
  summarise(value=sum(value, na.rm=T)) %>%
  ungroup() %>%
  mutate(fiscal_yr_period=paste(fiscal_year,period,sep="")) %>% 
  mutate(indicatorx = paste0(indicator, "_", numeratordenom)) %>% 
  mutate(disagg_level = 
           if_else(is.na(trendssemifine)| trendssemifine=="", 
                   "Topline", "Age disaggs")) %>% 
  mutate(Age_Sex = paste(sex, trendssemifine, sep=" "))

# Getting only the TX_ML indicators by age-sex disaggregates 
# TX_ML_IIT
# TX_ML_RTT
# TX_ML_STOP
ouimtx <- datimx %>%
  #  select(-cumulative,-approvallevel,-approvalleveldescription,-source_name) %>%
  filter(indicator %in% "TX_ML") %>%
  filter(standardizeddisaggregate %in% c(
    "Age/Sex/ARTCauseofDeath",
    "Age/Sex/ARTNoContactReason/HIVStatus")) %>% 
  # Recode indicators for TX_ML
  mutate(indicatorx = case_when(
    standardizeddisaggregate %in% c("Age/Sex/ARTNoContactReason/HIVStatus") &
      otherdisaggregate %in% 
      c("No Contact Outcome - Lost to Follow-Up 3+ Months Treatment",
        "No Contact Outcome - Lost to Follow-Up <3 Months Treatment")~
      "tx_ml_iit",
    standardizeddisaggregate %in% c("Age/Sex/ARTNoContactReason/HIVStatus") &
      otherdisaggregate %in% 
      c("No Contact Outcome - Died")~
      "tx_ml_died",
    standardizeddisaggregate %in% c("Age/Sex/ARTNoContactReason/HIVStatus") &
      otherdisaggregate %in% 
      c("No Contact Outcome - Transferred Out")~
      "tx_ml_tran",
    standardizeddisaggregate %in% c("Age/Sex/ARTNoContactReason/HIVStatus") &
      otherdisaggregate %in% 
      c("No Contact Outcome - Refused Stopped Treatment")~
      "tx_ml_stop",
    standardizeddisaggregate %in% c("Age/Sex/ARTCauseofDeath") &
      otherdisaggregate %in% 
      c("COD: HIV Disease Resulting in Other Infectious and Parasitic Disease")~
      "tx_ml_died_infection")) %>% 
  select(-source_name) %>%
  pivot_longer(targets:cumulative,names_to="period") %>%
  filter(!is.na(value)) %>%
  group_by_if(is.character) %>%
  summarise(value=sum(value, na.rm=T)) %>%
  ungroup() %>%
  mutate(fiscal_yr_period=paste(fiscal_year,period,sep="")) %>% 
  mutate(disagg_level = 
           if_else(is.na(trendssemifine)| trendssemifine=="", 
                   "Topline", "Age disaggs")) %>% 
  mutate(Age_Sex = paste(sex, trendssemifine, sep=" "))


# final dataset
ouimlong <- bind_rows(ouim1, ouimtx)


# Creating MER data frame to merge with COVID dataset
mer <- ouimlong %>% 
  select(operatingunit, countryname, 
 orgunituid, sitename, sitetype,     
 snu1, snu1uid, psnu, psnuuid,  snuprioritization,    
 primepartner, fundingagency, mech_code, mech_name,    
    disagg_level, sex, trendssemifine, Age_Sex,
    indicator, indicatorx, fiscal_yr_period, value) %>% 
  group_by_if(is.character) %>%
  summarise(value=sum(value, na.rm=T)) %>%
  ungroup() %>% 
  filter(fiscal_yr_period %ni% c(
    "2019cumulative",
    "2019targets"
    )) %>% 
  mutate(Date = case_when(
    fiscal_yr_period=="2019qtr1"       ~  as.Date("2020-01-01"),   
    fiscal_yr_period=="2019qtr2"       ~  as.Date("2020-03-31"),   
    fiscal_yr_period=="2019qtr3"       ~  as.Date("2020-06-30"),   
    fiscal_yr_period=="2019qtr4"       ~  as.Date("2020-09-30"),   
    fiscal_yr_period=="2020qtr1"       ~  as.Date("2020-01-01"),   
    fiscal_yr_period=="2020qtr2"       ~  as.Date("2020-03-31"),  
    fiscal_yr_period=="2020qtr3"       ~  as.Date("2020-06-30"),   
    fiscal_yr_period=="2020qtr4"       ~  as.Date("2020-09-30"),
    fiscal_yr_period=="2020targets"    ~  as.Date("2020-09-30"),
    fiscal_yr_period=="2020cumulative" ~  as.Date("2020-09-30"),
    fiscal_yr_period=="2021qtr1"       ~  as.Date("2021-01-01"),
    fiscal_yr_period=="2021targets"    ~  as.Date("2021-01-01"),
    fiscal_yr_period=="2021cumulative" ~  as.Date("2021-01-01"))) %>% 
  mutate(fiscal_yr = if_else(fiscal_yr_period %in% c("2020targets","2021targets"), 
    paste0("fy", fiscal_yr_period),
    if_else(fiscal_yr_period=="2020cumulative", "fy2020cumm",
      if_else(fiscal_yr_period=="2021cumulative", "fy2021cumm",
        paste0("fy", substr(fiscal_yr_period, 1, 4)))))) 
  

# Dataset with baseline 1: Q2 values
bq2 <- mer %>% 
  filter(fiscal_yr_period %in% c("2020qtr2")) %>% 
  mutate(fiscal_yr_period = "2020qtr2_baseline") %>% 
  mutate(fiscal_yr = "fy2020q2_baseline")
  
baseline_dt <- function(df, dt){
  df$Date <- as.Date(dt)
  return(df)
}  

baselistq2 <- purrr::map(.x=c(
  "2020-01-01",
  "2020-03-31",
  "2020-06-30",
  "2020-09-30",
  "2021-01-01"
), .f=~baseline_dt(bq2, .x))


base_q2 <- dplyr::bind_rows(baselistq2)


# Second base-line dataset, using average of Q1 and Q2
bq1 <- mer %>% 
  filter(fiscal_yr_period %in% c("2020qtr1")) %>% 
  mutate(fiscal_yr_period = "2020qtr1_baseline") %>% 
  mutate(fiscal_yr = "fy2020q1_baseline")


baselistq1 <- purrr::map(.x=c(
  "2020-01-01",
  "2020-03-31",
  "2020-06-30",
  "2020-09-30",
  "2021-01-01"
), .f=~baseline_dt(bq1, .x))


base_q1 <- dplyr::bind_rows(baselistq1)

# ------------------------------------------------

# Dataset with baseline 1: Q2 values
bq3 <- mer %>% 
  filter(fiscal_yr_period %in% c("2019qtr3")) %>% 
  mutate(fiscal_yr_period = "2019qtr3_baseline") %>% 
  mutate(fiscal_yr = "fy2019q3_baseline")
  

baselistq3 <- purrr::map(.x=c(
  "2020-01-01",
  "2020-03-31",
  "2020-06-30",
  "2020-09-30",
  "2021-01-01"
), .f=~baseline_dt(bq3, .x))


base_q3 <- dplyr::bind_rows(baselistq3)


# Second base-line dataset, using average of Q1 and Q2
bq4 <- mer %>% 
  filter(fiscal_yr_period %in% c("2019qtr4")) %>% 
  mutate(fiscal_yr_period = "2019qtr4_baseline") %>% 
  mutate(fiscal_yr = "fy2019q4_baseline")


baselistq4 <- purrr::map(.x=c(
  "2020-01-01",
  "2020-03-31",
  "2020-06-30",
  "2020-09-30",
  "2021-01-01"
), .f=~baseline_dt(bq4, .x))


base_q4 <- dplyr::bind_rows(baselistq4)

# #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# # making this for extended dates to go across chart
# # Dataset with baseline 1: Q2 values
# bq2x <- mer %>% 
#   filter(fiscal_yr_period %in% c("2020qtr2")) %>% 
#   mutate(fiscal_yr_period = "2020q2_baselinex") %>% 
#   mutate(fiscal_yr = "fy2020q2_baselinex")
# 
# baselistq2x <- purrr::map(.x=c(
#   start_datex,
#   "2020-01-01",
#   "2020-03-31",
#   "2020-06-30",
#   "2020-09-30",
#   end_datex
# ), .f=~baseline_dt(bq2x, .x))
# 
# 
# base_q2x <- dplyr::bind_rows(baselistq2x)
# 
# 
# # Second base-line dataset, using average of Q1 and Q2
# bq1x <- mer %>% 
#   filter(fiscal_yr_period %in% c("2020qtr1")) %>% 
#   mutate(fiscal_yr_period = "2020q1_baselinex") %>% 
#   mutate(fiscal_yr = "fy2020q1_baselinex")
# 
# 
# baselistq1x <- purrr::map(.x=c(
#   start_datex,
#   "2020-01-01",
#   "2020-03-31",
#   "2020-06-30",
#   "2020-09-30",
#   end_datex
# ), .f=~baseline_dt(bq1x, .x))
# 
# 
# base_q1x <- dplyr::bind_rows(baselistq1x)

mer_ou <- bind_rows(mer, base_q2, base_q1,
  base_q3, base_q4)

# 
# testx <- mer_ou %>% 
#   filter(fiscal_yr_period %in% c("2019q4_baseline", "2020q1_baseline")) %>% 
#   group_by(indicatorx, fiscal_yr_period, Date) %>% 
#   summarise(val = sum(value, na.rm=T)) %>% 
#   ungroup()

return(mer_ou) }

oufilesx <- oufiles[c(3, 19, 9, 16)]


oufilesx <- oufiles



# Getting data for all the OUs at site-level
merdf <- purrr::map(.x = oufilesx, .f = ~mer_pull(.x))

mer_all <- dplyr::bind_rows(merdf)

# Creating the variable country_code for the MER data
mer1 <- mer_all %>% 
  mutate(country_code = 
      if_else(countryname %in% c("Eswatini"),         
        parse_country("Swaziland", 
        to = "iso3c", language = c("en")),
        parse_country(countryname, 
        to = "iso3c", language = c("en")))) %>% 
  mutate(qtr = substr(fiscal_yr_period, 5, 8)) %>% 
  # mutate(indicator = substr(indicatorx,1,nchar(indicatorx)-2)) %>% 
  mutate(mer_match = paste(country_code, indicator, fiscal_yr_period, sep="_")) %>% 
  select(-indicator)


# Creating wide dataset for MER table
mer_wide <- mer1 %>% 
    select(operatingunit, countryname, country_code,
    fundingagency, primepartner, mech_code, mech_name,  
    snu1, psnu, snuprioritization, orgunituid, sitename, sitetype,
    disagg_level, trendssemifine, sex, Age_Sex,
    indicatorx, fiscal_yr_period, value) %>% 
  group_by_if(is.character) %>% 
  summarise_all(list(~sum(., na.rm=T))) %>% 
  ungroup() %>% 
  spread(fiscal_yr_period, value) %>% 
  filter(!is.na(indicatorx)) %>% 
 mutate_if(is.numeric, ~replace(., is.na(.), 0))



mer_wide$q1q2_average <- rowMeans(mer_wide[,c('2020qtr1', '2020qtr2')], na.rm=TRUE)

mer_wide$delta_q4 <- ifelse(is.na(mer_wide$q1q2_average), 0,
  ifelse(is.na(mer_wide$`2020qtr4`), NA_real_, mer_wide$`2020qtr4`- mer_wide$q1q2_average))

mer_wide$delta_q3 <- ifelse(is.na(mer_wide$q1q2_average), 0,
  ifelse(is.na(mer_wide$`2020qtr3`), NA_real_, mer_wide$`2020qtr3`- mer_wide$q1q2_average))

mer_wide$delta_q1 <- ifelse(is.na(mer_wide$q1q2_average), 0,
  ifelse(is.na(mer_wide$`2021qtr1`), NA_real_, mer_wide$`2021qtr1`- mer_wide$q1q2_average))

mer_wide$target_miss <- ifelse(is.na(mer_wide$`2020targets`), NA_real_,
  ifelse(is.na(mer_wide$`2020cumulative`), NA_real_, mer_wide$`2020cumulative`- mer_wide$`2020targets`))

mer_wide$per_target_miss <- ifelse(is.na(mer_wide$`2020targets`)|mer_wide$`2020targets`==0, NA_real_, 
  mer_wide$target_miss/mer_wide$`2020targets`)



mer_wide$all_average <- rowMeans(mer_wide[,c('2019qtr3', 
                                       '2019qtr4', 
                                       '2020qtr1', 
                                       '2020qtr2')], na.rm=TRUE)

mer_wide$delta_q4x <- ifelse(is.na(mer_wide$all_average), 0,
  ifelse(is.na(mer_wide$`2020qtr4`), NA_real_, mer_wide$`2020qtr4`- mer_wide$all_average))

mer_wide$delta_q3x <- ifelse(is.na(mer_wide$all_average), 0,
  ifelse(is.na(mer_wide$`2020qtr3`), NA_real_, mer_wide$`2020qtr3`- mer_wide$all_average))

mer_wide$delta_q1x <- ifelse(is.na(mer_wide$all_average), 0,
  ifelse(is.na(mer_wide$`2021qtr1`), NA_real_, mer_wide$`2021qtr1`- mer_wide$all_average))

mer_wide$mer_matchx <- paste0(
                             mer_wide$country_code,
                             mer_wide$indicatorx,
                             mer_wide$mech_code,
                             mer_wide$orgunituid,
                             mer_wide$disagg_level,
                             mer_wide$Age_Sex
                             )

mer_wide$mer_matchxx <- paste0(
                             mer_wide$country_code,
                             mer_wide$indicatorx,
                             mer_wide$orgunituid,
                             mer_wide$disagg_level
                             )


mer1$mer_matchx <- paste0(
                         mer1$country_code,
                         mer1$indicatorx,
                         mer1$mech_code,
                         mer1$orgunituid,
                         mer1$disagg_level,
                         mer1$Age_Sex
                             )

# Site-level dataset for map categorization
merf1 <- mer1 %>% 
  select(operatingunit, countryname, country_code,
    snu1, psnu, snuprioritization, orgunituid, sitename, sitetype,
    disagg_level, indicatorx, fiscal_yr_period, value) %>% 
  group_by_if(is.character) %>% 
  summarise_all(list(~sum(., na.rm=T))) %>% 
  ungroup() %>% 
  spread(fiscal_yr_period, value) %>% 
  filter(!is.na(indicatorx)) %>% 
  # convert nulls to zeros to have a more conservative average
  # if site doesn't report one quarter the average drops, as nulls are treated as zeroes
 mutate_if(is.numeric, ~replace(., is.na(.), 0))


merf1$q1q2_average <- rowMeans(merf1[,c('2020qtr1', '2020qtr2')], na.rm=TRUE)

merf1$delta_q4 <- ifelse(is.na(merf1$q1q2_average), 0,
  ifelse(is.na(merf1$`2020qtr4`), NA_real_, merf1$`2020qtr4`- merf1$q1q2_average))

merf1$delta_q3 <- ifelse(is.na(merf1$q1q2_average), 0,
  ifelse(is.na(merf1$`2020qtr3`), NA_real_, merf1$`2020qtr3`- merf1$q1q2_average))

merf1$delta_q1 <- ifelse(is.na(merf1$q1q2_average), 0,
  ifelse(is.na(merf1$`2021qtr1`), NA_real_, merf1$`2021qtr1`- merf1$q1q2_average))

merf1$perdl_q4 <- ifelse(is.na(merf1$q1q2_average) | merf1$q1q2_average==0, NA_real_, 
  merf1$delta_q4/merf1$q1q2_average)

merf1$perdl_q3 <- ifelse(is.na(merf1$q1q2_average) | merf1$q1q2_average==0, NA_real_, 
  merf1$delta_q3/merf1$q1q2_average)

merf1$perdl_q1 <- ifelse(is.na(merf1$q1q2_average) | merf1$q1q2_average==0, NA_real_, 
  merf1$delta_q1/merf1$q1q2_average)

  
merf1$target_miss <- ifelse(is.na(merf1$`2020targets`), NA_real_,
  ifelse(is.na(merf1$`2020cumulative`), NA_real_, merf1$`2020cumulative`- merf1$`2020targets`))

merf1$per_target_miss <- ifelse(is.na(merf1$`2020targets`)|merf1$`2020targets`==0, NA_real_, 
  merf1$target_miss/merf1$`2020targets`)



merf1$all_average <- rowMeans(merf1[,c('2019qtr3', 
                                       '2019qtr4', 
                                       '2020qtr1', 
                                       '2020qtr2')], na.rm=TRUE)

merf1$delta_q4x <- ifelse(is.na(merf1$all_average), 0,
  ifelse(is.na(merf1$`2020qtr4`), NA_real_, merf1$`2020qtr4`- merf1$all_average))

merf1$delta_q3x <- ifelse(is.na(merf1$all_average), 0,
  ifelse(is.na(merf1$`2020qtr3`), NA_real_, merf1$`2020qtr3`- merf1$all_average))

merf1$delta_q1x <- ifelse(is.na(merf1$all_average), 0,
  ifelse(is.na(merf1$`2021qtr1`), NA_real_, merf1$`2021qtr1`- merf1$all_average))


merf1$perdl_q4x <- ifelse(is.na(merf1$all_average) | merf1$all_average==0, NA_real_, 
  merf1$delta_q4x/merf1$all_average)

merf1$perdl_q3x <- ifelse(is.na(merf1$all_average) | merf1$all_average==0, NA_real_, 
  merf1$delta_q3x/merf1$all_average)

merf1$perdl_q1x <- ifelse(is.na(merf1$all_average) | merf1$all_average==0, NA_real_, 
  merf1$delta_q1x/merf1$all_average)

# Reversing the deviation for indicators you'd expect an increase when impacted by COVID
# "tx_ml_died"            = more deaths due to COVID?
# "tx_ml_tran"            = more transfers due to COVID?
# "tx_ml_stop"            = more patient stopping treatment due to COVID
# "tx_ml_died_infection"  = more patients dying of infectious diseases

rev_vars <- c(
"tx_ml_died"   ,       
"tx_ml_tran"   ,       
"tx_ml_stop"   ,       
"tx_ml_died_infection"
)

merf1$delta_q4 <- if_else(merf1$indicatorx %in% rev_vars, -1 * merf1$delta_q4, merf1$delta_q4)   
merf1$delta_q3 <- if_else(merf1$indicatorx %in% rev_vars, -1 * merf1$delta_q3, merf1$delta_q3)
merf1$delta_q1 <- if_else(merf1$indicatorx %in% rev_vars, -1 * merf1$delta_q1, merf1$delta_q1)
merf1$perdl_q4 <- if_else(merf1$indicatorx %in% rev_vars, -1 * merf1$perdl_q4, merf1$perdl_q4)
merf1$perdl_q3 <- if_else(merf1$indicatorx %in% rev_vars, -1 * merf1$perdl_q3, merf1$perdl_q3)
merf1$perdl_q1 <- if_else(merf1$indicatorx %in% rev_vars, -1 * merf1$perdl_q1, merf1$perdl_q1)

merf1$delta_q4x <- if_else(merf1$indicatorx %in% rev_vars, -1 * merf1$delta_q4x, merf1$delta_q4x)   
merf1$delta_q3x <- if_else(merf1$indicatorx %in% rev_vars, -1 * merf1$delta_q3x, merf1$delta_q3x)
merf1$delta_q1x <- if_else(merf1$indicatorx %in% rev_vars, -1 * merf1$delta_q1x, merf1$delta_q1x)
merf1$perdl_q4x <- if_else(merf1$indicatorx %in% rev_vars, -1 * merf1$perdl_q4x, merf1$perdl_q4x)
merf1$perdl_q3x <- if_else(merf1$indicatorx %in% rev_vars, -1 * merf1$perdl_q3x, merf1$perdl_q3x)
merf1$perdl_q1x <- if_else(merf1$indicatorx %in% rev_vars, -1 * merf1$perdl_q1x, merf1$perdl_q1x)




# Creating the dataset for the map
mer_map <- merf1 %>% 
  select(operatingunit:indicatorx,
    delta_q4:per_target_miss,
    delta_q4x:perdl_q1x) %>% 
  gather(colvars, values, delta_q4:perdl_q1x) %>% 
  filter(!is.na(values)) %>% 
  filter(!is.na(colvars)) %>% 
  mutate(baseline = if_else(str_detect(colvars, "x"), "All Qs (fy19Q3:fy20Q2)", "Recent Qs (fy20Q1+Q2)")) %>% 
  mutate(quarter = case_when(
    str_detect(colvars, "q1") ~ "Q1",
    str_detect(colvars, "q3") ~ "Q3",
    str_detect(colvars, "q4") ~ "Q4",
    str_detect(colvars, "target") ~ "Target")) %>% 
  mutate(colvarx = case_when(
    str_detect(colvars, "perdl") ~ "percent_diff",
    str_detect(colvars, "delta") ~ "absolute_diff",
    colvars=="per_target_miss" ~ "percent_diff",
    colvars=="target_miss" ~ "absolute_diff")) %>%
  select(-colvars) %>%
  spread(colvarx, values)
  

# Creating cutpoints for map of sites by quarter
dev_cut <- c(-2, -1, -.5, -.1, 0)

legend_labs <- c(">200% impact",
                 "101-200%",
                 "51-100%",
                 "11-50%",
                 "1-10%",
                 "No impact")


xdf <- mer_map 

# create category upper and lower ranges
q1_cat1_range <- xdf$percent_diff[ xdf$percent_diff <  dev_cut[1]]
q1_cat2_range <- xdf$percent_diff[ xdf$percent_diff >= dev_cut[1] &  xdf$percent_diff < dev_cut[2]]
q1_cat3_range <- xdf$percent_diff[ xdf$percent_diff >= dev_cut[2] &  xdf$percent_diff < dev_cut[3]]
q1_cat4_range <- xdf$percent_diff[ xdf$percent_diff >= dev_cut[3] &  xdf$percent_diff < dev_cut[4]]
q1_cat5_range <- xdf$percent_diff[ xdf$percent_diff >= dev_cut[4] &  xdf$percent_diff < dev_cut[5]]
q1_cat6_range <- xdf$percent_diff[ xdf$percent_diff >= dev_cut[5]] 


mer_map$cat <- if_else(is.na(mer_map$percent_diff), "not estimated",
  if_else(mer_map$percent_diff %in% q1_cat1_range, legend_labs[1],
  if_else(mer_map$percent_diff %in% q1_cat2_range, legend_labs[2],
  if_else(mer_map$percent_diff %in% q1_cat3_range, legend_labs[3],
  if_else(mer_map$percent_diff %in% q1_cat4_range, legend_labs[4],
  if_else(mer_map$percent_diff %in% q1_cat5_range, legend_labs[5], 
  if_else(mer_map$percent_diff %in% q1_cat6_range, legend_labs[6], 
    "not estimated")))))))



mer_map$vals <- if_else(is.na(mer_map$percent_diff), "cat7",
  if_else(mer_map$percent_diff %in% q1_cat1_range, "cat6",
  if_else(mer_map$percent_diff %in% q1_cat2_range, "cat5",
  if_else(mer_map$percent_diff %in% q1_cat3_range, "cat4",
  if_else(mer_map$percent_diff %in% q1_cat4_range, "cat3",
  if_else(mer_map$percent_diff %in% q1_cat5_range, "cat2", 
  if_else(mer_map$percent_diff %in% q1_cat6_range, "cat1", 
    "not estimated")))))))

mer_map$mer_matchxx <- paste0(
                             mer_map$country_code,
                             mer_map$indicatorx,
                             mer_map$orgunituid,
                             mer_map$disagg_level
                             )

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


#  Creating date ranges for the quarters
q1xstart_date <- as.Date("2019-10-01")
q1xend_date   <- as.Date("2020-01-01")
q1xdates <- seq(from=q1xstart_date, to = q1xend_date, by=1)


q2start_date <- as.Date("2020-01-02")
q2end_date   <- as.Date("2020-03-31")
q2dates <- seq(from=q2start_date, to = q2end_date, by=1)

q3start_date <- as.Date("2020-04-01")
q3end_date   <- as.Date("2020-06-30")
q3dates <- seq(from=q3start_date, to = q3end_date, by=1)

q4start_date <- as.Date("2020-07-01")
q4end_date   <- as.Date("2020-09-30")
q4dates <- seq(from=q4start_date, to = q4end_date, by=1)

q1start_date <- as.Date("2020-10-01")
q1end_date   <- as.Date("2021-01-01")
q1dates <- seq(from=q1start_date, to = q1end_date, by=1)

ouimfile <- list.files(ffolder, pattern="OU_IM")
ou_namex <- paste(input, ouimfile, sep="/")

ounamex <- unzip(ou_namex, list=T) %>% .$Name

#pull in MSD, filter to just the indicators and standarddisaggs we want, and pivot to wide by indicator and long by period
if(file.exists(paste(coviddir,ounamex,sep="/"))==T){
  ouim <- fread(file=paste(coviddir,ounamex,sep="/"))
  } else {
unzip(ou_namex,
  exdir = coviddir)

ouim <- fread(file=paste(coviddir,ounamex,sep="/"))
  } 

mer1x <- mer1
# mer1x <- ouim %>% 
#     mutate(country_code = 
#       if_else(countryname %in% c("Eswatini"),         
#         parse_country("Swaziland", 
#         to = "iso3c", language = c("en")),
#         parse_country(countryname, 
#         to = "iso3c", language = c("en")))) 

#read in datasets that are input data for subsequent functions
#cases and deaths
fun_ncov <- dget(paste0(rfunctions.dir, "get_ncov_data.R"))
ncov_data<-fun_ncov(rfunctions.dir)
#country metadata
fun_country <- dget(paste0(rfunctions.dir, "get_country.R"))
country_data<-fun_country()


#country date metadata
fun_country_date<-dget(paste0(rfunctions.dir,"get_country_date.R"))
country_date_long<-fun_country_date(rfunctions.dir)

df <- ncov_data %>% 
  select(country_code, Date, 
         `Cumulative Cases`, `Cumulative Deaths`, 
         `Population 2018.x`,
         data_source) %>% 
  dplyr::rename(cases  = `Cumulative Cases`,
                deaths = `Cumulative Deaths`,
                pop    = `Population 2018.x`) %>% 
  filter(country_code %in% unique(mer1x$country_code))


# Setting up the filters for Power BI
# 1) View (Cases, Deaths)
# 2) Metric (Counts, Rate, Cumulative)
# 3) Period (1, 7, 14, 30, or cumulative i.e. 999)
# 4) Source (JHU, WHO)
indicat_vec <- c("cases" ,  "deaths")
metricx_vec <- c("countsx", "ratesx")
periodx_vec <- c(7, 1, 999)
sourcex_vec <- c("JHU", "WHO") 

# indicat = indicat_vec[1]
# metricx = metricx_vec[2]
# periodx = periodx_vec[2]
# sourcex = sourcex_vec[1]


track_ncov_cross <- function(indicat , 
                       metricx , 
                       periodx , 
                       sourcex){
  
  dfx <- df %>% 
    filter(data_source %in% sourcex) %>% 
    gather(indicator, cumval, cases, deaths) %>% 
    filter(indicator %in% indicat) %>% 
    group_by(country_code) %>%
    arrange(Date) %>% 
    mutate(countsx =  case_when(periodx == 999 ~ cumval, 
                      TRUE ~        cumval - lag(cumval, periodx))) %>% 
    ungroup() %>% 
    mutate(countsx = if_else(countsx < 0, 0, countsx)) %>% 
    mutate(ratesx = if_else(pop > 0, round((countsx/pop)*100000,1), NA_real_)) %>% 
    mutate(pop_there = if_else(pop > 0, 1, 0)) %>% 
    gather(metrictype, valuex, countsx, ratesx) %>% 
    filter(metrictype %in% metricx) %>% 
    mutate(periodval = if_else(periodx==999, "Cumulative",
                         if_else(periodx==1, "24 hours",
                               paste0(periodx, " days")))) %>% 
    group_by(country_code) %>%
    mutate(roll_val = round(rollmean(valuex, k = 7, 
      fill = NA, align = "right"),1)) %>%
    ungroup() %>% 
    select(indicator, metrictype, periodval, data_source,
           country_code, Date, pop, 
           valuex, roll_val) 
  
  # xdfx <- dfx %>% filter(Date==max(df$Date)) 
  

fdfx <- dfx %>% 
  # Changing names for categories for Power BI
mutate(Indicator = case_when(
  indicator %in% c("deaths") ~ "Deaths",
  indicator %in% c("cases") ~  "Cases"
)) %>% 
  mutate(Metric = case_when(
    metrictype %in% c("countsx") ~ "Counts",
    metrictype %in% c("ratesx")    ~  "Rate"
  )) %>% 
  select(Indicator, Metric, periodval, data_source, 
         country_code, Date, pop, valuex, roll_val) 

return(fdfx)

}

# creating dataset with all permutations and combinations of variables
veclist <- expand.grid(
indicat = indicat_vec,
metricx = metricx_vec,
periodx = periodx_vec,
sourcex = sourcex_vec, KEEP.OUT.ATTRS = TRUE, stringsAsFactors = F)

# running function on all combinations
trkdf <- purrr::pmap(veclist, track_ncov_cross)

covid <- dplyr::bind_rows(trkdf)



fun_covid <- function(x){
  
covidx <- covid %>% 
  filter(Metric == x) %>% 
  filter(periodval %in% c("Cumulative")) %>% 
  filter(data_source %in% c("WHO")) %>% 
  select(-Metric, -periodval, -data_source, -roll_val) %>% 
  filter(Date %in% c(q3start_date, q3end_date,
                     q4start_date, q4end_date, 
                     q1start_date, q1end_date)) %>% 
  mutate(qdate = case_when(
    Date == q3start_date ~  "q3start_date",
    Date == q3end_date   ~  "q3end_date",
    Date == q4start_date ~  "q4start_date",
    Date == q4end_date   ~  "q4end_date",
    Date == q1start_date ~  "q1start_date",
    Date == q1end_date   ~  "q1end_date"
  )) %>% 
  select(-Date) %>% 
  filter(!is.na(country_code))

covid2 <- covidx %>% 
  spread(qdate, valuex) %>% 
  mutate_if(is.numeric , replace_na, replace = 0) %>% 
  mutate(q3rate = q3end_date-q3start_date,
         q4rate = q4end_date-q4start_date,
         q1rate = q1end_date-q1start_date) %>% 
  select(country_code, Indicator, q3rate, q4rate, q1rate) %>% 
  gather(rate, val, q3rate, q4rate, q1rate) %>% 
  mutate(colvar = paste0(Indicator, rate)) %>% 
  select(-Indicator, -rate) %>% 
  spread(colvar, val) %>% 
  # filter(country_code %in% unique(mer1$country_code)) %>% 
  mutate(total_cases = Casesq3rate + Casesq4rate + Casesq1rate,
         total_deaths = Deathsq3rate + Deathsq4rate + Deathsq1rate)


countryframe <- mer1x %>% 
  select(country_code, countryname) %>% 
  unique()

  
covid3 <- left_join(covid2, countryframe) 

return(covid3)}


dcounts <- fun_covid("Counts")
drate   <- fun_covid("Rate")

dcounts$metric <- "Counts"
drate$metric  <- "Rate"

dcovid <- bind_rows(dcounts, drate) %>% 
  gather(valtype, value, 
    Casesq3rate,  
    Casesq4rate,
    Casesq1rate,
    Deathsq3rate,
    Deathsq4rate,
    Deathsq1rate,
    total_cases,
    total_deaths) %>% 
  mutate(qtr_date = case_when(
   str_detect(valtype, "q3")    ~ as.Date("2020-06-30"),
   str_detect(valtype, "q4")    ~ as.Date("2020-09-30"),
   str_detect(valtype, "q1")    ~ as.Date("2021-01-01"),  
   str_detect(valtype, "total") ~ as.Date("2021-01-01"))) %>% 
  mutate(case_death = case_when(
   str_detect(valtype, "Case|case")    ~ "Cases",
   str_detect(valtype, "Death|death")  ~ "Deaths")) %>% 
  mutate(period = case_when(
   str_detect(valtype, "q3")    ~ "Q3",
   str_detect(valtype, "q4")    ~ "Q4",
   str_detect(valtype, "q1")    ~ "Q1",
   str_detect(valtype, "total") ~ "Q3+Q4+Q1")) %>%   
  mutate(periodx = case_when(
   str_detect(valtype, "q3")    ~ "fy2020Q3",
   str_detect(valtype, "q4")    ~ "fy2020Q4",
   str_detect(valtype, "q1")    ~ "fy2021Q1",
   str_detect(valtype, "total") ~ "fy20Q3+Q4+fy21Q1")) %>%   
mutate(row_var = paste0(metric, period, case_death))
  
rowvar_vec <- unique(dcovid$row_var)

cutpoints <- function(x){
# Getting data to map out:
xdfx <- dcovid %>% 
    filter(row_var == x)

xdfx$valx <- xdfx$value

q6  <- quantile(xdfx$valx[xdfx$valx>0], probs = seq(0, 1, 1/6), na.rm=T)     # Quintiles

# create category upper and lower ranges
cat1_range <- xdfx$valx[ xdfx$valx <  q6[2]]
cat2_range <- xdfx$valx[ xdfx$valx >= q6[2] &  xdfx$valx < q6[3]]
cat3_range <- xdfx$valx[ xdfx$valx >= q6[3] &  xdfx$valx < q6[4]]
cat4_range <- xdfx$valx[ xdfx$valx >= q6[4] &  xdfx$valx < q6[5]]
cat5_range <- xdfx$valx[ xdfx$valx >= q6[5] &  xdfx$valx < q6[6]]
cat6_range <- xdfx$valx[ xdfx$valx >= q6[6]] 

# Creating legend upper and lower ranges
# cat1_up <- max(cat1_range, na.rm = T)
#   cat1_2diff <- min(cat2_range, na.rm = T)-cat1_up
# cat2_lw <- cat1_up + (1/(10^decimalplaces(cat1_2diff)))
# cat2_up <- max(cat2_range, na.rm = T)
#   cat2_3diff <- min(cat3_range, na.rm = T)-cat2_up
# cat3_lw <- cat2_up + (1/(10^decimalplaces(cat2_3diff)))
# cat3_up <- max(cat3_range, na.rm = T)
#   cat3_4diff <- min(cat4_range, na.rm = T)-cat3_up
# cat4_lw <- cat3_up + (1/(10^decimalplaces(cat3_4diff)))
# cat4_up <- max(cat4_range, na.rm = T)
#   cat4_5diff <- min(cat5_range, na.rm = T)-cat4_up
# cat5_lw <- cat4_up + (1/(10^decimalplaces(cat4_5diff)))

cat2_lw <- min(cat2_range, na.rm = T)
cat3_lw <- min(cat3_range, na.rm = T)
cat4_lw <- min(cat4_range, na.rm = T)
cat5_lw <- min(cat5_range, na.rm = T)
cat6_lw <- min(cat6_range, na.rm = T)


legend_labs <- c(
  paste0("", format(cat2_lw, big.mark=",",scientific=FALSE)), 
  paste0("", format(cat3_lw, big.mark=",",scientific=FALSE)), 
  paste0("", format(cat4_lw, big.mark=",",scientific=FALSE)), 
  paste0("", format(cat5_lw, big.mark=",",scientific=FALSE)), 
  paste0("", format(cat6_lw, big.mark=",",scientific=FALSE)), 
  paste0("", cat6_lw, "+")) 



xdfx$qcats <- if_else(is.na(xdfx$valx), "not estimated",
  if_else(xdfx$valx %in% cat1_range, legend_labs[1],
  if_else(xdfx$valx %in% cat2_range, legend_labs[2],
  if_else(xdfx$valx %in% cat3_range, legend_labs[3],
  if_else(xdfx$valx %in% cat4_range, legend_labs[4],
  if_else(xdfx$valx %in% cat5_range, legend_labs[5], 
  if_else(xdfx$valx %in% cat6_range, legend_labs[6], 
    "not estimated")))))))


xdfx$qvals <- if_else(is.na(xdfx$valx), "absent",
  if_else(xdfx$valx %in% cat1_range, "cat1",
  if_else(xdfx$valx %in% cat2_range, "cat2",
  if_else(xdfx$valx %in% cat3_range, "cat3",
  if_else(xdfx$valx %in% cat4_range, "cat4",
  if_else(xdfx$valx %in% cat5_range, "cat5", 
  if_else(xdfx$valx %in% cat6_range, "cat6", 
    "not estimated")))))))

return(xdfx) }

# running function on all combinations
clist <- purrr::map(.x=rowvar_vec, .f=~cutpoints(.x))

cdf <- dplyr::bind_rows(clist)



# Bringing in Narratives dataset  
narrpath <- paste0(covid.dir, "MER-Narratives-Triangulation-master/")
narrvec <- list.files(narrpath, pattern="Narrative")


narrlist <- purrr::map(.x=narrvec, .f=~read_excel(paste0(narrpath, .x),
                         col_types = "text",
                         skip = 7))
narrdf <- dplyr::bind_rows(narrlist)


ndf <- narrdf %>% 
  dplyr::rename(country      = `Organisation Site`,
                indicator    = Indicator,
                funding_mech = `Funding Mechanism`,
                fiscal_yr    = `Fiscal Year`,
                quarter      = `Fiscal Quarter`,
                narrative    = `Narrative`) %>% 
  filter(!is.na(quarter)) %>% 
  select(
    country     ,
    indicator   ,
    funding_mech,
    fiscal_yr   ,
    quarter     ,
    narrative   
  ) %>% 
  mutate(fiscal_yr_period = case_when(
    quarter=="Q2" ~ "2020qtr2",
    quarter=="Q3" ~ "2020qtr3",
    quarter=="Q4" ~ "2020qtr4",
    quarter=="Q1" ~ "2021qtr1",
  )) %>% 
  mutate(country_code = 
      if_else(country %in% c("Eswatini"),         
        parse_country("Swaziland", 
        to = "iso3c", language = c("en")),
        parse_country(country, 
        to = "iso3c", language = c("en")))) %>% 
  mutate(qtr = substr(fiscal_yr_period, 5, 8)) %>% 
  # checking if COVID is mentioned in narrative
  mutate(covid_bin = if_else(str_detect(narrative, "COVID|covid|corona|ncov"), 1, 0)) %>% 
  mutate(covid_narr = if_else(covid_bin==1, narrative, "")) %>% 
  mutate(mer_match = paste(country_code, indicator, fiscal_yr_period, sep="_")) 
  

# extracting individual COVID sentences
ndf1 <- ndf %>% filter(covid_bin==1) %>% 
  # Create serial number for each narrative
  mutate(narr_num = row_number()) %>% 
  # select(mer_match, covid_narr) %>% 
  unnest_tokens(sentence_var, covid_narr, token="sentences", to_lower=FALSE) %>% 
  mutate(covid_2bin = 
      if_else(str_detect(sentence_var, "COVID|covid|corona|ncov|Covid|Pandemic|pandemic"), 1, 0)) %>% 
  filter(covid_2bin==1) %>% 
  group_by(narr_num) %>% 
  mutate(narr_2num = row_number()) %>% 
  ungroup() %>% 
  # Extract mech_code 
  mutate(mech_code = if_else(funding_mech %in% c("[OU Level]"), NA_character_,
    substr(funding_mech, 1, 5))) %>% 
  mutate(link_narrative_key = paste(country_code, mech_code, fiscal_yr_period, sep="_")) 


ndfx <- ndf1 %>% 
  select(-covid_2bin) %>% 
  group_by(narr_num) %>% 
mutate(narrative_covid = paste("(", narr_2num, ") ", 
  sentence_var, collapse = "\n")) %>% 
ungroup() %>% 
filter(narr_2num==1) %>% 
select(-sentence_var, -narr_2num, -covid_bin, )


# start_date <- as.Date("2019-10-01")
# end_date   <- as.Date(Sys.Date())

all_dates <- seq(from=start_date, to = end_date, by=1)
# Creating the dates dataframe
date_df <- as.data.frame(all_dates) %>% 
  dplyr::rename(date = all_dates) %>% 
  select(date) %>% unique() %>% 
  mutate(quarter = case_when(
    date %in% q1xdates ~ "Q1x",
    date %in% q2dates ~ "Q2",
    date %in% q3dates ~ "Q3",
    date %in% q4dates ~ "Q4",
    date %in% q1dates ~ "Q1",
    TRUE              ~ "Not yet reported"
  ))


# News
date_ou_frame <- expand.grid(
date = unique(date_df$date),
iso3code = unique(mer1x$country_code), 
  KEEP.OUT.ATTRS = TRUE, stringsAsFactors = F) %>% 
  mutate(date_ou_match = paste0(iso3code, "_", date)) 


country_data1 <- country_data %>% 
  filter(iso3code %in% unique(mer1x$country_code)) 

covid1 <- covid %>% 
  filter(country_code %in% unique(mer1x$country_code)) %>% 
  mutate(matchvar = paste0(country_code, Indicator, Metric))


cdf1 <- cdf %>% 
  filter(country_code %in% unique(mer1x$country_code)) %>% 
  mutate(matchvar = paste0(country_code, case_death, metric)) %>% 
  mutate(count_rate = if_else(metric=="Rate", " (per 100,000)", ""))

# cdf_table <- cdf1 %>% 
#   mutate(matchvar = paste0(country_code, case_death, metric))

  


outdir <- paste0(covid.dir, "output/")


# Output date df
write_tsv(date_df, paste0(outdir,"date_df.txt"),na="")

# Output country data
write_tsv(country_data1, paste0(outdir,"country_lookup.txt"),na="")

# Output MER data
fwrite(mer1, paste0(outdir,"mer_df.txt"),na="")

# Output MER data table with site-level data only
fwrite(mer_map, paste0(outdir,"mer_maptable.txt"),na="")

# Output MER data for map with site-level data only
fwrite(mer_wide, paste0(outdir,"mer_wide.txt"),na="")


# Output COVID data
write_tsv(covid1, paste0(outdir,"covid_df.txt"),na="")

# Output COVID data for country map by quarter
write_tsv(cdf1, paste0(outdir,"covid_maptable.txt"),na="")
# write_tsv(cdf_table, paste0(outdir,"covid_tooltip.txt"),na="")


# Output Narratives data
write_tsv(ndf, paste0(outdir,"Narratives.txt"),na="")
write_tsv(ndfx, paste0(outdir,"COVID_narr.txt"),na="")


# itfpathx <- paste0("C:/Users/",
#   Sys.getenv("USERNAME"),
#   "/CDC/ITF-COVID19 International Task Force - CDC ITF/")
# 
# # Reading in the ITF Mitigation data 
# itfpath <- ifelse(dir.exists(itfpathx),
#                   itfpathx,
#                   "Directory does not exist")
# 
# itf <- read.csv(paste0(itfpath, "CDC ITF Mitigation Tracker.csv"))
# 
# itf1 <- itf %>% 
#   dplyr::rename(iso3code = ISO.Code.3) %>% 
#   filter(iso3code %in% unique(mer1$country_code)) %>% 
#   mutate(Action.taken = gsub("^\\s+|\\s+$", "", Action.taken)) %>% 
#   mutate(Action.taken = if_else(Action.taken %in% c("Extend with same stringency"),
#     "Extend", Action.taken)) %>% 
#   mutate(date_ou_match = paste0(iso3code, "_", Date.implemented.or.lifted)) %>% 
#   mutate(action_num = case_when(
#     Action.taken %in% c("Impose") ~ "n1",
#     Action.taken %in% c("Extend") ~ "n2",
#     Action.taken %in% c("Strengthen") ~ "n3",
#     Action.taken %in% c("Ease") ~ "n4",
#     Action.taken %in% c("Lift") ~ "n5"
#   ))
# 
# # itf1x <- left_join(date_ou_frame, itf1)
# 
# 
# write_tsv(itf1, paste0(outdir,"ITF_mitigation.txt"),na="")


# Getting the stringency index data
strx <- read.csv("https://raw.githubusercontent.com/OxCGRT/covid-policy-tracker/master/data/timeseries/stringency_index.csv")
  
strx1 <- strx %>%
  select(-X) %>% 
  pivot_longer(-c(country_code, country_name), 
    names_to = "datex", values_to = "score") %>% 
  mutate(day = substr(datex, 2, 3)) %>% 
  mutate(month = substr(datex, 4, 6)) %>% 
  mutate(year = substr(datex, 7, 10)) %>% 
  mutate(date = as.Date(paste(year, month, day, sep="-"), "%Y-%b-%d")) %>% 
  select(country_code, date, score) %>% 
  filter(country_code %in% unique(mer1x$country_code)) 


write_tsv(strx1, paste0(outdir,"stringency.txt"),na="")



# create dataset with mitigation data, along with stringency value to show data point
strx2 <- strx1 %>% 
    mutate(date_ou_match = paste0(country_code, "_", date)) %>% 
    select(-date)

# 
# strmit <- left_join(itf1, strx2)
# 
# write_tsv(strmit, paste0(outdir,"mitigation_stringency.txt"),na="")


# Reading in the WHO mitigation data
who_url <- "https://extranet.who.int/xmart-api/odata/NCOV_PHM/CLEAN_PHSM"

degree_attributes <- jsonlite::fromJSON(who_url)

who <- as.data.frame(degree_attributes$value)
names(who) <- tolower(names(who))

who1 <- who %>% 
  select(
    who_id,
    country_code,
    admin_level,
    area_covered:comments,
    date_start,
    date_end,
    date_entry,
    enforcement,
    non_compliance_penalty,
    reason_ended) %>% 
  mutate(who_cat = if_else(
    who_category %in% c(
      "Other measures", 
      "other measures", 
      "4.3.3",
      "4.2.1",
      "4.3.2"
      ), "Other measures", 
    who_category
  )) %>% 
  mutate(who_subcat = if_else(
    is.na(who_subcategory), "Other", who_subcategory))
  
# table(who1$who_subcategory)
# table(who1$who_category)


write_tsv(who1, paste0(outdir,"who_mitigation.txt"),na="")


# Go long with the data
who2 <- who1 %>% 
  select(    
    who_id,
    country_code,
    who_cat,
    who_subcat,
    date_start,
    date_end) %>% 
  gather(start_end, date, date_start, date_end) %>% 
  mutate(start_end = substr(start_end, 6, length(start_end))) %>% 
  # remove the missing dates
  filter(!is.na(date)) %>% 
  mutate(date_ou_match = paste0(country_code, "_", date)) %>% 
  filter(country_code %in% unique(mer1x$country_code)) 


strx3 <- strx2 %>% select(-country_code) %>% unique()

who_long <- left_join(who2, strx3) %>% arrange(who_id)


write_tsv(who_long, paste0(outdir,"who_mitigation_long.txt"),na="")

