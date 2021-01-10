################################################################################
## Data Processing Script for Master
## Description: A script for creating the master dataframe that combines the wildfire, AQI, and climate dataset.
##     The master dataframe takes each wildfire event in California between 2001 and 2015, and joins the AQI and climate
##     data for 30 days prior to the DISCOVERY_DATE (fire discovery date) and 30 days post CONT_DATE (fire contained date).
##     The data is utilized by the Shiny app to create dashboards.
################################################################################

# Load libraries
library(dplyr)
library(sqldf)

# Source helper code for climates
source("R/helper_climate.R")
source("R/prep_aqi.R")
source("R/prep_climate.R")

# Set the noaakey saved in .Renviron file in home directory - for Climate API
options('noaakey' = Sys.getenv('noaakey'))

# Get API email and API key saved in .Renviron file in home directory - for AQS API
aqs_api_email = Sys.getenv("aqs_api_email")
aqs_api_key = Sys.getenv("aqs_api_key")
year_min = 2001
year_max = 2015

# Data preparation functions for wildfire, AQI, and climate

# Prepare AQI data for California, 2000 - 2015
state_codes <- get_state_code(aqs_api_email = aqs_api_email, aqs_api_key = aqs_api_key, state_names = c('California'))
counties <- get_counties(aqs_api_email = aqs_api_email, aqs_api_key = aqs_api_key, state_codes = state_codes)
aqi <- daily_aqi(aqs_api_email = aqs_api_email, aqs_api_key = aqs_api_key, fips_list = counties$fips, year_min = year_min, year_max = year_max)

# Prepare climate data for California, 2000 - 2015
climate <- daily_climate_counties(fips_list = counties$fips, date_min = paste(year_min, '-01-01', sep = ''), date_max = paste(year_max, '-12-31', sep = ''))

# aqi <- readRDS('data-raw/aqi.rds') %>% filter(date >= '2001-01-01')
# climate <- readRDS('data-raw/climate.rds') %>% filter(date >= '2001-01-01')

# Load the wildfires dataset from the .RData files
load("data/wildfires.Rda")

# Manipulate dataframes prior to the join

# Wildfire
wildfires_mod <- wildfires %>%
  dplyr::filter(STATE == 'CA' & DISCOVERY_DATE >= paste(year_min, '-01-01', sep = '')) %>% # filter on California and wildfires starting 2000
  dplyr::inner_join(counties, by = c('FIPS_NAME' = 'name'))

# AQI
aqi_mod <- aqi %>%
  dplyr::mutate(fips = paste(state_code, county_code, sep='')) %>%
  dplyr::mutate(date = as.Date(date))

# Climate
climate_mod <- climate

# Master query that joins all the datasets
# AQI and climate data are joined to the wildfire events by 30 days prior to the DISCOVERY_DATE and 30 days post CONT_DATE
# We use sqldf as it allows joining of tables based on inequality conditions
master_query <- 'select fire.FIRE_NAME
                  , fire.DISCOVERY_DATE as disc_date__Date
                  , fire.CONT_DATE as cont_date__Date
                  , fire.STAT_CAUSE_DESCR
                  , fire.FIRE_SIZE
                  , fire.FIRE_SIZE_CLASS
                  , fire.LATITUDE
                  , fire.LONGITUDE
                  , fire.STATE
                  , fire.FIPS_NAME
                  , fire.fips
                  , clim.date as clim_date__Date
                  , clim.prcp
                  , clim.snow
                  , clim.snwd
                  , clim.tmax
                  , clim.tmin
                  , aqi.aqi
                  , aqi.co
                  , aqi.ozone
                  , aqi.no2
                  , aqi.pm25
                  , aqi.pm10
                from wildfires_mod fire
                  join climate_mod clim on fire.fips = clim.fips and clim.date between fire.DISCOVERY_DATE - 30 and fire.CONT_DATE + 30
                  join aqi_mod aqi on clim.date = aqi.date and clim.fips = aqi.fips'

# Run master query string in sqldf
master <- sqldf::sqldf(master_query, method = "name__class")

# Write as .RData
usethis::use_data(aqi, overwrite = TRUE, compress = "xz")
usethis::use_data(climate, overwrite = TRUE, compress = "xz")
usethis::use_data(master, overwrite = TRUE, compress = "xz")
