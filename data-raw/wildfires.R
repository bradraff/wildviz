################################################################################
## Data Processing Script for Wildfire
## Description: A script for creating the wildfire.RData.
## Data: 1.88 Million US Wildfires - 24 years of geo-referenced wildfire records (1992 - 2015)
## Source: https://www.kaggle.com/rtatman/188-million-us-wildfires
## Description: This data publication contains a spatial database of wildfires that occurred in the United States from 1992 to 2015.
## Format: SQLite db
################################################################################

library(dplyr)

# Source SQLite database name
db_name = 'data-raw/FPA_FOD_20170508.sqlite'
cols=c('FIRE_NAME', 'DISCOVERY_DATE', 'CONT_DATE', 'STAT_CAUSE_DESCR', 'FIRE_SIZE', 'FIRE_SIZE_CLASS',
       'LATITUDE', 'LONGITUDE', 'STATE', 'FIPS_CODE', 'FIPS_NAME')
year_min = 2001
year_max = 2015

# Connect to the wildfire SQLite db
con <- DBI::dbConnect(drv = RSQLite::SQLite(), dbname = db_name)

# List all tables in db
tables <- DBI::dbListTables(con)

# Create a dataframe from the Fires table
select_query = 'SELECT * FROM Fires WHERE STATE = "CA"'

wildfires_res <- DBI::dbSendQuery(conn = con, statement = select_query)
wildfires_df <- DBI::dbFetch(wildfires_res)

# Clear query result and disconnect from db
DBI::dbClearResult(wildfires_res)
DBI::dbDisconnect(con)

# Basic manipulation - filter rows with COUNTY = NA and format the DISCOVERY_DATE
wildfires <- wildfires_df %>%
  dplyr::filter(!is.na(COUNTY)) %>%
  dplyr::mutate(DISCOVERY_DATE = as.Date(DISCOVERY_DATE, origin = structure(-2440588, class = 'Date')),
                CONT_DATE = as.Date(CONT_DATE, origin = structure(-2440588, class = 'Date'))) %>%
  dplyr::filter((format(DISCOVERY_DATE, '%Y') >= year_min) & (format(DISCOVERY_DATE, '%Y') <= year_max)) %>% # filter by year_min and year_max
  dplyr::select(all_of(cols))

# Write as .RData
usethis::use_data(wildfires, overwrite = TRUE, compress = "xz")
