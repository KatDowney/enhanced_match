require(dplyr)
require(tidyr)
require(readr)
library(geosphere)
require(ggplot2)
require(knitr)
library(kableExtra)
library (rJava)
library (RJDBC)
library (bigQueryR)
library (googleAuthR)
library (bigrquery)
library (ggpubr)
library(leaflet)
library(rgdal)

dcc_sales_2021 <- read.csv('data/dcc_july_2021.csv')
View(dcc_sales_2021)

# advertised new cars by month from Looker:
# https://autotrader.eu.looker.com/explore/trusted/proxy_sold_data?qid=0O7XrY7RcOPAEeoKz6n31N&toggle=fil

# bigquery project settings
bqProject <- "at-data-platform-prod"
bqDataset <- "product"
bqBilling <- "at-data-platform-svc-prod"
connectToBigQuery <- function(project,dataset,billing){
  dbConnect(
    bigrquery::bigquery(),
    project = project,
    dataset = dataset,
    billing = billing
  )
}
con <- connectToBigQuery(project = bqProject, dataset = bqDataset, billing = bqBilling)
sql <- paste0(
  "

SELECT 
  sold.vrm as sold_vrm,
  sold.new_or_used as sold_new_or_used,
  sold.retailer_id as sold_did,
  sold.group_id as sold_gid,
  vrm_match_flag,
  viewed.vrm as viewed_vrm,
  viewed.event_date as viewed_event_date,
  viewed.retailer_id as viewed_did,
  viewed.group_id as viewed_gid
FROM `at-data-platform-prod.vehicle_sales.confirmed_sales_record_events` 
WHERE sold.group_id IN	('10023837') AND viewed.group_id IN ('10023837')
  ")

d <- bq_dataset_query(
  con,
  query = sql, 
  billing = bqBilling
)

enhanced_match <- bq_table_download(d)
# rm(con)

# write out data so don't need to keep running
#write.csv(enhanced_match, 'data/enhanced_match_v1.csv')
View(head(enhanced_match))

#create new market ex vrm match flag
enhanced_match <- enhanced_match %>%
  mutate(me_vrm_match_flag = ifelse(sold_vrm == viewed_vrm, 1,0))

#join on the enriched sales data

enhanced_match <- left_join(enhanced_match, dcc_sales_2021, by = c('sold_vrm' = 'VRM'), copy = TRUE)

# q1.how many sales did Auto Trader influence (split by the usual VRM, Site, Group level)
vrm_agg <- enhanced_match %>%
  group_by(sold_vrm) %>%
  summarise(row_count = n(), 
            vrm_match_flag_sum = sum(vrm_match_flag, na.rm = TRUE),
            me_vrm_match_flag_sum = sum(me_vrm_match_flag, na.rm = TRUE)
  ) %>%
  mutate(vrm_match_flag = ifelse(vrm_match_flag_sum >0 ,1,0),
         me_vrm_match_flag = ifelse(me_vrm_match_flag_sum >0 ,1,0))  

#CHECKING OUT EXAMPLE
vrm_example <- enhanced_match %>%
  filter(sold_vrm == 'GU15MXO')

write.csv(vrm_example, 'outputs/vrm_example.csv')

#aggregate VRMs to show how many sales through current match  vs ME friendly match

vrm_agg_2 <- vrm_agg %>%
  group_by() %>%
  summarise(vrm_matches  = sum(vrm_match_flag),
            me_vrm_matches  = sum(me_vrm_match_flag))
View(vrm_agg_2)
#barplot(vrm_agg$vrm_match_flag_sum)
#total value profit - need to pull in original match file


