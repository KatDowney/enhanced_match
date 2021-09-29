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
  sold_date as sold_date,
  sold.vrm as sold_vrm,
  sold.new_or_used as sold_new_or_used,
  sold.retailer_id as sold_did,
  sold.group_id as sold_gid,
  sold.retailer_latitude as sold_did_lat,
  sold.retailer_longitude as sold_did_lon,
  sold.retailer_postcode as sold_did_postcode,
  sold.make as sold_make,
  sold.model as sold_model,
  buyer.postcode_district as buyyer_postcode_district,
  buyer.latitude as buyer_lat,
  buyer.longitude buyer_lon,
  vrm_match_flag,
  viewed.vrm as viewed_vrm,
  viewed.event_date as viewed_event_date,
  viewed.retailer_id as viewed_did,
  viewed.group_id as viewed_gid,
  viewed.distance_between as viewed_distance_between,
  viewed.retailer_postcode as viewed_retailer_postcode,
  viewed.retailer_latitude as viewed_retailer_postcode,
  viewed.retailer_longitude as viewed_retailer_postcode,
 
  
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

#barplot normal vs me matches

#total value profit - create a distance banding
enhanced_match <- enhanced_match %>%
        mutate(viewed_distance_banding = ifelse(viewed_distance_between <10, "less than 10 miles",
                                         ifelse(viewed_distance_between <20, "less than 20 miles",
                                         ifelse(viewed_distance_between <50, "less than 50 miles",
                                        ifelse(viewed_distance_between <100, "less than 100 miles", "100 or more miles")))))

enhanced_match$viewed_distance_banding <- factor(enhanced_match$viewed_distance_banding, levels = c("less than 10 miles", "less than 20 miles", "less than 50 miles", "less than 100 miles", "100 or more miles"), ordered = TRUE)

        
View(enhanced_match)

profit_agg <- enhanced_match %>%
            filter(me_vrm_match_flag == 1) %>%
            group_by(viewed_distance_banding) %>%
            summarise(row_count = n(),
                      distinct_sales = n_distinct(sold_vrm),
                      avg_profit = mean(Chassis.Margin))

View(profit_agg)

barplot(profit_agg$avg_profit, names.arg = profit_agg$viewed_distance_banding, col = 'steelblue', main = "Avg profit by distance", xlab = "Distance Banding", ylab = 'Mean Profit' )

#q4.How far away are they from the advertiser vs. Traditional sites? 
#want to pull in pre and post ME data here so it's a boxplot of may/jun/july

barplot(profit_agg$distinct_sales, names.arg = profit_agg$viewed_distance_banding, col = 'steelblue4', main = "Avg sales by distance", xlab = "Distance Banding", ylab = 'Avg sales' )



