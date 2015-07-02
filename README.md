# AdReport: Analytics Platform Ads on Facebook 
## Insight Data Engineering Project

[View the website here.](http://52.25.86.7:5000/cassandra_test#home)

AdReport is a tool to analyse the performance of ads on Facebook. It can also be used to compare performance across different bidding level, bidding types and targeting specs. It is implemented using the following technologies:

* Apache Kafka 0.8.2.0
* Apache HDFS
* Spark SQL
* Apache Cassandra
* Flask with Highcharts and Bootstrap 

## Platform:
### Analytics:
The Analytics page gives some examples of analysis that can be done with the data. The pie chart shows composition of different bid_types. The bar chart shows a comparision for average cost per result and average cost per total action across different bid types.
![GitHub Logo](/Images/ss1.png)

### Queries:
The Queries Tab allows retreiving of information about an ad account or an ad set. When account id is provided on the first field, it shows up a table showing all the active adsets under that account. When ad set id is provided on the second field, it shows up graphs on performance metrics pertaining to that particular adset.
![GitHub Logo](/Images/ss3.png)

For example, in the following graph, you can see number of clicks and cost per unique click for the ad set queried. 

![GitHub Logo](/Images/ss2.png)

## Data Pipeline:
![GitHub Logo](/Images/ss4.png)


