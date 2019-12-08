# Large-Scale-Geo-Spatial-Analysis-using-Apache-Spark
## CSE 511 - Data Processing at Scale

The project was aimed to conduct Large Scale Geo-Spatial Analysis of the NYC Taxi Trip Dataset - https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page

The aim was to find statistically signficant zones and spots for taxi pickups and drops in spatial temporal domain using Apache Spark and Spark SQL.

- An Apache spark cluster was set up using Amazon EC2 Instances
- Apache Spark Standalone Cluster was used as the cluster manager
- Amazon S3 was used as our cloud based Object Store
- Spark SQL was used to query and process the data
- Code was written in Scala
- Spatial queries such as range query, range join query, distance query, distance join query, hot zone analysis and hot cell analysis were executed.
  - Spatial queries were executed by implementing user defined functions such as ST_contains and ST_within in Scala.
  - ST_contains takes a point and a rectangle and returns a boolean indicating whether the point is inside the rectangle.
  - ST_within takes two points and a distance and returns a boolean indication whether the distance between the points is not more than the distance provided.

Technologies used are : Apache Spark, Spark SQL, Amazon EC2, Amazon S3, Scala, Sbt


