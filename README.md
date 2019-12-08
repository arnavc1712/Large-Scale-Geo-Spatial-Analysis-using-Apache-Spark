# Large-Scale-Geo-Spatial-Analysis-using-Apache-Spark
## CSE 511 - Data Processing at Scale

The project was aimed to conduct Large Scale Geo-Spatial Analysis of the NYC Taxi Trip Dataset - https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page

The aim was to find statistically signficant zones and spots for taxi pickups and drops in spatial temporal domain using Apache Spark and Spark SQL. http://sigspatial2016.sigspatial.org/giscup2016/problem

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

### Steps to Execute

#### Phase 1
To create the JAR File:

 ```bash
 sbt assembly
 ```
 
 Install Apache Spark and Run:
 ```bash
 ./bin/spark-submit {YOUR-JAR-FILE}.jar result/output rangequery src/resources/arealm10000.csv -93.63173,33.0183,-93.359203,33.219456 rangejoinquery src/resources/arealm10000.csv src/resources/zcta10000.csv distancequery src/resources/arealm10000.csv -88.331492,32.324142 1 distancejoinquery src/resources/arealm10000.csv src/resources/arealm10000.csv 0.1
```

#### Phase 2
To create the JAR File:

 ```bash
 sbt assembly
 ```
 
To submit the code to Spark run:
```bash
./bin/spark-submit {YOUR-JAR-FILE}.jar test/output hotzoneanalysis src/resources/point-hotzone.csv src/resources/zone-hotzone.csv hotcellanalysis src/resources/yellow_tripdata_2009-01_point.csv
```
