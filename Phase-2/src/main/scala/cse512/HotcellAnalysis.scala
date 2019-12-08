package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
//import scala.math._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
  {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    pickupInfo.show()

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      )))
    spark.udf.register("CalculateY",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      )))
    spark.udf.register("CalculateZ",(pickupTime: String)=>((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      )))
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
    pickupInfo.createOrReplaceTempView("intermediate")
    pickupInfo.show()

    // Define the min and max of x, y, z
    val minimums = spark.sql("select min(x) as min_x,min(y) as min_y,min(z) as min_z from intermediate").first()

    val minX = -74.50/HotcellUtils.coordinateStep
    val maxX = -73.70/HotcellUtils.coordinateStep
    val minY = 40.50/HotcellUtils.coordinateStep
    val maxY = 40.90/HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells:Double = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

    // YOU NEED TO CHANGE THIS PART
    val givenpointsDf = spark.sql("select x,y,z from intermediate where x>=" + minX + " and x<= " + maxX + " and y>= " + minY + " and y<= " + maxY + " and z>= " + minZ + " and z<= " + maxZ + " order by x,y,z").persist();
    givenpointsDf.createOrReplaceTempView("Df0")
    givenpointsDf.show()

    val uniqueCombsDF = spark.sql("select x,y,z,count(*) as cnt from Df0 group by x,y,z order by x,y,z")
    uniqueCombsDF.createOrReplaceTempView("uniqueCombCount")
    uniqueCombsDF.show()
    var xValues = uniqueCombsDF.select("x").rdd.map(i => i(0)).collect() //used to store all x values in a list
    var yValues = uniqueCombsDF.select("y").rdd.map(i => i(0)).collect() //used to store all y values in a list
    var zValues = uniqueCombsDF.select("z").rdd.map(i => i(0)).collect() //used to store all z values in a list
    var listOfValues = uniqueCombsDF.select("cnt").rdd.map(i => i(0)).collect() //used to store all counts in a list
    spark.udf.register("Square",(num:Int)=>((num*num)));

    val lengthoflist = xValues.length


    var mapOfCounts:Map[String, Int] = Map()
    for (i <- 0 to lengthoflist - 1) {
      val key = xValues(i).toString + "|" + yValues(i).toString + "|" + zValues(i).toString
      val value = listOfValues(i).toString.toInt
      mapOfCounts += key -> value
    }

    val sumPointsDF = spark.sql("select sum(cnt) as sum,sum(Square(cnt)) as sum_squared from uniqueCombCount")
    sumPointsDF.show()
    val sumPoints = sumPointsDF.first().getLong(0) //Only used for mean calculation
    val sumSquared = sumPointsDF.first().getLong(1) //Only used for SD calculation
    val mean:Double = sumPoints/numCells; //mean is calculated here
    //val mean = mean.asInstanceOf[Double]
    //val i:Double = 2.0;
    val intermed:Double = sumSquared/numCells //
    val power:Double = mean*mean
    val value:Double = intermed - power
    val SD:Double = math.sqrt(value)

//    spark.udf.register("CalcNumNeighbors",(minX:Int,maxX:Int,minY:Int,maxY:Int,minZ:Int,maxZ:Int,X:Int,Y:Int,Z:Int) => ((
//      HotcellUtils.countNeighbors(minX,maxX,minY,maxY,minZ,maxZ,X,Y,Z)
//      )))
//
//    spark.udf.register("CalcDen",(n_count:Long) =>(({
//      var x = SD * math.sqrt((numCells*n_count - (n_count*n_count))/(numCells-1))
//      x
//    })))
//    //  val neigboursSum = spark.sql(f"select CalcNumNeighbors($minX%s,$maxX%s,$minY%s,$maxY%s,$minZ%s,$maxZ%s,l1.x,l1.y,l1.z), l1.x,l1.y,l1.z, sum(l2.cnt) as n_count from uniqueCombCount l1,uniqueCombCount l2 where (l1.x=l2.x and l1.y=l2.y and l1.z=l2.z) or (l2.x=l1.x-1 and l2.y=l1.y-1 and l2.z=l1.z-1) or (l2.x=l1.x+1 and l2.y=l1.y+1 and l2.z=l1.z+1) group by (l1.z,l1.y,l1.x) order by (l1.z,l1.y,l1.x)")
//
//    val neighboursSum = spark.sql(f"select CalcNumNeighbors($minX%s,$maxX%s,$minY%s,$maxY%s,$minZ%s,$maxZ%s,l1.x,l1.y,l1.z) as n_count, l1.x as x,l1.y as y,l1.z as z,sum(l2.cnt) as n_sum from uniqueCombCount l1, uniqueCombCount l2 where (l1.x=l2.x or l1.x=l2.x-1 or l1.x=l2.x+1) and (l1.y=l2.y or l1.y=l2.y-1 or l1.y=l2.y+1) and (l1.z=l2.z or l1.z=l2.z-1 or l1.z=l2.z+1) group by l1.x,l1.y,l1.z order by l1.x,l1.y,l1.z")
//
//    neighboursSum.createOrReplaceTempView("neighboursSum")
//
//    val num = spark.sql(f"select x,y,z,(n_sum-($mean*n_count)) as num from neighboursSum")
//
//    val den = spark.sql(f"select x,y,z,calcDen(n_count) as den from neighboursSum")
//
//    num.createOrReplaceTempView("numerator")
//    den.createOrReplaceTempView("denominator")
//
//
//    val res = spark.sql("select n1.x as x,n1.y as y,n1.z as z,(num/den) as res from numerator n1,denominator n2 where n1.x=n2.x and n1.y=n2.y and n1.z=n2.z order by res desc")
//
//    res.show()



    //  neighboursSum.show()

    spark.udf.register("calculateGScore", (x: Int, y: Int, z: Int) => (HotcellUtils.calculateGScore(x, y, z, minX,minY,minZ,maxX,maxY,maxZ,SD, mean, numCells, mapOfCounts)))
    var Result = spark.sql(f"select x, y, z from uniqueCombCount order by calculateGScore(x, y, z) desc limit 50").persist()
    Result.show()

    return Result





    //return pickupInfo // YOU NEED TO CHANGE THIS PART
    //return res
  }
}