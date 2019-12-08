package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

import scala.collection.mutable.ListBuffer

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

//  def countNeighbors (minX:Int,maxX:Int,minY:Int,maxY:Int,minZ:Int,maxZ:Int,X:Int,Y:Int,Z:Int): Int = {
//
//    var count = 0
//
//    val cond1 = 7;
//    val cond2 = 11;
//    val cond3= 17;
//    val cond4 = 26;
//
//    if (X==minX || X==maxX){
//      count+=1;
//    }
//
//    if (Y==minY || Y==maxX) {
//      count+=1;
//    }
//
//    if (Z==minZ || Z==maxX){
//      count+=1;
//    }
//
//    if (count==1) {
//      return cond3
//    };
//
//    else if (count==2){
//      return cond2;
//    }
//
//    else if (count==3){
//      return cond1;
//    }
//
//    return cond4;
//
//  }

  // YOU NEED TO CHANGE THIS PART

  def calculateGScore(x: Int, y: Int, z: Int,minX:Double,minY:Double,minZ:Double,maxX:Double,maxY:Double,maxZ:Double,SD: Double, mean: Double, numCells: Double, mapOfCounts: Map[String, Int]): Double = {
    val xValues = List(x-1, x, x+1)
    val yValues = List(y-1, y, y+1)
    val zValues = List(z-1, z, z+1)

    var neighbours = new ListBuffer[Long]()
    for (i <- xValues) {
      for (j <- yValues) {
        for (k <- zValues) {

            if (i>=minX && i<=maxX && j>=minY && j<=maxY && k>=minZ && k<=maxZ) {
              val key = String.valueOf(i) + "|" + String.valueOf(j) + "|" + String.valueOf(k)
              if (mapOfCounts.contains(key)) {
                neighbours += mapOfCounts(key)
              }

            }
        }
      }
    }

    val num_neighbours = neighbours.size
    val sum_neighbours = neighbours.sum
    val dividend =sum_neighbours- (mean*num_neighbours)
    val divisor = SD * math.sqrt((numCells*num_neighbours - num_neighbours*num_neighbours)/(numCells-1))

    val result = dividend / divisor

    //println(result)

    return result
  }

}