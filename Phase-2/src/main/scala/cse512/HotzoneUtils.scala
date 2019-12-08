package cse512
import scala.math._
object HotzoneUtils {

  def ST_Contains(queryRectangle: String,pointString: String):Boolean = {
    var rectPoints: Array[Double] = queryRectangle.split(",").map(_.toDouble);
    var points: Array[Double] = pointString.split(",").map(_.toDouble);
    var smaller_x = min(rectPoints(0),rectPoints(2))
    var larger_x = max(rectPoints(0),rectPoints(2))
    var smaller_y = min(rectPoints(1),rectPoints(3))
    var larger_y = max(rectPoints(1),rectPoints(3))
    if (points(0)>=smaller_x && points(0)<=larger_x && points(1)>=smaller_y && points(1)<=larger_y){
      return true;
    }
    return false;
  }

  // YOU NEED TO CHANGE THIS PART

}
