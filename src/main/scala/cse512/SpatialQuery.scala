package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{

  /**
    * Check whether the queryRectangle fully contains the point (Consider on-boundary point).
    * @param queryRectangle
    * @param pointString
    * @return true or false
    */
  def ST_Contains(queryRectangle: String, pointString: String): Boolean = {

    if (queryRectangle == null || pointString == null) {
      return false
    }

    val valsRectangle = queryRectangle.split(",")
    val valsPoint = pointString.split(",")
    if (valsRectangle.length != 4 || valsPoint.length != 2) {
      return false
    }

    val recX1: Double = Math.min(valsRectangle(0).toDouble, valsRectangle(2).toDouble)
    val recY1: Double = Math.min(valsRectangle(1).toDouble, valsRectangle(3).toDouble)
    val recX2: Double = Math.max(valsRectangle(0).toDouble, valsRectangle(2).toDouble)
    val recY2: Double = Math.max(valsRectangle(1).toDouble, valsRectangle(3).toDouble)

    val pointX: Double = valsPoint(0).toDouble
    val pointY: Double = valsPoint(1).toDouble

    if (pointX >= recX1 && pointY >= recY1 && pointX <= recX2 && pointY <= recY2) {
      return true
    }

    return false
  }

  /**
    * Check whether the two points are within the given distance.
    * @param pointString1
    * @param pointString2
    * @param distance
    * @return true or false
    */
  def ST_Within(pointString1: String, pointString2: String, distance: Double): Boolean = {

    if (pointString1 == null || pointString2 == null) {
      return false
    }

    val valsPoint1 = pointString1.split(",")
    val valsPoint2 = pointString2.split(",")
    if (valsPoint1.length != 2 || valsPoint2.length != 2) {
      return false
    }

    val point1X: Double = valsPoint1(0).toDouble
    val point1Y: Double = valsPoint1(1).toDouble
    val point2X: Double = valsPoint2(0).toDouble
    val point2Y: Double = valsPoint2(1).toDouble

    val euclidean: Double = Math.sqrt(Math.pow(point1X - point2X, 2) + Math.pow(point1Y - point2Y, 2))

    return euclidean <= distance
  }

  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(ST_Contains(queryRectangle, pointString)))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(ST_Contains(queryRectangle, pointString)))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>(ST_Within(pointString1, pointString2, distance)))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>(ST_Within(pointString1, pointString2, distance)))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
