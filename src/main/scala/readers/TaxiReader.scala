package readers
import com.esri.core.geometry.Point
import config.Constants
import geo.Feature
import geo.GeoJsonProtocol._
import org.apache.spark.sql.functions.{to_timestamp, udf}
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author Maxime Lovino
  * @date 2019-05-11
  * @package
  * @project YellowSpark
  */

object TaxiReader {
  private val MILES_TO_KM = 1.60934
  private val GPS_MARGIN = 0.5

  def parseTaxiData(spark: SparkSession, boroughs: IndexedSeq[Feature], clean: Boolean = true): DataFrame = {
    import spark.implicits._
    def trimmedDataFrame(df: DataFrame): DataFrame = {
      df.columns.foldLeft(df)((curr, col) => curr.withColumnRenamed(col, col.trim()))
    }

    val bBoroughs = spark.sparkContext.broadcast(boroughs)

    val bLookup = (x: Double, y: Double) => {
      val feature: Option[Feature] = bBoroughs.value.find(f => {
        f.geometry.contains(new Point(x, y))
      })

      feature match {
        case Some(f) => f("borough").convertTo[String]
        case _ => "NA"
      }
    }
    val boroughUDF = udf(bLookup)


    def greatCircleDistance = (lat1: Double, lon1: Double, lat2: Double, lon2: Double) => {
      val R = 6378.137
      val dLat = lat2 * Math.PI / 180 - lat1 * Math.PI / 180
      val dLon = lon2 * Math.PI / 180 - lon1 * Math.PI / 180
      val a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
        Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
          Math.sin(dLon / 2) * Math.sin(dLon / 2)
      val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
      R * c
    }


    val greatCircleDistanceUDF = udf(greatCircleDistance)


    val speedKmh = (distance: Double, time: Double) => (distance / time) * 3600
    val speedUdf = udf(speedKmh)

    val milesConversion = (miles: Double) => miles * MILES_TO_KM
    val milesConversionUdf = udf(milesConversion)


    val rawTripsDf = spark.read.format("csv").option("header", "true").load(s"${Constants.rootFolderScheme}/trip_data_*.csv")
    val rawFaresDf = spark.read.format("csv").option("header", "true").load(s"${Constants.rootFolderScheme}/trip_fare_*.csv")


    val cleanedUpDf = trimmedDataFrame(rawFaresDf) // The fares CSV contains spaces in headers so we have to trim the column names
    val dateFormat = "yyyy-MM-dd HH:mm:ss"

    val faresDf = cleanedUpDf
      .withColumn("pickup_datetime", to_timestamp($"pickup_datetime", dateFormat))
      .withColumn("fare_amount", $"fare_amount".cast(DoubleType))
      .withColumn("surcharge", $"surcharge".cast(DoubleType))
      .withColumn("mta_tax", $"mta_tax".cast(DoubleType))
      .withColumn("tip_amount", $"tip_amount".cast(DoubleType))
      .withColumn("tolls_amount", $"tolls_amount".cast(DoubleType))
      .withColumn("total_amount", $"total_amount".cast(DoubleType))
      .withColumn("taxi_revenue", $"tip_amount" + $"fare_amount")


    val tripsDf = rawTripsDf
      .withColumn("rate_code", $"rate_code".cast(IntegerType))
      .withColumn("pickup_datetime", to_timestamp($"pickup_datetime", dateFormat))
      .withColumn("dropoff_datetime", to_timestamp($"dropoff_datetime", dateFormat))
      .withColumn("passenger_count", $"passenger_count".cast(IntegerType))
      .withColumn("trip_time_in_secs", $"trip_time_in_secs".cast(IntegerType))
      .withColumn("trip_distance", $"trip_distance".cast(DoubleType))
      .withColumn("trip_distance_km", milesConversionUdf($"trip_distance"))
      .withColumn("average_speed_kmh", speedUdf($"trip_distance_km", $"trip_time_in_secs"))
      .withColumn("pickup_longitude", $"pickup_longitude".cast(DoubleType))
      .withColumn("pickup_latitude", $"pickup_latitude".cast(DoubleType))
      .withColumn("dropoff_longitude", $"dropoff_longitude".cast(DoubleType))
      .withColumn("dropoff_latitude", $"dropoff_latitude".cast(DoubleType))
      .withColumn("pickup_borough", boroughUDF($"pickup_longitude", $"pickup_latitude"))
      .withColumn("dropoff_borough", boroughUDF($"dropoff_longitude", $"dropoff_latitude"))
      .withColumn("great_circle_distance_km", greatCircleDistanceUDF($"pickup_latitude", $"pickup_longitude", $"dropoff_latitude", $"dropoff_longitude"))
      .drop("vendor_id")

    faresDf.printSchema()
    tripsDf.printSchema()

    val df = tripsDf.join(faresDf, Seq("medallion", "hack_license", "pickup_datetime"), "inner")

    if (clean) {
      val finalDf = df.filter($"rate_code" !== 5)
        .filter($"average_speed_kmh" < 120)
        .filter("pickup_borough <> 'NA' AND dropoff_borough <> 'NA'") //This removes all rides starting OR finishing outside NYC
        .where(s"(rate_code = 1 AND great_circle_distance_km < (trip_distance_km + $GPS_MARGIN)) OR rate_code <>1")
        .where("passenger_count > 0")
        .filter("fare_amount > 0")
        .filter("trip_time_in_secs < (24*60*60)")
        .filter("average_speed_kmh > 1")

      finalDf.cache()
      finalDf
    } else {
      df.cache()
      df
    }
  }

}
