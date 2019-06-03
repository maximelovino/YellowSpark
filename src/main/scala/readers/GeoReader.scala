package readers

import geo.GeoJsonProtocol._
import geo.{Feature, FeatureCollection}
import spray.json._

/**
  * @author Maxime Lovino
  * @date 2019-05-11
  * @package readers
  * @project YellowSpark
  */
object GeoReader {

  def parseBoroughs(): IndexedSeq[Feature] = {
    val geojsonSource = scala.io.Source.fromURL("https://yellowspark-us.s3.amazonaws.com/nyc-boroughs.geojson")
    val geojson = geojsonSource.mkString
    geojsonSource.close()
    val features = geojson.parseJson.convertTo[FeatureCollection]

    features.sortBy(f => {
      val borough = f("boroughCode").convertTo[Int]
      (borough, -f.geometry.area2D())
    })
  }

}
