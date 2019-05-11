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
    val geojson = scala.io.Source.fromResource("nyc-boroughs.geojson").mkString
    val features = geojson.parseJson.convertTo[FeatureCollection]

    features.sortBy(f => {
      val borough = f("boroughCode").convertTo[Int]
      (borough, -f.geometry.area2D())
    })
  }

}
