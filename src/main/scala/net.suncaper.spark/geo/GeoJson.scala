package net.suncaper.spark.geo


import com.esri.core.geometry.{GeoJsonImportFlags, Geometry, GeometryEngine}
import spray.json._

object GeoJson {


  /**
    * 对应这geoJson中的feature属性的实体
    *
    * @param id
    * @param properties
    * @param geometry
    */
  case class Feature(id: Option[JsValue],
                     properties: Map[String, JsValue],
                     geometry: RichGeometry) {
    def apply(property: String): JsValue = properties(property)

    def get(property: String): Option[JsValue] = properties.get(property)
  }

  case class FeatureCollection(features: Array[Feature])
    extends IndexedSeq[Feature] {
    def apply(index: Int): Feature = features(index)

    def length: Int = features.length
  }

  object GeoJsonProtocol extends DefaultJsonProtocol {

    implicit object RichGeometryJsonFormat extends RootJsonFormat[RichGeometry] {
      def write(g: RichGeometry): JsValue = {
        GeometryEngine.geometryToGeoJson(g.spatialReference, g.geometry).parseJson
      }

      def read(value: JsValue): RichGeometry = {
        val mg = GeometryEngine.geometryFromGeoJson(value.compactPrint,
          GeoJsonImportFlags.geoJsonImportDefaults, Geometry.Type.Unknown)
        new RichGeometry(mg.getGeometry, mg.getSpatialReference)
      }
    }

    implicit object FeatureJsonFormat extends RootJsonFormat[Feature] {
      def write(f: Feature): JsObject = {
        val buf = scala.collection.mutable.ArrayBuffer(
          "type" -> JsString("Feature"),
          "properties" -> JsObject(f.properties),
          "geometry" -> f.geometry.toJson)
        f.id.foreach(v => {
          buf += "id" -> v
        })
        JsObject(buf.toMap)
      }

      def read(value: JsValue): Feature = {
        val jso = value.asJsObject
        val id = jso.fields.get("id")
        val properties = jso.fields("properties").asJsObject.fields
        val geometry = jso.fields("geometry").convertTo[RichGeometry]
        Feature(id, properties, geometry)
      }
    }

    implicit object FeatureCollectionJsonFormat extends RootJsonFormat[FeatureCollection] {
      def write(fc: FeatureCollection): JsObject = {
        JsObject(
          "type" -> JsString("FeatureCollection"),
          "features" -> JsArray(fc.features.map(_.toJson): _*)
        )
      }

      def read(value: JsValue): FeatureCollection = {
        FeatureCollection(value.asJsObject.fields("features").convertTo[Array[Feature]])
      }
    }

  }
}
