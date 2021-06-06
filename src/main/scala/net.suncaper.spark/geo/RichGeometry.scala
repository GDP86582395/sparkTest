package net.suncaper.spark.geo

import com.esri.core.geometry.{GeometryEngine, SpatialReference, Geometry}

import scala.language.implicitConversions

/**
  *
  * Geometry的包装类（包含指定的坐标系SpatialReference），提供更加方便的计算空间关系的方法
  *
  * @param geometry         the geometry object
  * @param spatialReference optional spatial reference; if not specified, uses WKID 4326 a.k.a.
  *                         WGS84, the standard coordinate frame for Earth.
  */
class RichGeometry(val geometry: Geometry,
                   val spatialReference: SpatialReference = SpatialReference.create(4326)) extends Serializable {

  def area2D(): Double = geometry.calculateArea2D()

  def distance(other: Geometry): Double = {
    GeometryEngine.distance(geometry, other, spatialReference)
  }

  def contains(other: Geometry): Boolean = {
    GeometryEngine.contains(geometry, other, spatialReference)
  }

  def within(other: Geometry): Boolean = {
    GeometryEngine.within(geometry, other, spatialReference)
  }

  def overlaps(other: Geometry): Boolean = {
    GeometryEngine.overlaps(geometry, other, spatialReference)
  }

  def touches(other: Geometry): Boolean = {
    GeometryEngine.touches(geometry, other, spatialReference)
  }

  def crosses(other: Geometry): Boolean = {
    GeometryEngine.crosses(geometry, other, spatialReference)
  }

  def disjoint(other: Geometry): Boolean = {
    GeometryEngine.disjoint(geometry, other, spatialReference)
  }
}

/**
  * Helper object for implicitly creating RichGeometry wrappers
  * for a given Geometry instance.
  */
object RichGeometry extends Serializable {
  implicit def createRichGeometry(g: Geometry): RichGeometry = new RichGeometry(g)
}

