package com.can

import com.can.Node.{DataResponseFailed, DataResponseSuccess, DataStorageResponseFailure, DataStorageResponseSuccess, Movie}
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

object JsonFormats {
  import DefaultJsonProtocol._

  implicit val movieJsonFormat: RootJsonFormat[Movie] = jsonFormat3(Movie)
  implicit val movieStorageResponseSuccess: RootJsonFormat[DataStorageResponseSuccess] =
    jsonFormat1(DataStorageResponseSuccess)
  implicit val movieStorageResponseFailure: RootJsonFormat[DataStorageResponseFailure] =
    jsonFormat1(DataStorageResponseFailure)
  implicit val movieSearchResponseSuccess: RootJsonFormat[DataResponseSuccess] =
    jsonFormat1(DataResponseSuccess)
  implicit val movieSearchResponseFailure: RootJsonFormat[DataResponseFailed] =
    jsonFormat1(DataResponseFailed)
}
