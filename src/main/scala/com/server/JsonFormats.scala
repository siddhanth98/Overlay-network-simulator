package com.server

import com.chord.Node._
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

/**
 * Spray json (de)marshaller to handle conversion between scala objects and json objects passed in http requests.
 */
object JsonFormats {
  import DefaultJsonProtocol._
  implicit val allDataJsonFormat: RootJsonFormat[AllData] = jsonFormat1(AllData)
  implicit val dataJsonFormat: RootJsonFormat[Data] = jsonFormat3(Data)
  implicit val dataResponseSuccess: RootJsonFormat[DataResponseSuccess] = jsonFormat1(DataResponseSuccess)
  implicit val dataResponseJsonFormat: RootJsonFormat[DataResponseFailed] = jsonFormat1(DataResponseFailed)
  implicit val dataStorageResponseSuccess: RootJsonFormat[DataStorageResponseSuccess] = jsonFormat1(DataStorageResponseSuccess)
  implicit val dataStorageResponseFailed: RootJsonFormat[DataStorageResponseFailed] = jsonFormat1(DataStorageResponseFailed)
}
