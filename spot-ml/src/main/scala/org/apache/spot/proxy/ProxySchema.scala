package org.apache.spot.proxy

import org.apache.spark.sql.types._

/**
  * Data frame column names used in the proxy suspicious connects analysis.
  */
object ProxySchema {

  // fields from the input

  val Date = "p_date"
  val DateField = StructField(Date, StringType)

  val Time = "p_time"
  val TimeField = StructField(Time, StringType)

  val ClientIP = "clientip"
  val ClientIPField = StructField(ClientIP, StringType)

  val Host = "host"
  val HostField = StructField(Host, StringType)

  val ReqMethod = "reqmethod"
  val ReqMethodField = StructField(ReqMethod, StringType)

  val UserAgent = "useragent"
  val UserAgentField = StructField(UserAgent, StringType)

  val ResponseContentType = "resconttype"
  val ResponseContentTypeField = StructField(ResponseContentType, StringType)

  val Duration = "duration"
  val DurationField = StructField(Duration, IntegerType)

  val UserName = "username"
  val UserNameField = StructField(UserName, StringType)

  val AuthGroup = "authgroup"

  val ExceptionId = "exceptionid"

  val FilterResult = "filterresult"

  val WebCat = "webcat"
  val WebCatField = StructField(WebCat, StringType)

  val Referer = "referer"
  val RefererField = StructField(Referer, StringType)

  val RespCode = "respcode"
  val RespCodeField = StructField(RespCode, StringType)

  val Action = "action"

  val URIScheme = "urischeme"

  val URIPort = "uriport"
  val URIPortField = StructField(URIPort, StringType)

  val URIPath = "uripath"
  val URIPathField = StructField(URIPath, StringType)

  val URIQuery = "uriquery"
  val URIQueryField = StructField(URIQuery, StringType)

  val URIExtension = "uriextension"

  val ServerIP = "serverip"
  val ServerIPField = StructField(ServerIP, StringType)

  val SCBytes = "scbytes"
  val SCBytesField = StructField(SCBytes, IntegerType)

  val CSBytes = "csbytes"
  val CSBytesField = StructField(CSBytes, IntegerType)

  val VirusID = "virusid"
  val BcappName = "bcappname"
  val BcappOper = "bcappoper"

  val FullURI = "fulluri"
  val FullURIField = StructField(FullURI, StringType)

  // output fields

  val Word = "word"
  val WordField = StructField(Word, StringType)

  val Score = "score"
  val ScoreField = StructField(Score, DoubleType)
}
