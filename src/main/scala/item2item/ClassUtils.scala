package item2item

import java.net.URLEncoder

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read, write}
import com.typesafe.scalalogging.Logger
import scalaj.http._

/**
  * Created by Frederick Ayala-Gomez
  */
object ClassUtils {

  case class DBPediaResource(URI: String, resource_json:JValue, frequency:Int)

  case class ItemContent(item_id: String, metadata:Map[String,String], dbpedia_entities:List[DBPediaResource], tags:List[String])

  case class ItemConsumption(user_id: String, item_id: String, timestamp: Int)

  def getDBPediaContent(dbpedia_resource:String,frequency:Int) = {
    implicit val formats = Serialization.formats(NoTypeHints)
    val db_pedia_resource = "http://dbpedia.org/data/" + URLEncoder.encode(dbpedia_resource.replace("http://dbpedia.org/resource/",""),java.nio.charset.StandardCharsets.UTF_8.toString()) + ".json"
    val request: HttpRequest = Http(db_pedia_resource).headers(Map("Accept" -> "application/json")).timeout(connTimeoutMs = 10000, readTimeoutMs = 5000)
    val response = request.asString.body
    DBPediaResource(dbpedia_resource,parse(response) \ dbpedia_resource,frequency)
  }

}
