package item2item

import item2item.ClassUtils._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read, write}
import org.apache.flink.api.scala.ExecutionEnvironment
import com.github.tototoshi.csv._
import java.io._

import org.apache.flink.api.common.functions.GroupReduceFunction
import org.apache.flink.util.Collector

/**
  * Created by Frederick Ayala-Gomez
  */
object ContentFeatures {
  def parse_content_file(env: ExecutionEnvironment, content_file: String): Map[String,ItemContent] = {
    env.readTextFile(content_file).collect().par.map {
      ic =>
        implicit val formats = DefaultFormats
        val jsValue = parse(ic)
        val parsed_ic = jsValue.extract[ItemContent]
        parsed_ic.item_id -> parsed_ic
    }.seq.toMap
  }

  def parse_similarity_file(env: ExecutionEnvironment, content_file: String): Map[String,Map[String,Double]] = {
    env.readTextFile(content_file).collect().par.map {
      ic =>
        val y  = ic.split(" ")
        y.head -> y.takeRight(y.size - 2).zipWithIndex.map(e=>(e._2+1).toString -> e._1.toDouble).toMap
    }.seq.toMap
  }

  def parse_plain_file(env: ExecutionEnvironment, content_file: String): Map[String,Set[String]] = {
    env.readTextFile(content_file).collect().map{
      x=>
        val s = x.split("\",\"").map(_.replace("\"", ""))
        s(1) -> s(2)
    }.groupBy(_._1).mapValues(x=>x.map(_._2).toSet)
  }

  def extract_list(json:JValue):List[String] = {
    for (
      JArray(objList) <- json;
      JObject(obj) <- objList;
      (key, JString(value)) <- obj; if value.contains("dbpedia.org/resource/")
    ) yield value
  }

  def jaccard(content_i:Set[String],content_j:Set[String]):Double = {
    content_i.intersect(content_j).size.toDouble / content_i.union(content_j).size.toDouble
  }

  def save_items_content_features(items_content:Map[String,ItemContent],path:String)={
    val clean_content = items_content.map{
      x=>
        extract_list(x._2.dbpedia_entities.head.resource_json).map(x._1 -> _.split("dbpedia.org/resource/").tail.head)
    }

    implicit object MyFormat extends DefaultCSVFormat {
      override val delimiter = ';'
      override val quoteChar = '|'
      override val escapeChar = '|'
      override val quoting = QUOTE_ALL
    }

    val f = new File(path + "content_features.csv")
    val writer = CSVWriter.open(f)

    clean_content.foreach{
      x =>
        x.foreach{
          y=>
            writer.writeRow(List(y._1,y._2))
        }
    }
    writer.close()
  }

  def compute_similarity_plain(compute_pairs:Vector[(String,String)],items_content_plain:Map[String,Set[String]]): Map[(String,String),Double] = {
    if(items_content_plain.nonEmpty){
      compute_pairs.par.map{
        p =>
          if(items_content_plain.contains(p._1) && items_content_plain.contains(p._2)){
            val item_i = items_content_plain(p._1)
            val item_j = items_content_plain(p._2)
            p -> jaccard(item_i,item_j)
          }else{
            p -> 0.0
          }
      }.seq.toMap
    }else{
      Map.empty[(String,String),Double]
    }
  }

  def compute_similarity(compute_pairs:Vector[(String,String)],items_content:Map[String,ItemContent]): Map[(String,String),Double] = {
    if(items_content.nonEmpty){
      compute_pairs.par.map{
        p =>
          if(items_content.contains(p._1) && items_content.contains(p._2)){
            val item_i = extract_list(items_content(p._1).dbpedia_entities.head.resource_json).map(_.split("dbpedia.org/resource/").tail.head).toSet
            val item_j = extract_list(items_content(p._2).dbpedia_entities.head.resource_json).map(_.split("dbpedia.org/resource/").tail.head).toSet
            p -> jaccard(item_i,item_j)
          }else{
            p -> 0.0
          }
      }.seq.toMap
    }else{
      Map.empty[(String,String),Double]
    }
  }
}
