import item2item.Utils
import org.apache.commons.math3.random.MersenneTwister
import org.scalatest._
import org.scalatest.matchers.ShouldMatchers
import Utils._
import java.lang.Iterable
import javax.ws.rs.DefaultValue
import breeze._
import breeze.linalg._
import breeze.numerics._
import org.apache.flink.api.common.functions.{GroupReduceFunction, _}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.common.operators.Order
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap
import scala.util.Random
import java.nio.file.{Path, Paths, Files,StandardOpenOption}
import java.nio.charset.{StandardCharsets}
import scala.collection.immutable.HashMap
import scala.io.Source
import breeze.stats.distributions._

class EuclideanItemRecommenderSpec extends FlatSpec with ShouldMatchers {
  val no_time_train = getClass.getResource("/i2i_books.train").getPath
  val time_train = getClass.getResource("/i2i_movielens.train").getPath
  val num_factors = 20
  val alpha_sampling = 3
  val learning_rate = 0.01
  val max_sample = 30
  val output = getClass.getResource("").getPath
  //val env = ExecutionEnvironment.createLocalEnvironment()
  //val streaming_env = StreamExecutionEnvironment.createLocalEnvironment()
  //val (grouped_items_notime, items_i_count_notime, items_j_count_notime, item_dist_notime) = processDataSetWithoutTimeRandomSequence(no_time_train,0,1,env,streaming_env)
  //val (grouped_items_time, items_i_count_time, items_j_count_time, item_dist_time) = processDataSetWithTime(time_train,4,1,env,streaming_env)

  val l_pr = List(50,65,70,72,72,78,80,82,84,84,85,86,88,88,90,94,96,98,98,99)

  def percentile_rank_simple(v:Int,list:List[Int],higher:Boolean):Double ={
    var B = 0
    if(higher)
      B = list.filter(_ > v).size
    else
      B = list.filter(_ < v).size
    val E = list.filter(_ == v).size
    val N = list.size
    (B + (0.5 * E)) / N
  }

  print(percentile_rank_simple(99,l_pr,true))
  print(percentile_rank_simple(99,l_pr,false))

  "The Simple Precentile Rank example" should " be .45" in {
    assert(percentile_rank_simple(84,l_pr,false) === 0.45)
  }

}