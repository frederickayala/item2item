package item2item

import java.lang.Iterable
import javax.ws.rs.DefaultValue

import breeze._
import breeze.linalg._
import breeze.numerics._
import EuclideanItemRecommenderEval._
import org.apache.flink.api.common.functions.{GroupReduceFunction, _}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.common.operators.Order
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap
import scala.util.Random
import java.nio.file.{Path, Paths, Files,StandardOpenOption}
import java.io.File
import java.nio.charset.{StandardCharsets}
import scala.collection.immutable.HashMap
import Utils._
import breeze.stats.distributions._
import com.typesafe.scalalogging._

object EuclideanItemRecommenderTrainer extends LazyLogging{

  def main(args: Array[String]) {
    val usage =
      """
    Usage: EuclideanItemRecommenderTrainer
      --create_flink Boolean : If false, we use the local enviroment. Otherwise we create one.
      --datafile String : The path to the dataset
      --consider_time Boolean : If true, the co-ocurrance is based on the time sequence
      --num_factors List[Int] : The number of factors for the model. List separated by a comma
      --epochs List[Int]      : Maximum number of iterations. List separated by a comma
      --learning_rate List[Double] : The learning rate step. List separated by a comma
      --stop_by_likelihood Boolean  : Stop learning if the likelihood doesn't change (true/false)
      --training_mode String  : Use SGD or GD. If SGD is chosen a mini batch of 100 samples is used.
      --approx_condition Double   : Stop if the difference of the last two epochs is not greater than this
      --implicit_ratings_from Int : Consider only items where ratings are higher than this
      --filter_min_support Int  : To filter by support of the conditioned item i in P(j|i). Min
      --filter_max_support Int  : To filter by support of the conditioned item i in P(j|i). Max
      --max_sample Int  : The maximum size of the sample
      --alpha_sampling Int  : The alpha used for sampling
      --output String : The folders were the results will be saved

    Notes: The datafile format should be String String Int Int. User Item Rating Timestamp(If required) separated by a ; and all quoted by "
      """
    if (args.length == 0) {
      println(usage)
      System.exit(1)
    }

    val arglist = args.toList
    type OptionMap = Map[String, String]

    def nextOption(map: OptionMap, list: List[String]): OptionMap = {
      def isSwitch(s: String) = (s(0) == '-')
      list match {
        case Nil => map
        case "--create_flink" :: value :: tail =>
          nextOption(map ++ Map("create_flink" -> value.toString), tail)
        case "--datafile" :: value :: tail =>
          nextOption(map ++ Map("datafile" -> value.toString), tail)
        case "--consider_time" :: value :: tail =>
          nextOption(map ++ Map("consider_time" -> value.toString), tail)
        case "--num_factors" :: value :: tail =>
          nextOption(map ++ Map("num_factors" -> value.toString), tail)
        case "--epochs" :: value :: tail =>
          nextOption(map ++ Map("epochs" -> value.toString), tail)
        case "--learning_rate" :: value :: tail =>
          nextOption(map ++ Map("learning_rate" -> value.toString), tail)
        case "--stop_by_likelihood" :: value :: tail =>
          nextOption(map ++ Map("stop_by_likelihood" -> value.toString), tail)
        case "--training_mode" :: value :: tail =>
          if(! List("SGD","GD").contains(value.toString)){
            println("The training_mode should be SGD or GD")
            println(usage)
            System.exit(1)
            nextOption(map ++ Map("training_mode" -> value.toString), tail)
          }
          else{
            nextOption(map ++ Map("training_mode" -> value.toString), tail)
          }
        case "--approx_condition" :: value :: tail =>
          nextOption(map ++ Map("approx_condition" -> value.toString), tail)
        case "--implicit_ratings_from" :: value :: tail =>
          nextOption(map ++ Map("implicit_ratings_from" -> value.toString), tail)
        case "--filter_min_support" :: value :: tail =>
          nextOption(map ++ Map("filter_min_support" -> value.toString), tail)
        case "--filter_max_support" :: value :: tail =>
          nextOption(map ++ Map("filter_max_support" -> value.toString), tail)
        case "--max_sample" :: value :: tail =>
          nextOption(map ++ Map("max_sample" -> value.toString), tail)
        case "--alpha_sampling" :: value :: tail =>
          nextOption(map ++ Map("alpha_sampling" -> value.toString), tail)
        case "--output" :: value :: tail =>
          // if the directory does not exist, create it
          val new_dir = new File(value.toString)
          if(!new_dir.exists()){
            try{
              println("creating directory: " + value.toString);
              new_dir.mkdir()
              if(value.toString.takeRight(1) == "/")
                nextOption(map ++ Map("output" -> value.toString), tail)
              else{
                val output = value.toString + "/"
                nextOption(map ++ Map("output" -> output), tail)
              }
            }
            catch {
              case e: Exception =>
                println("Error while creating the output directory: " + e.getMessage);
                System.exit(1)
                nextOption(map ++ Map("output" -> value.toString), tail)
            }
          }
          else{
            println("The output directory should not exist.");
            System.exit(1)
            nextOption(map ++ Map("output" -> value.toString), tail)
          }
        case option :: tail =>
          println("Unknown option " + option)
          println(usage)
          System.exit(1)
          nextOption(map, tail)
      }
    }

    val options = nextOption(Map(), arglist)
    println(options)

    val create_flink: Boolean = options("create_flink").toBoolean
    val (env, streaming_env) = get_flink(create_flink)

    val option_datafile = options("datafile")
    val consider_time: Boolean = options("consider_time").toBoolean
    val l_num_factors: List[Int] = options("num_factors").split(",").map(_.toInt).toList
    val l_epochs: List[Int] = options("epochs").split(",").map(_.toInt).toList
    val l_learning_rate: List[Double] = options("learning_rate").split(",").map(_.toDouble).toList
    val stop_by_likelihood: Boolean = options("stop_by_likelihood").toBoolean
    val approx: Double = options("approx_condition").toDouble
    val implicit_ratings_from: Int = options("implicit_ratings_from").toInt
    val filter_min_support: Int = options("filter_min_support").toInt
    val filter_max_support: Int = options("filter_max_support").toInt
    val max_sample: Int = options("max_sample").toInt
    val alpha_sampling: Int = options("alpha_sampling").toInt
    val output: String = options("output").toString
    val training_mode: String = options("training_mode").toString

    //Process the dataset: Sort by user and timestamp, obtain the pairs of items in ascending order, apply filters and get items distribution
    var grouped_pairs:Map[CoConsumed,Double] = Map.empty[CoConsumed,Double]
    var items_i_count:Map[String,Double] = Map.empty[String,Double]
    var items_j_count:Map[String,Double] = Map.empty[String,Double]
    var items_dist:Map[String,Double] = Map.empty[String,Double]
    var items_i_count_unfiltered:Map[String,Double] = Map.empty[String,Double]
    var items_j_count_unfiltered:Map[String,Double] = Map.empty[String,Double]
    var items_dist_unfiltered:Map[String,Double] = Map.empty[String,Double]

    val log_head = List[String]("LogDate","Case","Activity","Resource","Class","Method","Line","Start","End|PMLog|")
    logger.debug(log_head.mkString("\"","\",\"","\""))

    if(consider_time){
      val result = processDataSetWithTime(option_datafile,implicit_ratings_from,filter_min_support,filter_max_support,env,streaming_env)
      grouped_pairs = result._1
      items_i_count = result._2
      items_j_count = result._3
      items_dist = result._4
      items_i_count_unfiltered = result._5
      items_j_count_unfiltered = result._6
      items_dist_unfiltered = result._7
    }
    else{
      val result = processDataSetWithoutTimeRandomSequence(option_datafile,implicit_ratings_from,filter_min_support,filter_max_support,env,streaming_env)
      grouped_pairs = result._1
      items_i_count = result._2
      items_j_count = result._3
      items_dist = result._4
      items_i_count_unfiltered = result._5
      items_j_count_unfiltered = result._6
      items_dist_unfiltered = result._7
    }

    val folderPath: Path = Paths.get(output)
    scala.tools.nsc.io.File(folderPath.toString + "/options.txt").writeAll(options.toString())
    saveProcessedDataset(output,"training",grouped_pairs,items_i_count,items_j_count,items_dist,items_i_count_unfiltered,items_j_count_unfiltered,items_dist_unfiltered)
    val grouped_pairs_keys = grouped_pairs.keys.toVector
    val items = items_j_count.par.flatMap(y=>List.fill(y._2.toInt)(y._1)).toVector
    val items_i_j = (items_i_count.map(_._1) ++ items_j_count.map(_._1)).toVector.distinct

    //Optimize via Gradient Descent
    l_num_factors.par.map{
      num_factors =>
        l_learning_rate.par.map{
          learning_rate =>
            val epochs = l_epochs.max
            val caseId = num_factors.toString + "factors_" + epochs.toString + "epochs_" + learning_rate.toString + "learningrate"
            logIt(logger,"INFO","training_process",caseId,{
              val rand =  Rand.uniform
              var factors = randomFactors(num_factors, items_i_j, rand)

              def run_likelihood(epoch: Int, cummLogLikelihood: List[Double]): (Map[String,Factor],List[Double])= {
                var ll = 0
                println("Running epoch: " + epoch.toString)

                if (cummLogLikelihood.length > 2) {
                  println(scala.math.abs(cummLogLikelihood.head - cummLogLikelihood(1)))
                  if (epoch >= epochs | (scala.math.abs(cummLogLikelihood.head - cummLogLikelihood(1)) < approx && scala.math.abs(cummLogLikelihood.head - cummLogLikelihood(2)) < approx)) {
                    saveResults(folderPath,epoch -1,"pkifactors_" + num_factors.toString + "factors_" + epochs.toString + "epochs_" + learning_rate.toString + "rate"  , factors:Map[String,Factor])
                    (factors,cummLogLikelihood)
                  }
                  else {
                    if(training_mode == "GD"){
                      factors = train_batch(logger,caseId,factors,grouped_pairs_keys,alpha_sampling,items_dist,items,learning_rate,num_factors,max_sample,rand)
                    }
                    else{
                      factors = train_minibatch(logger,caseId,factors, grouped_pairs_keys,alpha_sampling,items_dist,items,learning_rate,num_factors,max_sample,rand)
                    }
                    run_likelihood(epoch + 1, ll :: cummLogLikelihood)
                  }
                }
                else {
                  if(training_mode == "GD"){
                    factors = train_batch(logger,caseId,factors,grouped_pairs_keys,alpha_sampling,items_dist,items,learning_rate,num_factors,max_sample,rand)
                  }
                  else{
                    factors = train_minibatch(logger,caseId,factors, grouped_pairs_keys,alpha_sampling,items_dist,items,learning_rate,num_factors,max_sample,rand)
                  }
                  run_likelihood(epoch + 1, ll :: cummLogLikelihood)
                }
              }

              def run_epochs(epoch: Int): Map[String,Factor]= {
                println("Running epoch: " + epoch.toString + " with options: " + num_factors.toString + " factors " + epochs.toString + " epochs " + learning_rate.toString + " learning rate.")
                if (l_epochs.contains(epoch)) {
                  saveResults(folderPath,epoch-1,"pkifactors_" + num_factors.toString + "factors_" + epoch.toString + "epochs_" + learning_rate.toString + "rate"  , factors:Map[String,Factor])
                }
                if(epoch == epochs){
                  (factors)
                }
                else {
                  if(training_mode == "GD")
                    factors = train_batch(logger,caseId,factors,grouped_pairs_keys,alpha_sampling,items_dist,items,learning_rate,num_factors,max_sample,rand)
                  else
                    factors = train_minibatch(logger,caseId,factors,grouped_pairs_keys,alpha_sampling,items_dist,items,learning_rate,num_factors,max_sample,rand)
                  run_epochs(epoch + 1)
                }
              }

              if (stop_by_likelihood)
                run_likelihood(0, List.empty[Double])
              else
                run_epochs(0)
            })
          }
        }
    }
}