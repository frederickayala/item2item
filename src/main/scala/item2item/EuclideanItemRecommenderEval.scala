package item2item

import java.lang.Iterable
import javax.ws.rs.DefaultValue

import breeze._
import breeze.linalg._
import breeze.numerics._
import org.apache.flink.api.common.functions.{GroupReduceFunction, _}
import org.apache.flink.api.scala._
import org.apache.flink.api.common.operators.Order
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.collection.immutable.ListMap
import scala.collection.immutable.SortedMap
import scala.util.Random
import java.nio.file.{Path, Paths, Files, StandardOpenOption}
import java.nio.charset.{StandardCharsets}
import scala.collection.immutable.HashMap
import scala.collection.immutable.HashSet
import Utils._
import breeze.stats.distributions._
import com.typesafe.scalalogging._
import SimilarityKernel._
import scala.collection.parallel.immutable.ParMap
import item2item.ClassUtils._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read, write}

/**
  * Created by Frederick Ayala-Gomez
  */
object EuclideanItemRecommenderEval extends LazyLogging{
  val usage =
    """
    Usage: EuclideanItemRecommenderEval
      --create_flink Boolean : If false, we use the local enviroment. Otherwise we create one.
      --training_file String : The path to the testing dataset
      --testing_file String : The path to the testing dataset
      --consider_time Boolean : If true, the co-ocurrance is based on the time sequence
      --factors_folder String : The path to the factors folder
      --load_training_files Boolean: If the Utils.readTraining() functions will be used
      --percentile_n Int  : Number of random samples users for the evaluation
      --implicit_ratings_from Int : Consider only items where ratings are higher than this
      --filter_min_support Int  : To filter by support of the conditioned item i in P(j|i). Min
      --filter_max_support Int  : To filter by support of the conditioned item i in P(j|i). Max
      --filter_min_support_training Int  : To filter by support of the conditioned item i in P(j|i). Min
      --filter_max_support_training Int  : To filter by support of the conditioned item i in P(j|i). Max
      --reference_size List[Int] : Different fisher reference sizes. List separated by a comma
      --modalities String : A string containing the selected modalities to be used (e.g. 'ecp,pki,cosine,jaccard')
      --fisher Boolean: If the similarity kernel will be used
      --mpr_sum_training Boolean: Indicates if we will sum the training
      --recall_at Boolean: Indicates the size of the list for recall
      --dcg_at Boolean: Indicates the size of the list for DCG
      --list_combinations: Type of combinations. List separated by comma
      --normalizations: Types of normalizations for the fisher kernels. List separated by comma
      --content_file: The content file for the items. This should be JSON file in each row that serializes to the class
      --jaccard_file: The jaccard file 
      --content_similarity_file: The content similarity file
    Notes: The datafile format should be Int Int Int Int. User Item Rating Timestamp separated by a space
    """

  def main(args: Array[String]): Unit = {
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
        case "--training_file" :: value :: tail =>
          nextOption(map ++ Map("training_file" -> value.toString), tail)
        case "--testing_file" :: value :: tail =>
          nextOption(map ++ Map("testing_file" -> value.toString), tail)
        case "--consider_time" :: value :: tail =>
          nextOption(map ++ Map("consider_time" -> value.toString), tail)
        case "--factors_folder" :: value :: tail =>
          if (value.toString.takeRight(1) == "/")
            nextOption(map ++ Map("factors_folder" -> value.toString), tail)
          else {
            val folder = value.toString + "/"
            nextOption(map ++ Map("factors_folder" -> folder), tail)
          }
        case "--percentile_n" :: value :: tail =>
          nextOption(map ++ Map("percentile_n" -> value.toString), tail)
        case "--load_training_files" :: value :: tail =>
          nextOption(map ++ Map("load_training_files" -> value.toString), tail)
        case "--implicit_ratings_from" :: value :: tail =>
          nextOption(map ++ Map("implicit_ratings_from" -> value.toString), tail)
        case "--filter_min_support" :: value :: tail =>
          nextOption(map ++ Map("filter_min_support" -> value.toString), tail)
        case "--filter_max_support" :: value :: tail =>
          nextOption(map ++ Map("filter_max_support" -> value.toString), tail)
        case "--filter_min_support_training" :: value :: tail =>
          nextOption(map ++ Map("filter_min_support_training" -> value.toString), tail)
        case "--filter_max_support_training" :: value :: tail =>
          nextOption(map ++ Map("filter_max_support_training" -> value.toString), tail)
        case "--reference_size" :: value :: tail =>
          nextOption(map ++ Map("reference_size" -> value.toString), tail)
        case "--fisher" :: value :: tail =>
          nextOption(map ++ Map("fisher" -> value.toString), tail)
        case "--mpr_sum_training" :: value :: tail =>
          nextOption(map ++ Map("mpr_sum_training" -> value.toString), tail)
        case "--recall_at" :: value :: tail =>
          nextOption(map ++ Map("recall_at" -> value.toString), tail)
        case "--dcg_at" :: value :: tail =>
          nextOption(map ++ Map("dcg_at" -> value.toString), tail)
        case "--list_combinations" :: value :: tail =>
          nextOption(map ++ Map("list_combinations" -> value.toString), tail)
        case "--normalizations" :: value :: tail =>
          nextOption(map ++ Map("normalizations" -> value.toString), tail)
        case "--modalities" :: value :: tail =>
          val alloweed_mods = List("pki","ecp_coocurrance","ecp_coocurrance_symmetric","jaccard_coocurrance","cosine_coocurrance","content","jaccard_file","content_similarity")
          if (value.split(',').toList.map(alloweed_mods.contains(_)).filter(_ == false).size != 0) {
            println("Unknown modality in modalities parameter:" + value)
            println("Accepted values are:")
            println(alloweed_mods)
            System.exit(1)
          }
          nextOption(map ++ Map("modalities" -> value.toString), tail)
        case "--content_file" :: value :: tail =>
          nextOption(map ++ Map("content_file" -> value.toString), tail)
        case "--jaccard_file" :: value :: tail =>
          nextOption(map ++ Map("jaccard_file" -> value.toString), tail)
        case "--content_similarity_file" :: value :: tail =>
          nextOption(map ++ Map("content_similarity_file" -> value.toString), tail)
        case option :: tail =>
            println("Unknown option " + option)
            System.exit(1)
            nextOption(map, tail)
      }
    }

    val options = nextOption(Map(), arglist)
    println(options)

//    val logger = Logger(LoggerFactory.getLogger("name"))

    val create_flink: Boolean = options("create_flink").toBoolean
    val (env, streaming_env) = get_flink(create_flink)

    val testing_file = options("testing_file")
    val training_file = options("training_file")
    val consider_time: Boolean = options("consider_time").toBoolean

    val factors_folder = options("factors_folder")
    val percentile_n: Int = options("percentile_n").toInt
    val implicit_ratings_from: Int = options("implicit_ratings_from").toInt
    val filter_min_support: Int = options("filter_min_support").toInt
    val filter_max_support: Int = options("filter_max_support").toInt
    val filter_min_support_training: Int = options("filter_min_support").toInt
    val filter_max_support_training: Int = options("filter_max_support").toInt
    val l_reference_size: List[Int] = options("reference_size").split(",").map(_.toInt).toList
    val selected_modalities: List[String] = options("modalities").split(",").toList
    val load_training_files: Boolean = options("load_training_files").toBoolean
    val fisher: Boolean = options("fisher").toBoolean
    val mpr_sum_training: Boolean = options("mpr_sum_training").toBoolean
    val l_recall_at: List[Int] = options("recall_at").split(",").map(_.toInt).toList
    val l_dcg_at: List[Int] = options("dcg_at").split(",").map(_.toInt).toList
    val l_list_combinations: List[String] = options("list_combinations").split(",").map(_.toString).toList
    val l_normalizations: List[String] = options("normalizations").split(",").map(_.toString).toList
    val content_file: String = options("content_file")
    val jaccard_file: String = options("jaccard_file")
    val content_similarity_file: String = options("content_similarity_file")

    //Process the datasets: Sort by user and timestamp, obtain the pairs of items in ascending order, apply filters and get items distribution
    var grouped_pairs: Map[CoConsumed, Double] = Map.empty[CoConsumed, Double]
    var items_i_count: Map[String, Double] = Map.empty[String, Double]
    var items_j_count: Map[String, Double] = Map.empty[String, Double]
    var item_dist: Map[String, Double] = Map.empty[String, Double]
    var items_i_count_unfiltered: Map[String, Double] = Map.empty[String, Double]
    var items_j_count_unfiltered: Map[String, Double] = Map.empty[String, Double]
    var item_dist_unfiltered: Map[String, Double] = Map.empty[String, Double]

    var grouped_pairs_train: Map[CoConsumed, Double] = Map.empty[CoConsumed, Double]
    var items_i_count_train: Map[String, Double] = Map.empty[String, Double]
    var items_j_count_train: Map[String, Double] = Map.empty[String, Double]
    var item_dist_train: Map[String, Double] = Map.empty[String, Double]
    var items_i_count_train_unfiltered: Map[String, Double] = Map.empty[String, Double]
    var items_j_count_train_unfiltered: Map[String, Double] = Map.empty[String, Double]
    var item_dist_train_unfiltered: Map[String, Double] = Map.empty[String, Double]

    println("<step> loading training datasets from the files")
    if (load_training_files) {
      val loaded_files = readTraining(factors_folder,env)
      grouped_pairs_train = loaded_files._1.collect().toMap
      items_i_count_train = loaded_files._2.collect().toMap
      items_j_count_train = loaded_files._3.collect().toMap
      item_dist_train = loaded_files._4.collect().toMap
      items_i_count_train_unfiltered = loaded_files._5.collect().toMap
      items_j_count_train_unfiltered = loaded_files._6.collect().toMap
      item_dist_train_unfiltered = loaded_files._7.collect().toMap
    }

    if (consider_time) {
      println("<step> processing the testing file")
      val result_testing = processDataSetWithTime(testing_file, implicit_ratings_from, filter_min_support, filter_max_support, env, streaming_env)
      grouped_pairs = result_testing._1
      items_i_count = result_testing._2
      items_j_count = result_testing._3
      item_dist = result_testing._4
      items_i_count_unfiltered = result_testing._5
      items_j_count_unfiltered = result_testing._6
      item_dist_unfiltered = result_testing._7

      if (!load_training_files) {
        println("<step> processing the training file")
        val result_training = processDataSetWithTime(training_file, implicit_ratings_from, filter_min_support_training, filter_max_support_training, env, streaming_env)
        grouped_pairs_train = result_training._1
        items_i_count_train = result_training._2
        items_j_count_train = result_training._3
        item_dist_train = result_training._4
        items_i_count_train_unfiltered = result_training._5
        items_j_count_train_unfiltered = result_training._6
        item_dist_train_unfiltered = result_training._7
      }
    }
    else {
      println("<step> processing the testing file")
      val result_testing = processDataSetWithoutTimeRandomSequence(testing_file, implicit_ratings_from, filter_min_support, filter_max_support, env, streaming_env)
      grouped_pairs = result_testing._1
      items_i_count = result_testing._2
      items_j_count = result_testing._3
      item_dist = result_testing._4
      items_i_count_unfiltered= result_testing._5
      items_j_count_unfiltered = result_testing._6
      item_dist_unfiltered = result_testing._7
      if (!load_training_files) {
        println("<step> processing the training file")
        val result_training = processDataSetWithoutTimeRandomSequence(training_file, implicit_ratings_from, filter_min_support_training, filter_max_support_training, env, streaming_env)
        grouped_pairs_train = result_training._1
        items_i_count_train = result_training._2
        items_j_count_train = result_training._3
        item_dist_train = result_training._4
        items_i_count_train_unfiltered = result_training._5
        items_j_count_train_unfiltered = result_training._6
        item_dist_train_unfiltered = result_training._7
      }
    }

    println("<step> Loading the content files")
    var items_content = Map.empty[String,ItemContent]
    if(content_file != "none"){
      items_content = ContentFeatures.parse_content_file(env,content_file)
    }

    var items_jaccard_file= Map.empty[String,Map[String,Double]]
    if(jaccard_file != "none"){
      items_jaccard_file = ContentFeatures.parse_similarity_file(env,jaccard_file)
    }

    var items_content_similarity_file = Map.empty[String,Map[String,Double]]
    if(content_similarity_file != "none"){
      items_content_similarity_file = ContentFeatures.parse_similarity_file(env,content_similarity_file)
    }

    ContentFeatures.save_items_content_features(items_content,factors_folder)

    saveProcessedDataset(factors_folder,"testing",grouped_pairs,items_i_count,items_j_count,item_dist,items_i_count_unfiltered,items_j_count_unfiltered,item_dist_unfiltered)

    println("<step> Loading the factors files")
    val pkifactors = new java.io.File(factors_folder).listFiles.filter(x=> x.getName.startsWith("pkifactors") && ! x.getName.startsWith("evaluation") && ! x.getName.endsWith(".pdf")).map(_.getName)

    val l_factors_data = pkifactors.par.map{
      pkifactor =>
        println("... Loading the file: " + pkifactor)
        pkifactor.replace(".csv","") -> readFactors(factors_folder + pkifactor)
    } .toMap

    //Calculate the maximum bias
    val l_Mb = l_factors_data.par.map{
      factors_data =>
        factors_data._1 -> factors_data._2.map(_._2.bias).max
    }

    println("<step> Filtering the data and preparing for evaluation")
    val factors_data_filter = l_factors_data.values.head
    // Filter the training pairs that contains items i
//    val grouped_pairs_train_filtered = grouped_pairs_train.view.filter(items_i_support.keys.toSet).toMap
//    val items_i_count_train_filtered = grouped_pairs_train_filtered.map(x => (x._1.i,x._2)).groupBy(_._1).mapValues(_.map(_._2).sum)
//    val items_j_count_train_filtered = grouped_pairs_train_filtered.map(x => (x._1.j,x._2)).groupBy(_._1).mapValues(_.map(_._2).sum)

    // Get the item support from the training dataset
    val items_i_grouped_training = grouped_pairs_train.map(x=>(x._1.i,x._2)).groupBy(_._1).mapValues(_.map(_._2).sum)
    val items_i_filtered_grouped_training = items_i_grouped_training.view.filter(x=> x._2 >= filter_min_support_training && x._2 <= filter_max_support_training).toMap
    val items_i_filtered_training = items_i_grouped_training.keys.toVector

    val filtered_grouped_pairs_training = grouped_pairs_train.view.filter(x=>items_i_filtered_grouped_training.contains(x._1.i)).toMap

    val filtered_grouped_pairs = grouped_pairs.filter(
      x =>
          factors_data_filter.contains(x._1.i) &&
          factors_data_filter.contains(x._1.j) &&
          items_i_count_train.contains(x._1.i) &&
          items_j_count_train.contains(x._1.j) &&
          items_i_filtered_grouped_training.contains(x._1.i)
      )

    // Get the counting of item j
    val items_j_grouped_training  = grouped_pairs_train.map(x=>(x._1.j,x._2)).groupBy(_._1).mapValues(_.map(_._2).sum)
    //val items_j_filtered_grouped_training  = filtered_grouped_pairs_training.map(x=>(x._1.j,x._2)).groupBy(_._1).mapValues(_.map(_._2).sum)
    val items_j_filtered_training = items_j_grouped_training.keys.toVector

    //NOTE: Use this to get random items considering their probability. DANGER: It's really slow
    val items_j = items_j_count_train.view.filter(x => factors_data_filter.contains(x._1)).par.flatMap(y=>List.fill(y._2.toInt)(y._1)).toVector
    //NOTE: Sampling by weighting
    //val items_j = item_dist_train.toSeq

    val total_items_j = items_j_count_train.values.sum

    val items_j_count_train_dist = items_j_count_train.par.map{ x => (x._1,x._2 / total_items_j)}.toList.sortBy(_._2)
    val items_j_count_train_dist_cum = items_j_count_train_dist.foldLeft((0.0, List[(String,Double)]())){
      (acu,i) => (i._2 + acu._1, (i._1 , i._2 + acu._1) :: acu._2)
    }._2

    val items_j_count_train_dist_map = items_j_count_train_dist_cum.par.map(
      x =>
        (x._1,x._2,scala.math.round(x._2 * 10000).toInt)
    ).groupBy(_._3).mapValues(x => x.map(y=>y._1 -> y._2).seq.toVector).seq

    val items_j_keys = items_j_count_train_dist_map.keySet.toVector

    val item_ij_training = items_i_count_train ++ items_j_count_train.map{ case (k,v) => k -> (v + items_i_count_train.getOrElse(k,0.0)) }

    //val item_i_total = items_i_count_train_unfiltered ++ items_i_count_unfiltered.map{ case (k,v) => k -> (v + items_i_count_train_unfiltered.getOrElse(k,0.0)) }

    //val item_j_total = items_j_count_train_unfiltered ++ items_j_count_unfiltered.map{ case (k,v) => k -> (v + items_j_count_train_unfiltered.getOrElse(k,0.0)) }

    //val item_total = item_i_total ++ item_j_total.map{ case (k,v) => k -> (v + item_i_total.getOrElse(k,0.0)) }

    //val pair_total = grouped_pairs_train ++ grouped_pairs.map{ case (k,v) => k -> (v + grouped_pairs_train.getOrElse(k,0.0)) }

    //  println("<step> counting item occurrance on dataset")
    //    val datafile_items = env.readCsvFile[(String, String, String)](training_file, fieldDelimiter=";",quoteCharacter='|').filter(_._3.toInt >= implicit_ratings_from).map{x=>(x._2,1.0)}.groupBy(0).sum(1).collect().view.filter(x=>item_ij_training.contains(x._1)).toMap
    //    val user_item_dataset = env.readCsvFile[(String, String, String)](training_file, fieldDelimiter=";",quoteCharacter='|').filter(_._3.toInt >= implicit_ratings_from).map{
    //      x=>(x._2,x._1)
    //    }.groupBy(0).reduceGroup(new GroupReduceFunction[(String, String), Map[String,List[String]]] {
    //      override def reduce(values: Iterable[(String, String)], out: Collector[Map[String,List[String]]]): Unit = {
    //        val l = values.asScala.toList
    //        out.collect(l.groupBy(_._1).mapValues(_.map(_._2).toList))
    //      }
    //    }).flatMap{x=>x.map{y=>y}}.collect().toMap

    println("<step> calculating the percentile rank for each pair")
    println("<step> total number of evaluating pairs: " + filtered_grouped_pairs.size.toString)

    val sorted_items_prob = items_j_count_train.filter(x => factors_data_filter.contains(x._1)).toList.sortBy(-_._2)
    val log_head = List[String]("LogDate","Case","Activity","Resource","Class","Method","Line","Start","End|PMLog|")
    logger.debug(log_head.mkString("\"","\",\"","\""))

//    val dataset_filtered_grouped_pairs = env.fromCollection(filtered_grouped_pairs)
//    val dataset_mean_percentile = dataset_filtered_grouped_pairs.map(
//          new RichMapFunction[String, Int] {
//          def map(in: String):Int = { in.toInt }
//    })

    println("<step> calculating the distances between the items and the reference")
    val max_ref = sorted_items_prob.view.take(l_reference_size.max).toList

    val map_items_references = (for(item <- item_ij_training.keys.toList; x <- max_ref) yield (item,x._1)).filter(x=> x._1 != x._2)

//    val sim_items_refs = map_items_references.subsets.toVector.filter(_.size == 2).map{x=> (x.toList.head,x.toList.tail.head)}
    val items_ref_set = items_content.filterKeys(max_ref.map(_._1).toSet.union(item_ij_training.keys.toSet))
    val sim_map_items_ref = ContentFeatures.compute_similarity(map_items_references.toVector,items_ref_set)

    val map_items_references_mods = map_items_references.par.map(
      pair =>
        pair -> calculate_modalities(pair._1,pair._2,filtered_grouped_pairs_training,item_ij_training,item_ij_training(pair._1),l_factors_data,l_Mb,sim_map_items_ref,items_jaccard_file, items_content_similarity_file)
    ).toMap

    val l_modalities_combination = l_factors_data.par.map{
      factors_data =>
        var mods = selected_modalities.toSet[String]
        if(mods.contains("pki")){
          mods = mods - "pki"
          mods = mods + factors_data._1
        }
        mods.subsets.map(_.toList).toList.filter(_.size >= 1)
    }.toList.flatten.toSet

    val l_modalities_combination_string = l_modalities_combination.filter(_.size == 1).map(_.mkString(""))

    val mu_i_r = l_modalities_combination_string .par.map{
      m =>
        m -> max_ref.seq.map{
          r =>
            val pair_i_r = for(x <- items_i_filtered_training) yield(x,r._1)
            val filtered_pair = map_items_references_mods.filterKeys(pair_i_r.toSet).map(_._2(m))

            r._1 -> filtered_pair.sum / filtered_pair.size
        }.toMap
    }.toMap

    val variance_i_r = l_modalities_combination_string .par.map{
      m =>
        val r_vectors = max_ref.seq.map{
          r =>
            val pair_i_r = for(x <- items_i_filtered_training) yield(x,r._1)
            r._1 -> map_items_references_mods.filterKeys(pair_i_r.toSet).map(_._2(m))
        }.toMap

        val v_results = max_ref.seq.map{
          r =>
            val r_vector = r_vectors(r._1)
            val v_res = for(x <- r_vector) yield(scala.math.pow(x - mu_i_r(m)(r._1),2))
            r._1 -> v_res.sum / v_res.size
        }.toMap

        m -> v_results
    }.toMap

    val mu_j_r = l_modalities_combination_string .par.map{
      m =>
        m -> max_ref.seq.map{
          r =>
            val pair_j_r = for(x <- items_j_filtered_training) yield(x,r._1)
            val filtered_pair = map_items_references_mods.filterKeys(pair_j_r.toSet).map(_._2(m))

            r._1 -> filtered_pair.sum / filtered_pair.size
        }.toMap
    }.toMap

    val variance_j_r = l_modalities_combination_string .par.map{
      m =>
        val r_vectors = max_ref.seq.map{
          r =>
            val pair_j_r = for(x <- items_j_filtered_training) yield(x,r._1)
            r._1 -> map_items_references_mods.filterKeys(pair_j_r.toSet).map(_._2(m))
        }.toMap

        val v_results = max_ref.seq.map{
          r =>
            val r_vector = r_vectors(r._1)
            val v_res = for(x <- r_vector) yield(scala.math.pow(x - mu_j_r(m)(r._1),2))
            r._1 -> v_res.sum / v_res.size
        }.toMap

        m -> v_results
    }.toMap

    println("<step> evaluating the pairs")

    val mean_percentile = filtered_grouped_pairs.toList.sortBy(_._2).par.map {
      pair =>
        val caseId = pair._1.i + "," + pair._1.j
        logIt(logger,"INFO","testing_pair",caseId,{

          
          val items = logIt(logger,"DEBUG","get_random_j_items",caseId,{
            val items_j_count_train_dist_cum_filtered = items_j_count_train_dist_map.filterNot(_ == pair._1.j)
            //We sample on the full dataset if the items vector is not that big, let's say 4 million.
            //Otherwise we better go tricky
            if(items_j.size < 1000000)
              List(pair._1.j) ::: scala.util.Random.shuffle(items_j.filterNot(_ == pair._1.j)).distinct.take(percentile_n).toList
            else
              List(pair._1.j) ::: takeFromCummulativeMap(items_j_count_train_dist_cum_filtered,immutable.Vector.empty[String], items_j_keys, percentile_n, new Random()).toList
          })

          //Use the ocurrance of i considering all the possible training pairs
          val ni_coocurrance = item_ij_training(pair._1.i)

          //val ni_dataset = datafile_items(pair._1.i)

          //Compute the similarity between i and the items
          val map_i_sample = for(item <- items) yield (pair._1.i,item)
          //    val sim_items_refs = map_items_references.subsets.toVector.filter(_.size == 2).map{x=> (x.toList.head,x.toList.tail.head)}
          val items_ref_set = items_content.filterKeys(max_ref.map(_._1).toSet.union(item_ij_training.keys.toSet))
          val sim_map_i_sample = ContentFeatures.compute_similarity(map_i_sample.toVector,items_ref_set)

          val measures =
            items.par.map {
              j =>
                (j, calculate_modalities(pair._1.i, j,filtered_grouped_pairs_training,item_ij_training,ni_coocurrance,l_factors_data,l_Mb,sim_map_i_sample,items_jaccard_file, items_content_similarity_file))
            }.toMap

          //Compute the fisher information of the different modalities
          //First compute the estimated value for each of the models
          val modalities = measures.head._2.keys.toList
          val j_size = measures.size
          //val j_size_freq = filtered_grouped_pairs.filterKeys(measures.map(r => new CoConsumed(pair._1.i,r._1)).toSet).values.sum

          val e_i_j = modalities.map(
            m =>
              (m, measures.map(y => y._2(m)).sum / j_size)
          ).toMap

          var map_fishers_combinations = Map.empty[String,Map[String,Map[String,Double]]]

          var max_ref_mod = ParMap.empty[String,ParMap[String,Map[String,Double]]]

          if(fisher){

            max_ref_mod = logIt(logger,"DEBUG","j_ref_modalities",caseId,{
              measures.par.map {
                j=>
                  val r_j_mods = max_ref.par.map{
                    r =>
                      (r._1, calculate_modalities(r._1,j._1,filtered_grouped_pairs_training,item_ij_training,ni_coocurrance,l_factors_data,l_Mb,sim_map_items_ref,items_jaccard_file, items_content_similarity_file))
                  }.toMap
                  (j._1,r_j_mods)
              }
            })

//            Remove the comments to try other fisher combinations
//            map_fishers_combinations += ("fisher_combination_with_variance_l2_norm" ->
//              l_modalities_combination.par.flatMap {
//                combination =>
//                  calculate_combination_with_variance_l2_norm(logger,caseId,combination,l_reference_size,max_ref,measures,max_ref_mod,e_i_j)
//              }.toMap.seq)
//
//            map_fishers_combinations += ("fisher_combination_with_variance_l1" ->
//              l_modalities_combination.par.flatMap {
//                combination =>
//                  calculate_combination_with_variance_l1(logger,caseId,combination,l_reference_size,max_ref,measures,max_ref_mod,e_i_j)
//              }.toMap.seq)
//
            map_fishers_combinations += ("fisher_combination_without_variance_l2_norm" ->
              l_modalities_combination.par.flatMap {
                combination =>
                  calculate_combination_without_variance_l2_norm(logger,caseId,combination,l_reference_size,max_ref,measures,max_ref_mod,e_i_j)
              }.toMap.seq)
//
//            map_fishers_combinations += ("fisher_combination_without_variance_l1_norm" ->
//              l_modalities_combination.par.flatMap {
//                combination =>
//                    calculate_combination_without_variance_l1_norm(logger,caseId,combination,l_reference_size,max_ref,measures,max_ref_mod,e_i_j)
//              }.toMap.seq)

            val individual_fisher_kernels = l_modalities_combination.filter(_.size == 1).flatMap {
                combination =>
                  combination.map{
                    c =>
                      c -> calculate_kernel(logger,caseId,c,pair._1.i,measures,map_items_references_mods,mu_i_r,variance_i_r,mu_j_r,variance_j_r,l_reference_size,max_ref,l_normalizations)
                  }
              }.toMap

            map_fishers_combinations += ("fisher_kernels" ->
              l_modalities_combination.flatMap{
                combination =>
                  l_reference_size.par.flatMap{
                    reference_size =>
                      l_normalizations.map {
                        normalization =>
                          val comb_name = "fisher_kernels_" + reference_size.toString + "_" + combination.mkString("_") + "_" + normalization
                          val kernel_modalities = combination.map {
                              c =>
                                individual_fisher_kernels(c)(reference_size,normalization)
                            }
                          val combs = items.map {
                            item =>
                              item -> kernel_modalities.map {
                                k=>
                                  k(item)
                              }.sum
                          }.toMap
                          comb_name -> combs
                      }
                  }
              }.toMap)

//            map_fishers_combinations += ("fisher_combination_without_variance_l2_norm_new" ->
//              l_modalities_combination.par.flatMap {
//                combination =>
//                  calculate_combination_without_variance_l2_norm_new(logger,caseId,pair._1.i,combination,l_reference_size,max_ref,measures,max_ref_mod,map_items_references_mods,e_i_j)
//              }.toMap.seq)
//
//            map_fishers_combinations += ("fisher_combination_with_variance_l2_norm_new" ->
//              l_modalities_combination.par.flatMap {
//                combination =>
//                  calculate_combination_with_variance_l2_norm_new(logger,caseId,pair._1.i,combination,l_reference_size,max_ref,measures,max_ref_mod,map_items_references_mods,e_i_j)
//              }.toMap.seq)

          }

          val all_fishers = map_fishers_combinations.keys.toVector.flatMap{
            fs =>
              map_fishers_combinations(fs).keys.map{
                fisher_combination =>
                  val list_scores = map_fishers_combinations(fs)(fisher_combination)
                  fisher_combination -> list_scores
              }
          }.toMap

          val fisher_keys = all_fishers.keys.toList
          val modalities_combinations = for(f <- fisher_keys; m <- modalities.toList) yield(List(f,m))
          val fisher_combinations = (for(f1 <- fisher_keys; f2 <- fisher_keys; if f1 != f2) yield(List(f1,f2).sorted)).distinct
          val fisher_combinations_baselines = for(f1 <- fisher_combinations; m <- modalities.toList) yield(m :: f1)
          val combinations = modalities_combinations ::: fisher_combinations ::: fisher_combinations_baselines

          //5.1 Comine by simple sum
          var combined_lists_simple_sum = ParMap.empty[String,Map[String,Double]]
          if(l_list_combinations.contains("simple_sum")){
            combined_lists_simple_sum = combinations.par.map{
              combination =>
                val name = "lc_simple_sum_" + combination.mkString("+")

                val combined_items = items.map{
                  item =>
                    val combination_result = combination.map{
                      m =>
                        var item_score = 0.0
                        if(m.indexOf("fisher") > -1)
                          item_score = all_fishers(m)(item)
                        else
                          item_score = measures(item)(m)
                        item_score
                    }.sum
                    item -> combination_result
                }.toMap
                name -> combined_items
            }.toMap
          }

          //5.2 Combine the modalities per item with the formula (x-m_x)/var_x + (y-m_y)/var_y ... where x is ecp, y is fisher, etc
          //Expected value and variance per modality
          var combined_lists_normalized_sum = ParMap.empty[String,Map[String,Double]]
          if(l_list_combinations.contains("normalized_sum")) {
            val modality_ev = modalities.map {
              m =>
                val list_scores = measures.map(y => (y._1, y._2(m)))
                m -> list_scores.values.sum / list_scores.keys.size
            }.toMap

            val modality_var = modalities.map {
              m =>
                val list_scores = measures.map(y => (y._1, y._2(m)))
                val diffs = list_scores.map(x => scala.math.pow(x._2 - modality_ev(m), 2))
                m -> diffs.sum / diffs.size
            }.toMap

            val fishers_ev = map_fishers_combinations.keys.toVector.flatMap {
              fs =>
                map_fishers_combinations(fs).keys.map {
                  fisher_combination =>
                    val list_scores = map_fishers_combinations(fs)(fisher_combination).seq
                    fisher_combination -> list_scores.values.sum / list_scores.keys.size
                }
            }.toMap

            val fishers_var = map_fishers_combinations.keys.toVector.flatMap {
              fs =>
                map_fishers_combinations(fs).keys.map {
                  fisher_combination =>
                    val list_scores = map_fishers_combinations(fs)(fisher_combination).seq
                    val diffs = list_scores.map(x => scala.math.pow(x._2 - fishers_ev(fisher_combination), 2))
                    fisher_combination -> list_scores.values.sum / list_scores.keys.size
                }
            }.toMap

            combined_lists_normalized_sum = combinations.par.map {
              combination =>
                val name = "lc_normalized_sum_" + combination.mkString("+")

                val combined_items = items.map {
                  item =>
                    val combination_result = combination.map {
                      m =>
                        var item_score = 0.0
                        if (m.indexOf("fisher") > -1)
                          item_score = (all_fishers(m)(item) - fishers_ev(m)) / fishers_var(m)
                        else
                          item_score = (measures(item)(m) - modality_ev(m)) / modality_var(m)
                        item_score
                    }.sum
                    item -> combination_result
                }.toMap
                name -> combined_items
            }.toMap
          }

          //5.3 Combine the modalities by rank
          var combined_lists_rank = ParMap.empty[String,Map[String,Double]]
          if(l_list_combinations.contains("ranking")) {
            combined_lists_rank = combinations.par.map {
              combination =>
                val name = "lc_rank_" + combination.mkString("+")

                val combination_ranks = combination.map {
                  m =>
                    var list_scores = Map.empty[String, Double]
                    if (m.indexOf("fisher") > -1)
                      list_scores = all_fishers(m)
                    else
                      list_scores = measures.map(y => (y._1, y._2(m))).seq

                    val list_scores_vector = list_scores.toVector
                    var list_scores_order = IndexedSeq.empty[String]

                    if (m.indexOf("fisher") > -1 || m.indexOf("pki") > -1)
                      list_scores_order = list_scores_vector.seq.sortBy(_._2).map(_._1)
                    else
                      list_scores_order = list_scores_vector.seq.sortBy(-_._2).map(_._1)

                    m -> list_scores_order
                }.toMap

                val combined_items = items.map {
                  item =>
                    val combination_result = combination.map {
                      m =>
                        combination_ranks(m).indexOf(item) + 1.0
                    }.sum
                    item -> combination_result
                }.toMap
                name -> combined_items
            }.toMap
          }

          val j = pair._1.j

          var result: SortedMap[String, Double] = SortedMap.empty[String, Double]

          logIt(logger,"DEBUG","percentile_rank",caseId, {
            for (modality <- modalities.toList) {
              val list_scores = measures.map(y => (y._1, y._2(modality))).seq
              val list_scores_vector = list_scores.toVector
              val list_scores_descendent = list_scores_vector.sortBy(-_._2)
              val list_scores_ascendent = list_scores_vector.sortBy(_._2)

              if (modality.contains("pki") || modality.contains("ecp")){
                result += modality + "_mpr" -> percentile_rank(j, list_scores, false,items_j_count_train, mpr_sum_training)
                l_recall_at.foreach(
                  recall_at =>
                    result += modality + "_recall@" + recall_at.toString -> recall(list_scores_ascendent.take(recall_at),j)
                )
                l_dcg_at.foreach(
                  dcg_at =>
                    result += modality + "_dcg@" + dcg_at.toString -> dcg(list_scores_descendent.take(dcg_at),j)
                )
              }
              else{
                result += modality + "_mpr" -> percentile_rank(j, measures.map(y => (y._1, y._2(modality))).seq, true,items_j_count_train, mpr_sum_training)
                l_recall_at.foreach(
                  recall_at =>
                    result += modality + "_recall@" + recall_at.toString -> recall(list_scores_descendent.take(recall_at),j)
                )
                l_dcg_at.foreach(
                  dcg_at =>
                    result += modality + "_dcg@" + dcg_at.toString -> dcg(list_scores_descendent.take(dcg_at),j)
                )
              }
            }

            for (modality <- combined_lists_simple_sum.keys.toList) {
              val list_scores = combined_lists_simple_sum(modality).seq
              val list_scores_vector = list_scores.toVector
              val list_scores_descendent = list_scores_vector.sortBy(-_._2)
              val list_scores_ascendent = list_scores_vector.sortBy(_._2)

              if (modality.contains("pki") || modality.contains("ecp")) {
                result += modality + "_mpr" -> percentile_rank(j, list_scores, false, items_j_count_train, mpr_sum_training)
                l_recall_at.foreach(
                  recall_at =>
                    result += modality + "_recall@" + recall_at.toString -> recall(list_scores_ascendent.take(recall_at), j)
                )
                l_dcg_at.foreach(
                  dcg_at =>
                    result += modality + "_dcg@" + dcg_at.toString -> dcg(list_scores_descendent.take(dcg_at), j)
                )
              }
              else {
                result += modality + "_mpr" -> percentile_rank(j, list_scores, true, items_j_count_train, mpr_sum_training)
                l_recall_at.foreach(
                  recall_at =>
                    result += modality + "_recall@" + recall_at.toString -> recall(list_scores_descendent.take(recall_at), j)
                )
                l_dcg_at.foreach(
                  dcg_at =>
                    result += modality + "_dcg@" + dcg_at.toString -> dcg(list_scores_descendent.take(dcg_at), j)
                )
              }
            }

            for (modality <- combined_lists_normalized_sum.keys.toList) {
              val list_scores = combined_lists_normalized_sum(modality).seq
              val list_scores_vector = list_scores.toVector
              val list_scores_descendent = list_scores_vector.sortBy(-_._2)
              val list_scores_ascendent = list_scores_vector.sortBy(_._2)

              if (modality.contains("pki") || modality.contains("ecp")) {
                result += modality + "_mpr" -> percentile_rank(j, list_scores, false, items_j_count_train, mpr_sum_training)
                l_recall_at.foreach(
                  recall_at =>
                    result += modality + "_recall@" + recall_at.toString -> recall(list_scores_ascendent.take(recall_at), j)
                )
                l_dcg_at.foreach(
                  dcg_at =>
                    result += modality + "_dcg@" + dcg_at.toString -> dcg(list_scores_descendent.take(dcg_at), j)
                )
              }
              else {
                result += modality + "_mpr" -> percentile_rank(j, list_scores, true, items_j_count_train, mpr_sum_training)
                l_recall_at.foreach(
                  recall_at =>
                    result += modality + "_recall@" + recall_at.toString -> recall(list_scores_descendent.take(recall_at), j)
                )
                l_dcg_at.foreach(
                  dcg_at =>
                    result += modality + "_dcg@" + dcg_at.toString -> dcg(list_scores_descendent.take(dcg_at), j)
                )
              }
            }

            for (modality <- combined_lists_rank.keys.toList) {
              val list_scores = combined_lists_rank(modality).seq
              val list_scores_vector = list_scores.toVector
              val list_scores_descendent = list_scores_vector.sortBy(-_._2)
              val list_scores_ascendent = list_scores_vector.sortBy(_._2)

              result += modality + "_mpr" -> percentile_rank(j, list_scores, false, items_j_count_train, mpr_sum_training)
              l_recall_at.foreach(
                recall_at =>
                  result += modality + "_recall@" + recall_at.toString -> recall(list_scores_ascendent.take(recall_at), j)
              )
              l_dcg_at.foreach(
                dcg_at =>
                  result += modality + "_dcg@" + dcg_at.toString -> dcg(list_scores_descendent.take(dcg_at), j)
              )
            }

            if(fisher){
              map_fishers_combinations.keys.toList.foreach(
                fs =>
                  for (fisher_combination <- map_fishers_combinations(fs).keys.toList) {
                    val list_scores = map_fishers_combinations(fs)(fisher_combination).seq
                    val list_scores_vector = list_scores.toVector
                    val list_scores_descendent = list_scores_vector.sortBy(-_._2)
                    val list_scores_ascendent = list_scores_vector.sortBy(_._2)

                    if(fisher_combination.indexOf("fisher_kernels") > -1)
                      result += fisher_combination + "_mpr" -> percentile_rank(j, list_scores, true ,items_j_count_train , mpr_sum_training)
                    else
                      result += fisher_combination + "_mpr" -> percentile_rank(j, list_scores, false,items_j_count_train , mpr_sum_training)

                    l_recall_at.foreach(
                      recall_at =>
                        result += fisher_combination + "_recall@" + recall_at.toString -> recall(list_scores_descendent.take(recall_at),j)
                    )
                    l_dcg_at.foreach(
                      dcg_at =>
                        result += fisher_combination + "_dcg@" + dcg_at.toString -> dcg(list_scores_descendent.take(dcg_at),j)
                    )
                  }
              )
            }
          })
          logger.info("EVALUATION_RESULT," + pair.toString + "," + filtered_grouped_pairs.getOrElse(pair._1,0.0).toString + "," + items_i_count_train(pair._1.i).toString + "," + items_i_count(pair._1.i).toString + "," + items_j_count_train(pair._1.j).toString + "," + items_j_count(pair._1.j).toString + "," + result.mkString(","))
          //"i","j","pair_freq_test","pair_freq_train","freq_i_train","freq_i_test","freq_j_train","freq_j_test"
          (pair._1,pair._2,filtered_grouped_pairs.getOrElse(pair._1,0.0),items_i_count_train(pair._1.i), items_i_count(pair._1.i),items_j_count_train(pair._1.j), items_j_count(pair._1.j)) -> (result)})
    }.toMap
    println("<step> saving the results")
    saveEvaluation(factors_folder + "evaluation.csv", mean_percentile.seq)
    saveEvaluationSummary(factors_folder + "evaluation_summary.csv", mean_percentile.seq)
    println("<step> finished evaluation")

  }
}
