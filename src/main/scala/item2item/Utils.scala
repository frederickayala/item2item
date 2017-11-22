package item2item

import java.lang.Iterable
import javax.ws.rs.DefaultValue
import breeze._
import breeze.linalg._
import breeze.numerics._
import EuclideanItemRecommenderTrainer._
import org.apache.flink.api.common.functions.{GroupReduceFunction, _}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.common.operators.Order
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap
import scala.collection.immutable
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.SortedMap
import scala.collection.parallel.ParIterable
import scala.collection.parallel.immutable.ParMap
import scala.util.Random
import java.nio.file.{Path}
import java.nio.charset.{StandardCharsets}
import scala.collection.immutable.HashMap
import breeze.stats.distributions.Rand
import com.github.tototoshi.csv._
import java.io._
import java.util.concurrent.ThreadLocalRandom
import com.typesafe.scalalogging.Logger
import java.util.Date
import java.text.SimpleDateFormat

/**
 * Created by Frederick Ayala-Gomez
 */
object Utils {

  case class CoConsumed(i: String, j: String)

  case class Factor(id: String, bias: Double, factors: linalg.DenseVector[Double])

  def takeFromCummulativeMap(tree: scala.collection.Map[Int,scala.collection.immutable.Vector[(String,Double)]]
                             , items: scala.collection.immutable.Vector[String]
                             , keys:scala.collection.immutable.Vector[Int],
                             remaining: Int, rand: Random): scala.collection.immutable.Vector[String] = {
    if (remaining == 0) items
    else{
      val bucket = keys(rand.nextInt(keys.size))
      val to_shuffle = tree(bucket).filterNot(x => items.contains(x._1))
      if(to_shuffle.length == 1)
        takeFromCummulativeMap(tree, items :+ to_shuffle.head._1, keys.filterNot(_ == bucket), remaining - 1,rand)
      else{
        val item = scala.util.Random.shuffle(to_shuffle).head._1
        takeFromCummulativeMap(tree, items :+ item, keys, remaining - 1,rand)
      }
    }
  }

  def getContextData(): Array[String]={
    val t =Thread.currentThread
    val tid =t.getId.toString
    val st = t.getStackTrace()(3)
    Array[String](tid,st.getClassName,st.getMethodName,st.getLineNumber.toString)
  }

  def saveResults(resultsDir: Path,epoch:Int, filename: String, factors:Map[String,Factor]): Unit ={
    implicit object MyFormat extends DefaultCSVFormat {
      override val delimiter = ';'
      override val quoteChar = '|'
      override val escapeChar = '|'
      override val quoting = QUOTE_ALL
    }

    val f = new File(resultsDir.toString + "/" + filename.replace(".","_").replace("-","_") + ".csv")
    val writer = CSVWriter.open(f)
    factors.foreach{
      x =>
        writer.writeRow(List(x._2.id,x._2.bias) ::: x._2.factors.toScalaVector().toList)
    }
    writer.close()
  }

  def saveEvaluationSummary(filename: String, mean_percentile:Map[(CoConsumed,Double,Double,Double,Double,Double,Double),SortedMap[String,Double]]): Unit ={
    implicit object MyFormat extends DefaultCSVFormat {
      override val delimiter = ';'
      override val quoteChar = '|'
      override val escapeChar = '|'
      override val quoting = QUOTE_ALL
    }

    val modalities = mean_percentile.head._2.keys
    val test_sum = mean_percentile.par.map(_._1._2).sum

    val mprs = modalities.par.map {
      modality =>
        val mod_sum = mean_percentile.par.map { x => x._2(modality) * x._1._2 }.sum
        val mod_mean = mod_sum / test_sum
        println(modality + ": " + mod_mean.toString)
        modality -> mod_mean
    }.toMap

    val f = new File(filename)
    val writer = CSVWriter.open(f)
    writer.writeRow(mprs.keys.toList)
    writer.writeRow(mprs.values.toList)
    writer.close()
  }

  def saveEvaluation(filename: String, mean_percentile:Map[(CoConsumed,Double,Double,Double,Double,Double,Double),SortedMap[String,Double]]): Unit ={
    implicit object MyFormat extends DefaultCSVFormat {
      override val delimiter = ';'
      override val quoteChar = '|'
      override val escapeChar = '|'
      override val quoting = QUOTE_ALL
    }

    val f = new File(filename)
    val writer = CSVWriter.open(f)
    writer.writeRow(List[String]("i","j","pair_freq_test","pair_freq_train","freq_i_train","freq_i_test","freq_j_train","freq_j_test") ::: mean_percentile.head._2.keys.toList)

    mean_percentile.foreach{
      x =>
        writer.writeRow(List(x._1._1.i,x._1._1.j,x._1._2,x._1._3,x._1._4,x._1._5,x._1._6,x._1._7) ::: x._2.values.toList)
    }
    writer.close()
  }

  def saveProcessedDataset(path:String, filetype:String, grouped_pairs:Map[CoConsumed,Double],items_i_count:Map[String,Double],items_j_count:Map[String,Double],items_dist:Map[String,Double],items_i_count_unfiltered:Map[String,Double],items_j_count_unfiltered:Map[String,Double],items_dist_unfiltered:Map[String,Double]) = {
    implicit object MyFormat extends DefaultCSVFormat {
      override val delimiter = ';'
      override val quoteChar = '|'
      override val escapeChar = '|'
      override val quoting = QUOTE_ALL
    }

    var f = new File(path + filetype + "_grouped_pairs.csv")
    var writer = CSVWriter.open(f)
    grouped_pairs.foreach{
      x =>
        writer.writeRow(List(x._1.i,x._1.j,x._2))
    }
    writer.close()

    f = new File(path + filetype + "_items_i_count.csv")
    writer = CSVWriter.open(f)
    items_i_count.foreach{
      x =>
        writer.writeRow(List(x._1,x._2))
    }
    writer.close()

    f = new File(path + filetype + "_items_j_count.csv")
    writer = CSVWriter.open(f)
    items_j_count.foreach{
      x =>
        writer.writeRow(List(x._1,x._2))
    }

    writer.close()

    f = new File(path + filetype + "_items_dist.csv")
    writer = CSVWriter.open(f)
    items_dist.foreach{
      x =>
        writer.writeRow(List(x._1,x._2))
    }

    writer.close()

    f = new File(path + filetype + "_items_i_count_unfiltered.csv")
    writer = CSVWriter.open(f)
    items_i_count_unfiltered.foreach{
      x =>
        writer.writeRow(List(x._1,x._2))
    }
    writer.close()

    f = new File(path + filetype + "_items_j_count_unfiltered.csv")
    writer = CSVWriter.open(f)
    items_j_count_unfiltered.foreach{
      x =>
        writer.writeRow(List(x._1,x._2))
    }

    writer.close()

    f = new File(path + filetype + "_items_dist_unfiltered.csv")
    writer = CSVWriter.open(f)
    items_dist_unfiltered.foreach{
      x =>
        writer.writeRow(List(x._1,x._2))
    }

    writer.close()
  }

  def readTraining(factors_folder: String,env: ExecutionEnvironment):(DataSet[(CoConsumed,Double)],DataSet[(String,Double)],DataSet[(String,Double)],DataSet[(String,Double)],DataSet[(String,Double)],DataSet[(String,Double)],DataSet[(String,Double)])={

    def parseLineStringDouble(line:String):(String, Double) = {
      val split = line.split(";").map{_.stripPrefix("|").stripSuffix("|")}
      (split(0),split(1).toDouble)
    }

    val grouped_pairs = env.readTextFile(factors_folder + "training_grouped_pairs.csv").map{
      x =>
        val split = x.split(";").map{_.stripPrefix("|").stripSuffix("|")}
        new CoConsumed(split(0),split(1)) -> split(2).toDouble
    }

    val items_i_count = env.readTextFile(factors_folder + "training_items_i_count.csv").map{parseLineStringDouble(_)}
    val items_j_count = env.readTextFile(factors_folder + "training_items_j_count.csv").map{parseLineStringDouble(_)}
    val items_dist = env.readTextFile(factors_folder + "training_items_dist.csv").map{parseLineStringDouble(_)}
    val items_i_count_unfiltered = env.readTextFile(factors_folder + "training_items_i_count_unfiltered.csv").map{parseLineStringDouble(_)}
    val items_j_count_unfiltered = env.readTextFile(factors_folder + "training_items_j_count_unfiltered.csv").map{parseLineStringDouble(_)}
    val items_dist_unfiltered = env.readTextFile(factors_folder + "training_items_dist_unfiltered.csv").map{parseLineStringDouble(_)}

    (grouped_pairs,items_i_count,items_j_count,items_dist,items_i_count_unfiltered,items_j_count_unfiltered,items_dist_unfiltered)
  }

  def readFactors(filename: String): Map[String,Factor] ={
    implicit object MyFormat extends DefaultCSVFormat {
      override val delimiter = ';'
      override val quoteChar = '|'
      override val escapeChar = '|'
      override val quoting = QUOTE_ALL
    }

    val reader = CSVReader.open(filename)
    val data:List[List[String]] = reader.all()
    val num_factors = data(0).size
    val factors = data.par.map{
      x=>
        x(0) -> new Factor(x(0),x(1).toDouble,DenseVector(x.slice(2,num_factors).par.map(_.toDouble).toArray))
    }
    factors.seq.toMap
  }

  def randomFactors(num_factors: Int, items: immutable.Vector[String], random: Rand[Double]): Map[String,Factor] = {
    items.par.map(
      item =>
        item -> new Factor(item, random.get(), linalg.DenseVector.rand(num_factors,random))
    ).seq.toMap
  }

  def processDataSetWithTime(option_datafile:String,implicit_ratings_from:Int, filter_pairs_frequencies_min:Int,filter_pairs_frequencies_max:Int, env:ExecutionEnvironment,streaming_env:StreamExecutionEnvironment):
  (Map[CoConsumed,Double],Map[String,Double],Map[String,Double],Map[String,Double],Map[String,Double],Map[String,Double],Map[String,Double]) ={
      val datafile = env.readCsvFile[(String, String, String, String)](option_datafile, fieldDelimiter=";",quoteCharacter='|').filter(_._3.toInt >= implicit_ratings_from)
      val ordered_user_timestamp = datafile.sortPartition(0,Order.ASCENDING).sortPartition(3,Order.ASCENDING).sortPartition(1,Order.ASCENDING)
        .groupBy(0).reduceGroup(new GroupReduceFunction[(String, String, String, String), List[CoConsumed]] {
        override def reduce(values: Iterable[(String, String, String, String)], out: Collector[List[CoConsumed]]): Unit = {
          val sorted_values = values.asScala.toList.sortBy(x=> (x._4,x._2))
          val pairs = (new CoConsumed(sorted_values.takeRight(1)(0)._2 , sorted_values.head._2)) :: sorted_values.sliding(2).filter(_.size > 1 ).map(x => new CoConsumed(x.head._2, x.tail.head._2)).toList
          out.collect(pairs.view.filter(x => x.i != x.j).toList)
        }
      }).flatMap{x=>x.map{y=>y}}

      //    To filter by support of the conditioned item i in P(j|i)
      val support_i = ordered_user_timestamp.map{x=>(x.i,1.0)}.groupBy(0).sum(1).filter(x=> x._2 >= filter_pairs_frequencies_min && x._2 <= filter_pairs_frequencies_max)
      val grouped_pairs = ordered_user_timestamp.join(support_i).where(_.i).equalTo(_._1).map{x=>(x._1,1.0)}.groupBy(0).sum(1).collect().toMap[CoConsumed,Double]
      val grouped_pairs_unfiltered = ordered_user_timestamp.map{x=>(x,1.0)}.groupBy(0).sum(1).collect().toMap[CoConsumed,Double]
      //    To filter pairs ocurring together at least
      //    val grouped_pairs = ordered_user_timestamp.map{x=>(x,1.0)}.groupBy(0).sum(1).filter(_._2 > filter_pairs_frequencies).collect().toMap[CoConsumed,Double]

      //Compute the distribution of the items from the original dataset
      val i_ocurrance_unfiltered = grouped_pairs_unfiltered.groupBy(_._1.i).mapValues(_.map(_._2).sum)
      val j_ocurrance_unfiltered = grouped_pairs_unfiltered.groupBy(_._1.j).mapValues(_.map(_._2).sum)
      val item_ocurrance_unfiltered = i_ocurrance_unfiltered ++ j_ocurrance_unfiltered.map{ case (k,v) => k -> (v + i_ocurrance_unfiltered.getOrElse(k,0.0)) }
      val total_item_ocurrance_unfiltered = item_ocurrance_unfiltered.values.sum
      val items_dist_unfiltered = item_ocurrance_unfiltered.par.map(x => x._1 -> x._2 / total_item_ocurrance_unfiltered).seq

      //Compute the distribution of the items from the filtered dataset
      val i_ocurrance = grouped_pairs.groupBy(_._1.i).mapValues(_.map(_._2).sum)
      val j_ocurrance = grouped_pairs.groupBy(_._1.j).mapValues(_.map(_._2).sum)
      val item_ocurrance = i_ocurrance ++ j_ocurrance.map{ case (k,v) => k -> (v + i_ocurrance.getOrElse(k,0.0)) }
      val total_item_ocurrance = item_ocurrance.values.sum
      val items_dist = item_ocurrance.par.map(x => x._1 -> x._2 / total_item_ocurrance).seq

      (grouped_pairs,i_ocurrance.seq,j_ocurrance.seq,items_dist,i_ocurrance_unfiltered.seq,j_ocurrance_unfiltered.seq,items_dist_unfiltered)
  }

  def processDataSetWithoutTimeRandomSequence(option_datafile:String,implicit_ratings_from:Int, filter_pairs_frequencies_min:Int, filter_pairs_frequencies_max:Int, env:ExecutionEnvironment,streaming_env:StreamExecutionEnvironment):
  (Map[CoConsumed,Double],Map[String,Double],Map[String,Double],Map[String,Double],Map[String,Double],Map[String,Double],Map[String,Double]) ={
    val datafile = env.readCsvFile[(String, String, String)](option_datafile, fieldDelimiter=";",quoteCharacter = '|').filter(_._3.toInt >= implicit_ratings_from)
    val ordered_user_timestamp = datafile.sortPartition(0,Order.ASCENDING).groupBy(0).reduceGroup(new GroupReduceFunction[(String, String, String), List[CoConsumed]] {
      override def reduce(values: Iterable[(String, String, String)], out: Collector[List[CoConsumed]]): Unit = {
        val sorted_values = values.asScala.toList
        val pairs = (new CoConsumed(sorted_values.takeRight(1)(0)._2 , sorted_values.head._2)) :: sorted_values.sliding(2).filter(_.size  >1 ).map(x => new CoConsumed(x.head._2, x.tail.head._2)).toList
        out.collect(pairs.view.filter(x => x.i != x.j).toList)
      }
    }).flatMap{x=>x.map{y=>y}}

    //    To filter by support of the conditioned item i in P(j|i)
    val support_i = ordered_user_timestamp.map{x=>(x.i,1.0)}.groupBy(0).sum(1).filter(x=> x._2 >= filter_pairs_frequencies_min && x._2 <= filter_pairs_frequencies_max)
    val grouped_pairs = ordered_user_timestamp.join(support_i).where(_.i).equalTo(_._1).map{x=>(x._1,1.0)}.groupBy(0).sum(1).collect().toMap[CoConsumed,Double]
    val grouped_pairs_unfiltered = ordered_user_timestamp.map{x=>(x,1.0)}.groupBy(0).sum(1).collect().toMap[CoConsumed,Double]
    //    To filter pairs ocurring together at least
    //    val grouped_pairs = ordered_user_timestamp.map{x=>(x,1.0)}.groupBy(0).sum(1).filter(_._2 > filter_pairs_frequencies).collect().toMap[CoConsumed,Double]

    //Compute the distribution of the items from the original dataset
    val i_ocurrance_unfiltered = grouped_pairs_unfiltered.groupBy(_._1.i).mapValues(_.map(_._2).sum)
    val j_ocurrance_unfiltered = grouped_pairs_unfiltered.groupBy(_._1.j).mapValues(_.map(_._2).sum)
    val item_ocurrance_unfiltered = i_ocurrance_unfiltered ++ j_ocurrance_unfiltered.map{ case (k,v) => k -> (v + i_ocurrance_unfiltered.getOrElse(k,0.0)) }
    val total_item_ocurrance_unfiltered = item_ocurrance_unfiltered.values.sum
    val items_dist_unfiltered = item_ocurrance_unfiltered.par.map(x => x._1 -> x._2 / total_item_ocurrance_unfiltered).seq

    //Compute the distribution of the items from the filtered dataset
    val i_ocurrance = grouped_pairs.groupBy(_._1.i).mapValues(_.map(_._2).sum)
    val j_ocurrance = grouped_pairs.groupBy(_._1.j).mapValues(_.map(_._2).sum)
    val item_ocurrance = i_ocurrance ++ j_ocurrance.map{ case (k,v) => k -> (v + i_ocurrance.getOrElse(k,0.0)) }
    val total_item_ocurrance = item_ocurrance.values.sum
    val items_dist = item_ocurrance.par.map(x => x._1 -> x._2 / total_item_ocurrance).seq

    (grouped_pairs,i_ocurrance.seq,j_ocurrance.seq,items_dist,i_ocurrance_unfiltered.seq,j_ocurrance_unfiltered.seq,items_dist_unfiltered)
  }

  def processDataSetWithoutTimeAllPairs(option_datafile:String,implicit_ratings_from:Int, filter_pairs_frequencies:Int, env:ExecutionEnvironment,streaming_env:StreamExecutionEnvironment):
  (Map[CoConsumed,Double],Map[String,Double],Map[String,Double],Map[String,Double]) ={
    val datafile = env.readCsvFile[(String, String, String)](option_datafile, fieldDelimiter=";",quoteCharacter = '|').filter(_._3.toInt >= implicit_ratings_from)
    val grouped_pairs = datafile.join(datafile).where(0).equalTo(0).filter{x=>x._1._2 != x._2._2}.map(
      x =>
        if(x._1._2 > x._2._2){
          (CoConsumed(x._1._2,x._2._2),1.0)
        }else{
          (CoConsumed(x._2._2,x._1._2),1.0)
        }
    ).groupBy(0).sum(1).filter(_._2 > filter_pairs_frequencies).collect().toMap[CoConsumed, Double]

    //Compute the distribution of the items
    val i_ocurrance = grouped_pairs.keys.map(x => (x.i, grouped_pairs(x))).groupBy(_._1).mapValues(_.map(_._2).sum)
    val j_ocurrance = grouped_pairs.keys.map(x => (x.j, grouped_pairs(x))).groupBy(_._1).mapValues(_.map(_._2).sum)
    val item_ocurrance = i_ocurrance ++ j_ocurrance.map { case (k, v) => k -> (v + i_ocurrance.getOrElse(k, 0.0)) }
    val total_items = item_ocurrance.values.sum
    val items_dist = item_ocurrance.map(x => x._1 -> x._2 / total_items)
    var cumm_dist:Map[String,Double] = Map.empty[String,Double]
    var prob = 0.0

    for(xs <- items_dist.toList.sortBy(-_._2)){
      if(prob == 0.0)
        prob = xs._2
      else
        prob += xs._2
      cumm_dist += (xs._1 -> prob)
    }

    (grouped_pairs,cumm_dist,items_dist,item_ocurrance)
  }

  def p_j_i(i: Factor, j: Factor, factors:Map[String,Factor]): Double = {
    val bias_j = j.bias

    val sum_i_k = factors.filter(x => x._1 != i.id).par.map {
      k =>
        -linalg.squaredDistance(i.factors, k._2.factors) + k._2.bias
    }

    val max_sum_i_k = sum_i_k.max

    val fixed_sum_i_k = sum_i_k.map {
      distance =>
        exp(distance - max_sum_i_k)
    }.sum
    val res = exp(-linalg.squaredDistance(i.factors, j.factors) + bias_j) / fixed_sum_i_k
    if(res.isNaN || res.isInfinite)
      throw new Exception("ERROR: p_j_i is either Nan or Infinite")
    res
  }

  def w_k_i(i: Factor, k: Factor, yL:Map[String,Factor], item_dist:Map[String,Double]): Double = {
    val P_k_D = item_dist.view.filter(_._1 == k.id).head._2
    val bias_k = k.bias

    val sum_i_l = yL.par.map {
      l =>
        val i_l_distance = linalg.squaredDistance(i.factors, l._2.factors)
        (l, -i_l_distance + k.bias)
    }

    val max_sum_il = sum_i_l.map(x => x._2).max

    val fixed_sum_i_l = sum_i_l.par.map {
      l =>
        exp(l._2 - max_sum_il) / item_dist(l._1._1)
    }.sum

    val res = ((exp(-linalg.squaredDistance(i.factors, k.factors) + bias_k)) / P_k_D) / fixed_sum_i_l
    if(res.isNaN || res.isInfinite)
      throw new Exception("ERROR: w_k_i is either Nan or Infinite")
    res
  }

  def getRandomItem(cumm_dist:Map[String,Double],rand:Rand[Double]): String ={
    val rangeMin = cumm_dist.values.min
    var rangeMax = cumm_dist.values.max
    if(rangeMin == rangeMax)
      rangeMax = rangeMax * 1.0000001

    val rnd = ThreadLocalRandom.current().nextDouble(rangeMin, rangeMax)
    var item = cumm_dist.view.maxBy(item => rnd <= item._2)._1
    val possible_items = cumm_dist.view.filter(item => item._2 == item._2)
    if(possible_items.size > 1)
      item = possible_items.toSeq(Random.nextInt(possible_items.size))._1
    item
  }

  def update_sample(logger:Logger,caseId:String, i:Factor, j:Factor, factors:Map[String,Factor],item_dist:Map[String,Double], items:immutable.Vector[String], alpha_sampling:Int, rand:Rand[Double], max_sample:Int): Map[String,Factor] = {
    val pji = p_j_i(i, j,factors)
    var continue = true


    val max_items = logIt(logger,"DEBUG","update_sample_get_random_pairs",caseId, {
      scala.util.Random.shuffle(items).distinct.take(max_sample)
    })

    logIt(logger,"DEBUG","update_sample_yL",caseId, {
      val max_yL = factors.filterKeys(max_items.take(max_sample).contains(_))
      var yL = Map.empty[String,Factor]
      var sample_probs = ParIterable.empty[Double]
      var cumm_sum = 0.0
      var n = 0
      while (continue && sample_probs.size <= max_items.size && n < max_items.size - 5) {
        yL = max_yL.slice(n,n+5)

        val yL_kwi = yL.par.map(
          k =>
            w_k_i(i, k._2,max_yL.take(sample_probs.size + 5),item_dist)
        )

        sample_probs ++= yL_kwi
        cumm_sum += yL_kwi.sum

        if (cumm_sum > alpha_sampling * pji)
          continue = false
        n += 5
      }
      max_yL.take(sample_probs.size)
    })
  }

  def update_wki(pairs: List[CoConsumed], yL:Map[String,Factor] ,factors:Map[String,Factor], item_dist:Map[String,Double]): Map[CoConsumed, Double] = {
    pairs.par.flatMap {
      item => for (xs <- yL.filter(x => x._1 != item.i).keys) yield (item.i, xs)
    }.distinct.map {
      pair =>
        new CoConsumed(pair._1, pair._2) -> w_k_i(factors(pair._1), factors(pair._2),yL,item_dist)
    }.seq.toMap
  }

  def update_distanceki(pairs: List[CoConsumed], yL:Map[String,Factor] ,factors:Map[String,Factor]): Map[CoConsumed, Double] = {
    pairs.par.flatMap {
      item => for (xs <- yL.filter(x => x._1 != item.i).keys) yield (item.i, xs)
    }.distinct.map {
      pair =>
        new CoConsumed(pair._1, pair._2) -> linalg.functions.euclideanDistance(factors(pair._1).factors, factors(pair._2).factors)
    }.seq.toMap
  }

  def update_i(i: Factor, j: Factor, item_dist:Map[String,Double], factors:Map[String,Factor], yL:Map[String,Factor], learning_rate:Double): (String,Factor) = {
    val i_j_distance = linalg.functions.euclideanDistance(i.factors, j.factors)
    val sumL = yL.filter(x => x._1 != i.id).par.map {
      k =>
        val x = w_k_i(i,k._2,yL,item_dist) * 4 * linalg.functions.euclideanDistance(i.factors,k._2.factors)
        if(x.isNaN || x.isInfinite)
          throw new Exception("ERROR: One of the sumL values is Nan or Infinite")
        x
    }

    //This looks so horrible I need to fix it...
    val sumL_sum = sumL.foldLeft(sumL.head){(m,x)=>m+x} - sumL.head
    val new_factor = i.factors - learning_rate * (-4 * i_j_distance * (i.factors - j.factors) + sumL_sum)
    val new_bias = i.bias - learning_rate * - w_k_i(i, i,yL,item_dist)
    if(new_bias.isNaN || new_bias.isInfinite || new_factor.findAll(x=>x.isNaN || x.isInfinite).size > 0)
      throw new Exception("ERROR: Either the bias or the new factor is Nan or Infinite")
    i.id -> new Factor(i.id, new_bias, new_factor)
  }

  def update_j(i: Factor, j: Factor, item_dist:Map[String,Double], yL:Map[String,Factor], learning_rate:Double): (String,Factor) = {
    val i_j_distance = - 4 * linalg.functions.euclideanDistance(i.factors, j.factors)
    val new_factor = j.factors - (learning_rate * i_j_distance * (j.factors - i.factors))
    val new_bias = j.bias - learning_rate * w_k_i(i, j,yL,item_dist)
    if(new_bias.isNaN || new_bias.isInfinite || new_factor.findAll(x=>x.isNaN || x.isInfinite).size > 0)
      throw new Exception("ERROR: Either the bias or the new factor is Nan or Infinite")
    j.id -> new Factor(j.id, new_bias, new_factor)
  }

  def update_k(i: Factor, j: Factor, k: Factor, item_dist:Map[String,Double], yL:Map[String,Factor], learning_rate:Double): (String,Factor) = {
    val i_k_distance = linalg.functions.euclideanDistance(i.factors, k.factors)
    val wki = w_k_i(i, j,yL,item_dist)
    val new_factor = k.factors - (learning_rate * wki * 4 * i_k_distance * (i.factors - k.factors))
    val new_bias = k.bias - learning_rate * -wki
    if(new_bias.isNaN || new_bias.isInfinite || new_factor.findAll(x=>x.isNaN || x.isInfinite).size > 0)
      throw new Exception("ERROR: Either the bias or the new factor is Nan or Infinite")
    k.id -> new Factor(k.id, new_bias, new_factor)
  }

  def training_ij(logger:Logger,caseId:String,i: Factor, j: Factor, item_dist:Map[String,Double], factors:Map[String,Factor], yL:Map[String,Factor], learning_rate:Double): Map[String,Factor] = {
    var new_factors: Map[String,Factor] = Map.empty[String,Factor]

//    try {
    new_factors += logIt(logger, "DEBUG", "update_i", caseId, {
      update_i(i, j, item_dist, factors, yL, learning_rate)
    })
    new_factors += logIt(logger, "DEBUG", "update_j", caseId, {
      update_j(i, j, item_dist, yL, learning_rate)
    })

    new_factors = new_factors ++ factors.filter(x => x._1 != i.id && x._1 != j.id && yL.contains(x._1)).par.map(
      k => logIt(logger, "DEBUG", "update_k", caseId, {
        update_k(i, j, k._2, item_dist, yL, learning_rate)
      })
    )
    new_factors = new_factors ++ (factors -- new_factors.keys.toSet)
    new_factors
//    }
//    catch{
//      case e: Exception =>
//        println("Error while training pair (" + i.id + "|" + j.id + ")" )
//        factors
//    }
  }

  def pki(i:Factor,k:Factor, max_bias:Double):Double ={
    linalg.functions.euclideanDistance(DenseVector.vertcat(i.factors,DenseVector(0.0)), DenseVector.vertcat(k.factors,DenseVector(sqrt(max_bias - k.bias))))
  }

  def ecp(nij:Double,ni:Double):Double ={
      (nij) / (ni + 1)
  }

  def jaccard(nij:Double,ni:Double, nj:Double):Double ={
      (nij) / (ni + nj - nij)
  }

  def cosine(nij:Double,ni:Double, nj:Double):Double ={
      (nij)/ sqrt(ni*nj)
  }

  def percentile_rank(j:String, list_probs:Map[String,Double], higher:Boolean,items_j_count_train: Map[String,Double],sum_training:Boolean):Double ={
    var scores_count = 0.0
    var j_score_frequency = 0.0
    var total_size = 0.0
    if(higher){
      if(sum_training){
        val j_keys = list_probs.view.filter(_._2 > list_probs(j)).map(_._1).toSet
        scores_count = items_j_count_train.filterKeys(j_keys).values.sum
      }
      else{
        scores_count = list_probs.view.filter(_._2 > list_probs(j)).size
      }
    }
    else{
      if(sum_training){
        val j_keys = list_probs.view.filter(_._2 < list_probs(j)).map(_._1).toSet
        scores_count = items_j_count_train.filterKeys(j_keys).values.sum
      }
      else{
        scores_count = list_probs.view.filter(_._2 < list_probs(j)).size
      }
    }
    //    val j_score_frequency = item_ocurrance_train(j)/2
    if(sum_training) {
      val j_keys = list_probs.view.filter(_._2 == list_probs(j)).map(_._1).toSet
      j_score_frequency = items_j_count_train.filterKeys(j_keys).values.sum / 2
      total_size =  items_j_count_train.filterKeys(list_probs.keys.toSet).values.sum
    }else{
      j_score_frequency = list_probs.view.filter(_._2 == list_probs(j)).size / 2
      total_size =  list_probs.size
    }

    (scores_count + j_score_frequency) / (total_size)
  }

  def recall(list_score:scala.collection.immutable.Vector[(String,Double)],j:String):Double ={
    if(list_score.map(_._1).contains(j))
      1
    else
      0
  }

  def dcg(list_score:scala.collection.immutable.Vector[(String,Double)],j:String):Double ={
    val index_of = list_score.map(_._1).indexOf(j) + 1
    if(index_of > 0)
      1 / scala.math.log(index_of + 1)
    else
      0
  }
  //Disable the logging
  def logIt[A](logger:Logger,log_level:String,activity:String, caseId:String,f: => A) : A= {
    val ret = f
    ret
  }

//  def logIt[A](logger:Logger,log_level:String,activity:String, caseId:String,f: => A) : A= {
//    val dt_formatter = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss,SSS");
//    val start = dt_formatter.format(new Date(System.currentTimeMillis()))
//    val ret = f
//    val end = dt_formatter.format(new Date(System.currentTimeMillis()))
//    if(log_level=="DEBUG")
//      logger.debug("\"" + caseId + "\",\"" + activity + "\",{},\"" + start + "\",\"" + end + "\"" + "|PMLog|", getContextData.mkString("\"","\",\"","\""))
//    if(log_level=="INFO")
//      logger.info("\"" + caseId + "\",\"" + activity + "\",{},\"" + start + "\",\"" + end + "\"" + "|PMLog|", getContextData.mkString("\"","\",\"","\""))
//    ret
//  }

  def train_minibatch(logger:Logger,caseId:String, factors:Map[String,Factor],co_consumed_pairs:immutable.Vector[CoConsumed],alpha_sampling:Int,item_dist:Map[String,Double],items:immutable.Vector[String],learning_rate:Double,num_factors:Int,max_sample:Int,rand:Rand[Double]): Map[String,Factor] = {
    logIt(logger,"INFO","SGD_Minibatch",caseId,{
      var new_factors = factors
      val random_pairs = logIt(logger,"DEBUG","sgd_get_random_pairs",caseId, {
        scala.util.Random.shuffle(co_consumed_pairs).take(100)
      })
      for (pair <- random_pairs) {
        val i = new_factors(pair.i)
        val j = new_factors(pair.j)
        //Update the sample for the item
        val yL = logIt(logger,"DEBUG","update_sample",caseId, {
          update_sample(logger,caseId,i, j, new_factors, item_dist, items, alpha_sampling, rand, max_sample)
        })
        //Learn the new factors for the i, j and other k
        new_factors = logIt(logger,"DEBUG","training_ij",caseId, {
          training_ij(logger,caseId,i, j, item_dist, new_factors, yL, learning_rate)
        })
      }
      new_factors
    })
  }

  def train_batch(logger:Logger,caseId:String,factors:Map[String,Factor],co_consumed_pairs:immutable.Vector[CoConsumed],alpha_sampling:Int,item_dist:Map[String,Double],items:immutable.Vector[String],learning_rate:Double,num_factors:Int,max_sample:Int,rand:Rand[Double]): Map[String,Factor] = {
    logIt(logger,"INFO","SGD_Batch",caseId,{
      var new_factors = factors
      for (pair <- co_consumed_pairs) {
        val i = new_factors(pair.i)
        val j = new_factors(pair.j)
        //Update the sample for the item
        val yL = logIt(logger,"DEBUG","update_sample",caseId, {
            update_sample(logger,caseId,i,j,new_factors,item_dist,items,alpha_sampling,rand,max_sample)
          })
        //Learn the new factors for the i, j and other k
        new_factors = logIt(logger,"DEBUG","training_ij",caseId, {
          training_ij(logger,caseId,i,j,item_dist,new_factors,yL,learning_rate)
        })
      }
      new_factors
    })
  }

  def calculate_modalities(i: String, j: String,
                           grouped_pairs_train:Map[CoConsumed, Double],
                           item_ij_training:Map[String,Double],
                           ni_coocurrance:Double,
                           l_factors_data:ParMap[String,Map[String,Factor]],
                           l_Mb:ParMap[String,Double],
                           items_content_similarity:Map[(String,String),Double],
                           items_jaccard_file: Map[String,Map[String,Double]],
                           items_content_similarity_file: Map[String,Map[String,Double]]
                          ): Map[String, Double] = {
    var result: List[(String, Double)] = List.empty[(String, Double)]
    val nij_coocurrance = grouped_pairs_train.getOrElse(new CoConsumed(i, j), 0.0) + grouped_pairs_train.getOrElse(new CoConsumed(j, i), 0.0)
    //            val nij_dataset = user_item_dataset(i).intersect(user_item_dataset(j)).size
    //Use the ocurrance of j considering all the possible training pairs
    //            val nj_dataset = datafile_items(j)
    val nj_coocurrance = item_ij_training(j)

    //            result = ("ecp", ecp(nij_dataset, ni_dataset)) :: result
    result = ("ecp_coocurrance", ecp(nij_coocurrance, ni_coocurrance)) :: result
    //result = ("ecp_coocurrance_symmetric", (ecp(nij_coocurrance, ni_coocurrance) + ecp(nij_coocurrance, nj_coocurrance)) / 2.0 ) :: result
    //            result = ("jaccard",jaccard(nij_dataset, ni_dataset, nj_dataset)) :: result
    result = ("jaccard_coocurrance",jaccard(nij_coocurrance, ni_coocurrance, nj_coocurrance)) :: result
    //            result = ("cosine",cosine(nij_dataset, ni_dataset, nj_dataset)) :: result
    result = ("cosine_coocurrance",cosine(nij_coocurrance, ni_coocurrance, nj_coocurrance)) :: result

    val factors_pki:List[(String,Double)] = l_factors_data.par.map{
      factors_data =>
        factors_data._1 -> pki(factors_data._2(i), factors_data._2(j), l_Mb(factors_data._1))
    }.toList

    result = ("content",items_content_similarity.getOrElse((i,j),items_content_similarity.getOrElse((j,i),0.0))) :: result

    if(items_jaccard_file.contains(i))
      result = ("jaccard_file",items_jaccard_file(i).getOrElse(j,0.0)) :: result
    else
      result = ("jaccard_file",0.0) :: result

    if(items_content_similarity_file.contains(i))
      result = ("content_similarity",items_content_similarity_file(i).getOrElse(j,0.0)) :: result
    else
      result = ("content_similarity", 0.0) :: result
    result = factors_pki ::: result

    result.toMap
  }

  def get_flink(create_flink:Boolean): (ExecutionEnvironment,StreamExecutionEnvironment) ={
    if (create_flink) {
//      val customConfiguration = new Configuration()
//      customConfiguration.setInteger("parallelism", 1)
//      customConfiguration.setInteger("jobmanager.heap.mb",2560)
//      customConfiguration.setInteger("taskmanager.heap.mb",10240)
//      customConfiguration.setInteger("taskmanager.numberOfTaskSlots",8)
//      customConfiguration.setInteger("taskmanager.network.numberOfBuffers",16384)
//      customConfiguration.setString("akka.ask.timeout","1000 s")
//      customConfiguration.setString("akka.lookup.timeout","100 s")
//      customConfiguration.setString("akka.framesize","8192Mb")
//      customConfiguration.setString("akka.watch.heartbeat.interval","100 s")
//      customConfiguration.setString("akka.watch.heartbeat.pause","1000 s")
//      customConfiguration.setString("akka.watch.threshold","12")
//      customConfiguration.setString("akka.transport.heartbeat.interval","1000 s")
//      customConfiguration.setString("akka.transport.heartbeat.pause","6000 s")
//      customConfiguration.setString("akka.transport.threshold","300")
//      customConfiguration.setString("akka.tcp.timeout","1000 s")
//      customConfiguration.setString("akka.throughput","15")
//      customConfiguration.setString("akka.log.lifecycle.events","on")
//      customConfiguration.setString("akka.startup-timeout","1000 s")
      (ExecutionEnvironment.createLocalEnvironment(),StreamExecutionEnvironment.createLocalEnvironment(1))
    }
    else{
      (ExecutionEnvironment.getExecutionEnvironment,StreamExecutionEnvironment.getExecutionEnvironment)
    }
  }
}