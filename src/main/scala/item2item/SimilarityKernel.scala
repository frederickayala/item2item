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
import scala.collection.immutable.ListMap
import scala.collection.immutable.SortedMap
import scala.util.Random
import java.nio.file.{Path, Paths, Files, StandardOpenOption}
import java.nio.charset.{StandardCharsets}
import scala.collection.immutable.HashMap
import scala.collection.immutable.HashSet
import scala.collection.parallel.immutable.ParMap
import Utils._
import breeze.stats.distributions._
import com.typesafe.scalalogging._
/**
  * Created by Frederick Ayala-Gomez
  */
object SimilarityKernel {

  def calculate_combination_ir(logger:Logger, caseId:String, combination:List[String],l_reference_size:List[Int],max_ref:Map[String,Double],measures:ParMap[String,Map[String,Double]],max_ref_mod:ParMap[String,ParMap[String,Map[String,Double]]],max_i_ref_mod:ParMap[String,Map[String,Double]],e_i_j:Map[String,Double]): ParMap[String,Map[String,Double]] ={
    logIt(logger,"DEBUG","fishers_combination_" + combination.mkString(","),caseId, {
      l_reference_size.par.map {
        reference_size =>
          logIt(logger,"DEBUG","fishers_reference_size" + reference_size.toString,caseId, {
            val references = max_ref.take(reference_size)

            val v_j_vectors = logIt(logger, "DEBUG", "vector_j_ref", caseId, {
              measures.par.map {
                j =>
                  val j_r_measures = max_ref_mod(j._1).take(reference_size)
                  val i_r_measures = max_i_ref_mod.take(reference_size)
                  val j_vector = combination.par.map {
                    m =>
                      val e_j_r = j_r_measures.par.map(y => y._2(m)).sum / reference_size
                      val e_i_r_j_r = (i_r_measures.par.map(y => y._2(m)).sum - j_r_measures.par.map(y => y._2(m)).sum) / reference_size

                      val v_numerator = references.par.map {
                        r =>
                          val numerator = i_r_measures(r._1)(m) - j_r_measures(r._1)(m)
                          val denominator = scala.math.pow(e_i_r_j_r,2)
                          numerator/denominator
                        // scala.math.pow(e_i_j(m) + e_j_r - measures(j._1)(m) - j_r_measures(r._1)(m), 2)
                        // scala.math.pow(0*e_i_j(m) + 0 * e_j_r - measures(j._1)(m) - j_r_measures(r._1)(m), 2)
                      }
                      (m, v_numerator)
                  }

                  val j_norm = j_vector.par.map(
                    m_jvector =>
                      m_jvector._1 ->(m_jvector._2.seq, norm(breeze.linalg.DenseVector(m_jvector._2.seq.toArray)))
                  ).toMap
                  j._1 -> j_norm
              }
            })

            val m_e_j_matrix = logIt(logger, "DEBUG", "vector_j_ref_expected_value", caseId, {
              combination.par.map {
                m =>
                  val list_vectors = v_j_vectors.map(x => x._2(m)._1)
                  val m_e_j = list_vectors.transpose.map(x => x.sum / x.size)
                  m -> m_e_j
              }.toMap
            })

            val v_fisher = logIt(logger, "DEBUG", "fisher_vector", caseId, {
              v_j_vectors.map {
                v_j =>
                  val res = combination.par.map {
                    m =>
                      //                        v_j_vectors(v_j._1)(m)._1.toList(0)
                      val n = (v_j_vectors(v_j._1)(m)._1 zip m_e_j_matrix(m)).map {
                        case (xi, yi) => xi / yi
                      }
                      norm(breeze.linalg.DenseVector(n.toArray))
                  }
                  v_j._1 -> res.sum / res.size
              }.seq
            })

            //                v_fisher.par.map{
            //                  f =>
            //                    if(measures(f._1)("pkifactors_50factors_200epochs_0_01rate") != scala.math.sqrt(f._2)/2.0)
            //                      println("Something is wrong")
            //                }
            "fisher_ir_" + reference_size.toString + "_" + combination.mkString("_") -> v_fisher
          })
      }
    }.toMap)
  }

  def calculate_combination_with_variance_l2_norm(logger:Logger, caseId:String, combination:List[String],l_reference_size:List[Int],max_ref:List[(String,Double)],measures:ParMap[String,Map[String,Double]],max_ref_mod:ParMap[String,ParMap[String,Map[String,Double]]],e_i_j:Map[String,Double]): ParMap[String,Map[String,Double]] ={
    logIt(logger,"DEBUG","fishers_combination_" + combination.mkString(","),caseId, {
      l_reference_size.par.map {
        reference_size =>
          logIt(logger,"DEBUG","fishers_reference_size" + reference_size.toString,caseId, {
            val references = max_ref.take(reference_size)
            //                val references = sorted_items_prob.filter(x=>x._1 == pair._1.i).toMap
            val v_j_vectors = logIt(logger, "DEBUG", "vector_j_ref", caseId, {
              measures.par.map {
                j =>
                  val j_r_measures = max_ref_mod(j._1).filterKeys(references.map(_._1).toSet)

                  val j_vector = combination.par.map {
                    m =>
                      val e_j_r = j_r_measures.par.map(y => y._2(m)).sum / reference_size

                      val v_numerator = references.seq.map {
                        r =>
                          scala.math.pow(e_i_j(m) + e_j_r - measures(j._1)(m) - j_r_measures(r._1)(m), 2)
                        //                            scala.math.pow(0*e_i_j(m) + 0 * e_j_r - measures(j._1)(m) - j_r_measures(r._1)(m), 2)
                      }

                      val e_v_numerator = v_numerator.sum / v_numerator.size

                      (m, v_numerator.map(_/e_v_numerator))
                  }

                  val j_norm = j_vector.par.map(
                    m_jvector =>
                      m_jvector._1 ->(m_jvector._2.seq, norm(breeze.linalg.DenseVector(m_jvector._2.seq.toArray)))
                  ).toMap
                  j._1 -> j_norm
              }
            })

            val m_e_j_matrix = logIt(logger, "DEBUG", "vector_j_ref_expected_value", caseId, {
              combination.par.map {
                m =>
                  val list_vectors = v_j_vectors.map(x => x._2(m)._1)
                  val m_e_j = list_vectors.transpose.map(x => x.sum / x.size)
                  m -> m_e_j
              }.toMap
            })

            val v_fisher = logIt(logger, "DEBUG", "fisher_vector", caseId, {
              v_j_vectors.map {
                v_j =>
                  val res = combination.par.map {
                    m =>
                      //                        v_j_vectors(v_j._1)(m)._1.toList(0)
                      val n = (v_j_vectors(v_j._1)(m)._1 zip m_e_j_matrix(m)).map {
                        case (xi, yi) => xi / yi
                      }
                      norm(breeze.linalg.DenseVector(n.toArray))
                  }
                  v_j._1 -> res.sum / res.size
              }.seq
            })

            //                v_fisher.par.map{
            //                  f =>
            //                    if(measures(f._1)("pkifactors_50factors_200epochs_0_01rate") != scala.math.sqrt(f._2)/2.0)
            //                      println("Something is wrong")
            //                }
            "fisher_with_variance_" + reference_size.toString + "_" + combination.mkString("_") -> v_fisher
          })
      }
    }.toMap)
  }

  def calculate_combination_without_variance_l2_norm(logger:Logger, caseId:String, combination:List[String],l_reference_size:List[Int],max_ref:List[(String,Double)],measures:ParMap[String,Map[String,Double]],max_ref_mod:ParMap[String,ParMap[String,Map[String,Double]]],e_i_j:Map[String,Double]): ParMap[String,Map[String,Double]] ={
    logIt(logger,"DEBUG","fishers_combination_" + combination.mkString(","),caseId, {
      l_reference_size.par.map {
        reference_size =>
          logIt(logger,"DEBUG","fishers_reference_size" + reference_size.toString,caseId, {
            val references = max_ref.take(reference_size)
            //                val references = sorted_items_prob.filter(x=>x._1 == pair._1.i).toMap
            val v_j_vectors = logIt(logger, "DEBUG", "vector_j_ref", caseId, {
              measures.par.map {
                j =>
                  val j_r_measures = max_ref_mod(j._1).filterKeys(references.map(_._1).toSet)

                  val j_vector = combination.seq.map {
                    m =>
                      val e_j_r = j_r_measures.par.map(y => y._2(m)).sum / reference_size

                      val v_numerator = references.seq.map {
                        r =>
                          scala.math.pow(e_i_j(m) + e_j_r - measures(j._1)(m) - j_r_measures(r._1)(m), 2)
                        //                            scala.math.pow(0*e_i_j(m) + 0 * e_j_r - measures(j._1)(m) - j_r_measures(r._1)(m), 2)
                      }

                      (m, v_numerator)
                  }

                  val j_norm = j_vector.seq.map(
                    m_jvector =>
                      m_jvector._1 ->(m_jvector._2.seq, norm(breeze.linalg.DenseVector(m_jvector._2.seq.toArray)))
                  ).toMap
                  j._1 -> j_norm
              }
            })

            val m_e_j_matrix = logIt(logger, "DEBUG", "vector_j_ref_expected_value", caseId, {
              combination.par.map {
                m =>
                  val list_vectors = v_j_vectors.map(x => x._2(m)._1)
                  val m_e_j = list_vectors.transpose.map(x => x.sum / x.size)
                  m -> m_e_j
              }.toMap
            })

            val v_fisher = logIt(logger, "DEBUG", "fisher_vector", caseId, {
              v_j_vectors.map {
                v_j =>
                  val res = combination.par.map {
                    m =>
                      //                        v_j_vectors(v_j._1)(m)._1.toList(0)
                      val n = (v_j_vectors(v_j._1)(m)._1 zip m_e_j_matrix(m)).map {
                        case (xi, yi) => xi / yi
                      }
                      norm(breeze.linalg.DenseVector(n.toArray))
                  }
                  v_j._1 -> res.sum
              }.seq
            })

            //                v_fisher.par.map{
            //                  f =>
            //                    if(measures(f._1)("pkifactors_50factors_200epochs_0_01rate") != scala.math.sqrt(f._2)/2.0)
            //                      println("Something is wrong")
            //                }
            "fisher_without_variance_l2_norm_" + reference_size.toString + "_" + combination.mkString("_") -> v_fisher
          })
      }
    }.toMap)
  }

  def calculate_combination_with_variance_l1(logger:Logger, caseId:String, combination:List[String],l_reference_size:List[Int],max_ref:List[(String,Double)],measures:ParMap[String,Map[String,Double]],max_ref_mod:ParMap[String,ParMap[String,Map[String,Double]]],e_i_j:Map[String,Double]): ParMap[String,Map[String,Double]] ={
    logIt(logger,"DEBUG","fishers_combination_" + combination.mkString(","),caseId, {
      l_reference_size.par.map {
        reference_size =>
          logIt(logger,"DEBUG","fishers_reference_size" + reference_size.toString,caseId, {
            val references = max_ref.take(reference_size)
            //                val references = sorted_items_prob.filter(x=>x._1 == pair._1.i).toMap
            val v_j_vectors = logIt(logger, "DEBUG", "vector_j_ref", caseId, {
              measures.par.map {
                j =>
                  val j_r_measures = max_ref_mod(j._1).filterKeys(references.map(_._1).toSet)

                  val j_vector = combination.par.map {
                    m =>
                      val e_j_r = j_r_measures.par.map(y => y._2(m)).sum / reference_size

                      val v_numerator = references.seq.map {
                        r =>
                          e_i_j(m) + e_j_r - measures(j._1)(m) - j_r_measures(r._1)(m)
                        //                            scala.math.pow(0*e_i_j(m) + 0 * e_j_r - measures(j._1)(m) - j_r_measures(r._1)(m), 2)
                      }

                      val e_v_numerator = v_numerator.sum / v_numerator.size

                      (m, v_numerator.map(_/e_v_numerator))
                  }

                  val j_norm = j_vector.par.map(
                    m_jvector =>
                      m_jvector._1 -> (m_jvector._2.seq, m_jvector._2.map(abs(_)).sum)
                  ).toMap
                  j._1 -> j_norm
              }
            })

            val m_e_j_matrix = logIt(logger, "DEBUG", "vector_j_ref_expected_value", caseId, {
              combination.par.map {
                m =>
                  val list_vectors = v_j_vectors.map(x => x._2(m)._1)
                  val m_e_j = list_vectors.transpose.map(x => x.sum / x.size)
                  m -> m_e_j
              }.toMap
            })

            val v_fisher = logIt(logger, "DEBUG", "fisher_vector", caseId, {
              v_j_vectors.map {
                v_j =>
                  val res = combination.par.map {
                    m =>
                      //                        v_j_vectors(v_j._1)(m)._1.toList(0)
                      val n = (v_j_vectors(v_j._1)(m)._1 zip m_e_j_matrix(m)).map {
                        case (xi, yi) => xi / yi
                      }
                      n.map(abs(_)).sum
                  }
                  v_j._1 -> res.sum / res.size
              }.seq
            })

            //                v_fisher.par.map{
            //                  f =>
            //                    if(measures(f._1)("pkifactors_50factors_200epochs_0_01rate") != scala.math.sqrt(f._2)/2.0)
            //                      println("Something is wrong")
            //                }
            "fisher_with_variance_l1_" + reference_size.toString + "_" + combination.mkString("_") -> v_fisher
          })
      }
    }.toMap)
  }

  def calculate_combination_without_variance_l1_norm(logger:Logger, caseId:String, combination:List[String],l_reference_size:List[Int],max_ref:List[(String,Double)],measures:ParMap[String,Map[String,Double]],max_ref_mod:ParMap[String,ParMap[String,Map[String,Double]]],e_i_j:Map[String,Double]): ParMap[String,Map[String,Double]] ={
    logIt(logger,"DEBUG","fishers_combination_" + combination.mkString(","),caseId, {
      l_reference_size.par.map {
        reference_size =>
          logIt(logger,"DEBUG","fishers_reference_size" + reference_size.toString,caseId, {
            val references = max_ref.take(reference_size)
            //                val references = sorted_items_prob.filter(x=>x._1 == pair._1.i).toMap
            val v_j_vectors = logIt(logger, "DEBUG", "vector_j_ref", caseId, {
              measures.par.map {
                j =>
                  val j_r_measures = max_ref_mod(j._1).filterKeys(references.map(_._1).toSet)

                  val j_vector = combination.par.map {
                    m =>
                      val e_j_r = j_r_measures.par.map(y => y._2(m)).sum / reference_size

                      val v_numerator = references.seq.map {
                        r =>
                          e_i_j(m) + e_j_r - measures(j._1)(m) - j_r_measures(r._1)(m)
                        //                            scala.math.pow(0*e_i_j(m) + 0 * e_j_r - measures(j._1)(m) - j_r_measures(r._1)(m), 2)
                      }

                      (m, v_numerator)
                  }

                  val j_norm = j_vector.par.map(
                    m_jvector =>
                      m_jvector._1 -> (m_jvector._2.seq, m_jvector._2.map(abs(_)).sum)
                  ).toMap
                  j._1 -> j_norm
              }
            })

            val m_e_j_matrix = logIt(logger, "DEBUG", "vector_j_ref_expected_value", caseId, {
              combination.par.map {
                m =>
                  val list_vectors = v_j_vectors.map(x => x._2(m)._1)
                  val m_e_j = list_vectors.transpose.map(x => x.sum / x.size)
                  m -> m_e_j
              }.toMap
            })

            val v_fisher = logIt(logger, "DEBUG", "fisher_vector", caseId, {
              v_j_vectors.map {
                v_j =>
                  val res = combination.par.map {
                    m =>
                      //                        v_j_vectors(v_j._1)(m)._1.toList(0)
                      val n = (v_j_vectors(v_j._1)(m)._1 zip m_e_j_matrix(m)).map {
                        case (xi, yi) => xi / yi
                      }
                      n.map(abs(_)).sum
                  }
                  v_j._1 -> res.sum / res.size
              }.seq
            })

            //                v_fisher.par.map{
            //                  f =>
            //                    if(measures(f._1)("pkifactors_50factors_200epochs_0_01rate") != scala.math.sqrt(f._2)/2.0)
            //                      println("Something is wrong")
            //                }
            "fisher_without_variance_l1_norm_" + reference_size.toString + "_" + combination.mkString("_") -> v_fisher
          })
      }
    }.toMap)
  }

  def compute_G(distance:breeze.linalg.DenseVector[Double],mu:breeze.linalg.DenseVector[Double],variance:breeze.linalg.DenseVector[Double],mode:String):breeze.linalg.DenseVector[Double] = {
    val g = (mu :- distance) :/ variance
    var g_res = DenseVector.fill(g.length){0.0}
    if(mode == "without_norm")
      g_res = g
    if(mode == "l1")
      g_res = g :/ breeze.linalg.sum(g.map(scala.math.abs(_)))
    if(mode == "l2")
      g_res = g :/ norm(g)
    return g_res
  }

  def calculate_kernel(logger:Logger,
                       caseId:String,
                       modality:String,
                       item_i:String,
                       measures:ParMap[String,Map[String,Double]],
                       map_items_references_mods:ParMap[(String,String),Map[String,Double]],
                       map_mu_i_r:ParMap[String,Map[String,Double]],
                       map_variance_i_r:ParMap[String,Map[String,Double]],
                       map_mu_j_r:ParMap[String,Map[String,Double]],
                       map_variance_j_r:ParMap[String,Map[String,Double]],
                       l_reference_size:List[Int],
                       max_ref:List[(String,Double)],
                       normalizations:List[String]
                      ): ParMap[(Int,String),Map[String,Double]] ={
    logIt(logger,"DEBUG","fishers_kernel_" + modality,caseId, {
      val max_items = l_reference_size.max
      val l_max_items = max_ref.view.filter(x => x._1 != item_i && ! measures.keys.toVector.contains(x._1)).take(max_items)
      val max_pairs_i_r = (for(r <- l_max_items) yield ((item_i,r._1))).toSet
      val max_pairs_j_r = measures.keys.flatMap{
        j=>
          (for(r <- l_max_items) yield ((j,r._1)))
      }.toSet

      val max_distance_i_r = map_items_references_mods.filterKeys(max_pairs_i_r)
      val max_distance_j_r = map_items_references_mods.filterKeys(max_pairs_j_r)

      val max_mu_i_r = map_mu_i_r(modality).filterKeys(l_max_items.map(_._1).toSet)
      val max_mu_j_r = map_mu_j_r(modality).filterKeys(l_max_items.map(_._1).toSet)

      val max_variance_i_r = map_variance_i_r(modality).filterKeys(l_max_items.map(_._1).toSet)
      val max_variance_j_r = map_variance_j_r(modality).filterKeys(l_max_items.map(_._1).toSet)

      l_reference_size.par.flatMap{
        reference_size =>
          normalizations.map{
            kernel_normalization =>
              val kernels = logIt(logger,"DEBUG","fishers_reference_size" + reference_size.toString,caseId, {
              val references = l_max_items.take(reference_size)
              val references_items = references.map(_._1).toSet
              //Information for item i
              val pairs_i_r = (for(r <- references) yield (item_i,r._1)).toSet
              val distance_i_r = DenseVector(max_distance_i_r.filterKeys(pairs_i_r).map{
                x=>
                  x._2(modality)
              }.toArray)

              val mu_i_r = DenseVector(max_mu_i_r.filterKeys(references_items).map{_._2}.toArray)
              val variance_i_r = DenseVector(max_variance_i_r.filterKeys(references_items).map{_._2}.toArray)

              measures.map{
                j =>
                  val item_j = j._1
                  val references_no_j = references_items.filter(_ != item_j)
                  //Information for item j
                  val pairs_j_r = (for(r <- references_no_j) yield (item_j,r))
                  val distance_j_r = DenseVector(max_distance_j_r.filterKeys(pairs_j_r).map{
                    x=>
                      x._2(modality)
                  }.toArray)

                  val mu_j_r = DenseVector(max_mu_j_r.filterKeys(references_no_j).map{_._2}.toArray)
                  val variance_j_r = DenseVector(max_variance_j_r.filterKeys(references_no_j).map{_._2}.toArray)

                  val Gir  = compute_G(distance_i_r,mu_i_r,variance_i_r,kernel_normalization)
                  val Gjr = compute_G(distance_j_r,mu_j_r,variance_j_r,kernel_normalization)

                  item_j -> Gir.t * Gjr
              }.seq
            })
              (reference_size,kernel_normalization) -> kernels
          }
      }
    }.toMap)
  }
}
