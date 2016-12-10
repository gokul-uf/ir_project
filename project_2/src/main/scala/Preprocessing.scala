package src.main.scala

import ch.ethz.dal.tinyir.io._
import ch.ethz.dal.tinyir.indexing._
import ch.ethz.dal.tinyir.processing._
import com.github.aztek.porterstemmer.PorterStemmer
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.{ Map => MMap }
import scala.collection.mutable.{ HashMap => MHashMap }
import ch.ethz.dal.tinyir.util
import ch.ethz.dal.tinyir.util.StopWatch
import scala.math.log
import java.io._

class Preprocessing {

  /*.replaceAll("vs.", "/").replaceAll(" +", "/").replaceAll("-+", "-").replaceAll("[0-9]", "/")
    .split(SPLITTERS).filter { x => x.trim() != "" }*/
  def transform(strings : Array[String]): Array[Seq[String]] = {
    strings
    .map { x => Tokenizer.tokenize(x) }
    .map { words => StopWords.filterOutSW(words) }
    .map { query =>
      query.map { term =>
        PorterStemmer.stem(term)
      }
    }
  }
}

object Preprocessing {
  val QUERIES_PATH : String       = "src/main/resources/queries/questions-descriptions.txt"

  def main(args: Array[String]): Unit = {
    val source = scala.io.Source.fromFile(QUERIES_PATH)
    val lines = try source.mkString finally source.close()

    var queries: Array[Seq[String]] = null

    val obj: Preprocessing = new Preprocessing()

    var queriesString = lines
                    .split("Topic:")
                    .drop(1)
                    .map { x =>
                      x
                      .substring(0, x.indexOf("<desc>"))
                      .trim()

                    }
    queries = obj.transform(queriesString)
    queries.foreach(println)
  }
}
