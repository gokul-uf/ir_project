package src.main.scala

import scala.collection.mutable.{ HashMap => MHashMap }
import java.io._

class MeasureResultZ {
  val resultFile: String  = "output.txt"
  val qrelPath: String    = "src/main/resources/rel/relevance-judgements.csv"
  val outputFile: String  = "score.txt"


  //Function to read from file
  def readFromFile(path: String) : String =  {
    val source = scala.io.Source.fromFile(path)
    try source.mkString finally source.close()
  }

  //Function to write to an output file
  def writeToFile(output: String) {
    if (output.trim() != "") {
      val fw = new FileWriter(this.outputFile, false)
      try {
        fw.write("\r\n" + output + "\r\n")
      } finally fw.close()
    }
  }
}

object MeasureResultZ{
  def main(args: Array[String]): Unit = {
    val obj: MeasureResultZ = new MeasureResultZ()

    var content: String = obj.readFromFile(obj.resultFile)
    var lines: Array[String] = content.split("\r\n").filter { x => x.trim() != "" }

    // _1 -> query num, _2 -> rank, _3 -> Doc name
    val results: Array[(Int, Int, String)]
      = lines.map { x => (x.split(" ")(0).toInt, x.split(" ")(1).toInt, x.split(" ")(2)) }

    //results.foreach(println)

    content = obj.readFromFile(obj.qrelPath)
    lines = content.split("\n").filter { x => x.trim() != "" }


    // _1 -> query num, _2 -> Binary relevance, _3 -> Doc name
    val qrels: Array[(Int, Int, String)]
      = lines.map { x => (x.split(" ")(0).toInt, x.split(" ")(3).toInt, x.split(" ")(2).replaceAll("-", "")) }

    var tp: MHashMap[Int, Double] = MHashMap.empty
    var fp: MHashMap[Int, Double] = MHashMap.empty
    var fn: MHashMap[Int, Double] = MHashMap.empty

    // Calculate true and false positives
    for(item<-results){
      if(!tp.contains(item._1)){
        tp += (item._1 -> 0.0)
      }

      if(qrels.filter(p => p._1==item._1 && p._3==item._3 && p._2==1).size > 0){
        tp(item._1) += 1.0
      }

      if(!fp.contains(item._1)){
        fp += (item._1 -> 0.0)
      }
      if(qrels.filter(p => p._1==item._1 && p._3==item._3 && p._2==0).size > 0){
        fp(item._1) += 1.0
      }
    }

    // Calculate false negatives
    for(i<-51 to 90){
      val relDocsResult = results.filter(p => p._1 == i)

      fn += (i -> qrels.filter(p => p._1==i && p._2==1 && !relDocsResult.contains(p)).size.toDouble)
    }

    // Calculate MAP
    var ap: MHashMap[Int, Double] = MHashMap.empty
    for(query<-51 to 90){
      var queryResult = results.filter(p => p._1 == query)

      var sum: Double = 0.0
      var index: Double = 1.0
      var numRelSoFar: Int = 0
      for(res<-queryResult){
        if(qrels.contains((res._1, 1, res._3))){
          numRelSoFar += 1
          var preSoFar: Double = numRelSoFar/index
          sum += preSoFar
          println("query=" + query + " sum=" + sum)
        }
        index += 1.0
      }

      ap += (query -> sum/math.min((tp(query)+fn(query)), index-1) )
    }

    val map: Double = ap.foldLeft(0.0){(sum, elem) => sum + elem._2}/40.0

    // Calculate precision, recall and F1
    var output: String = ""
    for(i<-51 to 90){
      var p: Double = tp(i)/(tp(i)+fp(i))
      var r: Double = tp(i)/math.min((tp(i)+fn(i)), results.count(p => p._1 == i))
      var f1: Double = 2*p*r/(p+r)
      //"% tp=%05.1f fp=%05.1f fn=%05.1f p=%07.5f r=%07.5f f1=%07.5f ap=%07.5f"
      output += "%d tp=%f fp=%f fn=%f p=%f r=%f f1=%f ap=%f".format(i, tp(i), fp(i), fn(i), p, r, f1, ap(i)) + "\r\n"
    }

    output += "map=%f".format(map)
    obj.writeToFile(output)

    println("Finished!")
  }
}
