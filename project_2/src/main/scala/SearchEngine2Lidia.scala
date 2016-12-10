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

// auxiliary classes
class DocToTokensMap(val name: String, val tokens: Seq[(String, Int)]) {}

class TestDocument(val n: String, val s: String) extends Document {
  def title = ""
  def body = s
  def name = n
  def date = ""
  def content = body
}

// search engine class
class SearchEngineZY {

  val DEBUG = false
  val SPLITTERS: String = "\"/-,&-\n()"

  // Paths
  val OUTPUT_PATH       : String = "outputZY.txt"
  val DOCS_PATH         : String = "src/main/resources/docs/"
  val QUERIES_PATH      : String = "src/main/resources/queries/questions-descriptions.txt"
  val QUERIES_REL_PATH  : String = "src/main/resources/rel/relevance-judgements.csv"

  // auxiliary print function
  def p(output : String){
    if(DEBUG){
      println(output)
    }
  }

  var invIndex: MHashMap[String, ListBuffer[(String, Int)]] = MHashMap.empty

  var docTokenSize: MHashMap[String, Double] = MHashMap.empty

  var collectionFreq: Double = 0

  var queries     : Array[Seq[String]]  = null
  var queriesNum  : Array[String]       = null
  var query       : Seq[String]         = null

  var queryResult : MHashMap[String, ListBuffer[(String, Int)]] = MHashMap.empty

  def collectionFrequency() {
    for (item <- this.invIndex) {
      for (elem <- item._2) {
        this.collectionFreq += elem._2
      }
    }

    println("Debug: collectionFrequency: Collection frequency is: " + this.collectionFreq)
  }

  //Function to write to an output file
  def writeToFile(output: String) {
    if (output.trim() != "") {
      val fw = new FileWriter(this.OUTPUT_PATH, true)
      try {
        fw.write("\r\n" + output + "\r\n")
      } finally fw.close()
    }
  }

  //Function to read from file
  def readFromFile(path: String): String = {
    val source = scala.io.Source.fromFile(path)
    try source.mkString finally source.close()
  }

  //Function to read query from query description file
  def readQueriesFromFile() {
    val source = scala.io.Source.fromFile(this.QUERIES_PATH)
    val lines = try source.mkString finally source.close()
    val preprocessor: Preprocessing = new Preprocessing()

    val queriesStrings = lines
                        .split("Topic:")
                        .drop(1)
                        .map { x => x.substring(0, x.indexOf("<desc>")).trim() }

    this.queries = preprocessor.transform(queriesStrings)

    this.queriesNum = lines
                      .split("Number:")
                      .drop(1)
                      .map { x => x.substring(0, x.indexOf("<dom>")).trim() }
                      .filter { x => x.trim() != "" }

    this.queriesNum.foreach(p)
    this.queries.foreach(x => p(x.mkString(" ")))
  }

  def QueryInvIndex(): MHashMap[String, ListBuffer[(String, Int)]] = {

    val result: MHashMap[String, ListBuffer[(String, Int)]] = MHashMap.empty
    for (term <- this.query) {
      // Question- what if query term not present in corpus
      if (this.invIndex.contains(term)) {
        result += (term -> this.invIndex(term))
      }
    }

    this.queryResult = result
    result
  }

  def createInvIndex(): MHashMap[String, ListBuffer[(String, Int)]] = {
    val docTokens: ListBuffer[DocToTokensMap] = tipsterParse()
    val invIndex = new MHashMap[String, ListBuffer[(String, Int)]]()

    var watch: StopWatch = new StopWatch()
    watch.start

    for (item <- docTokens) {
      for (token <- item.tokens) {
        if (invIndex.contains(token._1)) {
          invIndex(token._1) += (item.name -> token._2)
        } else {
          invIndex += (token._1 -> ListBuffer((item.name, token._2)))
        }
      }
    }

    watch.stop
    println("Debug: createInvIndex: Total index building time = " + watch.uptonow)

    this.invIndex =  invIndex
    invIndex
  }

  def tipsterParse(): ListBuffer[DocToTokensMap] = {
    val tipster = new TipsterStream(DOCS_PATH)
    println("Debug: tipsterParse: Number of files in zips = " + tipster.length)

    var docTokens: ListBuffer[DocToTokensMap] = ListBuffer.empty[DocToTokensMap]
    var watch: StopWatch = new StopWatch()
    var index: Int = 1
    watch.start
    for (doc <- tipster.stream.take(100000)) {
      var curDocTokens = createDocToTokensMap(doc)
      docTokens += curDocTokens

      this.docTokenSize += (curDocTokens.name -> calculateDocTokenSize(curDocTokens.tokens))

      if (index % 5000 == 0) {
        watch.stop
        println("Index=" + index + " ; Time = " + watch.uptonow)
        watch.start
      }
      index += 1
    }

    watch.stop
    //println("Debug: tipsterParse: Total read time = " + watch.uptonow)

    return docTokens
  }

  def tipsterParseTest(): ListBuffer[DocToTokensMap] = {
    var docTokens = ListBuffer.empty[DocToTokensMap]

    //val d1 = new TestDocument("1", "mr sherlock sherlock holmes holmes who was usually very late")
    //val d0 = new TestDocument("0", "i can tell a moriaty when i see one said holmes holmes holmes")

    val d1 = new TestDocument("doc1", "Xyzzy reports aqua profit but revenue isru down")
    val d0 = new TestDocument("doc2", "Quorus narrows quarter loss but revenue decreases further")

    var doc1 = createDocToTokensMap(d0)
    var doc2 = createDocToTokensMap(d1)

    docTokens.append(doc1, doc2)

    this.docTokenSize += (doc1.name -> calculateDocTokenSize(doc1.tokens))
    this.docTokenSize += (doc2.name -> calculateDocTokenSize(doc2.tokens))

    println("docTokensSize:")
    this.docTokenSize.foreach(println)

    this.queries = new Array[Seq[String]](2)
    this.queries(0) = "revenue profit".split(" ").map { x => PorterStemmer.stem(x) }
    this.queries(1) = "not present".split(" ").map { x => PorterStemmer.stem(x) }

    this.queriesNum = Array("1", "2")
    return docTokens
  }

  def createDocToTokensMap(doc: Document): DocToTokensMap = {
    //println("Debug: createDocToTokensMap: Title=" + doc.name)

    def words: List[String] = Tokenizer.tokenize(doc.content)
    //println("Debug: createDocToTokensMap: tokens = " + words)

    def wordsWithoutSW: Seq[String] = StopWords.filterOutSW(words)
    def porterStemmedList: Seq[String] = wordsWithoutSW.map(word => PorterStemmer.stem(word))
    //println("Debug: createDocToTokensMap: Porter = " + porterStemmedList)

    //return new DocToTokensMap(doc.name, wordsWithoutSW.groupBy(identity).
    //mapValues(_.size).filter(p => p._2 > 1).toSeq) //

    return new DocToTokensMap(doc.name, porterStemmedList.groupBy(identity).
      mapValues(_.size).toSeq) //.filter(p => p._2 > 1)
  }

  def calculateDocTokenSize(arg: Seq[(String, Int)]): Double = {
    arg.foldLeft(0.0) {
      (sum, elem) =>
        sum + elem._2
    }
  }
}

object SearchEngineZY {
  def main(args: Array[String]): Unit = {
    var watch: StopWatch = new StopWatch()
    watch.start

    val obj: SearchEngineZY = new SearchEngineZY()

    obj.readQueriesFromFile()
    obj.createInvIndex()
    obj.collectionFrequency()

    var queryNum: Int = 0

    for (q <- obj.queries) {
      obj.query = q
      obj.QueryInvIndex()

      //println("Debug: main: Result of query: " + q.mkString(" "))
      //obj.queryResult.foreach(println)

      //Language and Score Modelling
      //ScoreModeler(obj)

      val docRanks = LanguageModeler(obj)
      //println("Debug: main: docRanks:")
      //docRanks.foreach(println)

      // Write output to file
      val sortedOutput: Seq[(String, Double)] = docRanks.toSeq
        // For this query, only consider docs whose binary relevance for the query is present in qrels doc
        //.filter(p => qrelMap(obj.queriesNum(queryNum).toInt).contains(p._1))
        .sortBy(-_._2).take(100)
      var outputString: String = ""
      var index: Int = 1
      for (o <- sortedOutput) {
        outputString += obj.queriesNum(queryNum) + " " + index.toString() + " " + o._1 + " " + o._2 + "\r\n"
        index += 1
      }

      obj.writeToFile(outputString)

      queryNum += 1
    }

    watch.stop
    println("Debug: main: Time taken to rank all queries = " + watch.uptonow)
    println("Finished!")
  }

  def ScoreModeler(obj: SearchEngineZY): Unit = {}

  def LanguageModeler(obj: SearchEngineZY): MHashMap[String, Double] = {

    var watch: StopWatch = new StopWatch()
    watch.start
    var lambda: Double = 0.99

    // MHashMap[DocName, MHashMap[query_term, occurance_in_document]]
    var docToQueryTermMap: MHashMap[String, MHashMap[String, Double]] = MHashMap.empty

    // Find document to terms in query mapping
    for (q <- obj.queryResult) {
      for (doc <- q._2) {
        if (docToQueryTermMap.contains(doc._1)) {
          if (docToQueryTermMap(doc._1).contains(q._1)) {
            docToQueryTermMap(doc._1)(q._1) += doc._2
          } else {
            docToQueryTermMap(doc._1) += (q._1 -> doc._2)
          }
        } else {
          docToQueryTermMap += (doc._1 -> MHashMap(q._1 -> doc._2))
        }
      }
    }

    //println("Debug: LanguageModeler: docToQueryTermMap:")
    //docToQueryTermMap.foreach(println)

    // MHashMap[doc_name, rank]
    var docRanks: MHashMap[String, Double] = MHashMap.empty
    // Initialize for each document
    for (d <- docToQueryTermMap) {
      docRanks += (d._1 -> 0.0)
    }

    // For this query, calculate ranking of documents
    //todo: vary lambda according to query size
    lambda = 1.0 - (obj.query.size * 0.1)
    /*if (obj.query.size <= 3){
      lambda = 0.9
    } else {
      lambda = 0.3
    }*/
    for (t <- obj.query) {
      // Proceed only if query term present in corpus
      if (obj.invIndex.contains(t)) {
        var p_t_mc: Double = 0.0

        p_t_mc = obj.invIndex(t).foldLeft(0.0) {
          (sum, elem) =>
            sum + elem._2
        }

        p_t_mc /= obj.collectionFreq

        for (d <- docToQueryTermMap) {
          var p_t_md: Double = 0.0
          //var tf_t_d: Double = 0.0
          //var l_d: Double = 0.0
          if (d._2.contains(t)) {
            p_t_md = d._2(t) / obj.docTokenSize(d._1)

            //Bayesian
            //tf_t_d = d._2(t)
            //l_d = obj.docTokenSize(d._1)
          }

          //lambda = 1.0 - (obj.docTokenSize(d._1)/obj.collectionFreq)
          docRanks(d._1) += log(((1 - lambda) * p_t_mc) + (lambda * p_t_md))

          //Bayesian
          //docRanks(d._1) += log( (tf_t_d + (lambda*p_t_mc) )/(l_d + lambda) )
        }
      }
    }

    watch.stop
    //println("Debug: LanguageModeler: Time taken to rank = " + watch.uptonow)
    return docRanks
  }

  def UniGramLanguageModeler(obj: SearchEngineZY): MHashMap[String, Double] = {

    var watch: StopWatch = new StopWatch()
    watch.start
    var lambda: Double = 0.99

    // MHashMap[DocName, MHashMap[query_term, occurance_in_document]]
    var docToQueryTermMap: MHashMap[String, MHashMap[String, Double]] = MHashMap.empty

    // Find document to terms in query mapping
    for (q <- obj.queryResult) {
      for (doc <- q._2) {
        if (docToQueryTermMap.contains(doc._1)) {
          if (docToQueryTermMap(doc._1).contains(q._1)) {
            docToQueryTermMap(doc._1)(q._1) += doc._2
          } else {
            docToQueryTermMap(doc._1) += (q._1 -> doc._2)
          }
        } else {
          docToQueryTermMap += (doc._1 -> MHashMap(q._1 -> doc._2))
        }
      }
    }

    //println("Debug: LanguageModeler: docToQueryTermMap:")
    //docToQueryTermMap.foreach(println)

    // MHashMap[doc_name, rank]
    var docRanks: MHashMap[String, Double] = MHashMap.empty
    // Initialize for each document
    for (d <- docToQueryTermMap) {
      docRanks += (d._1 -> 0.0)
    }

    // For each candidate document, calculate rank score for the query
    for (d <- docToQueryTermMap) {

      for(t <- obj.query) {
        var tf_t: Double = 0.0
        if (d._2.contains(t)) {
          tf_t = d._2(t)
        }

        var p_t_md: Double = tf_t / obj.docTokenSize(d._1)

        var cf_t: Double = 0.0
        if (obj.invIndex.contains(t)) {
          cf_t = obj.invIndex(t).foldLeft(0.0) {
            (sum, elem) =>
              sum + elem._2
          }
        }

        if(cf_t == 0.0){
          printf("Collection frequency zero for " + t)
        }

        var p_t_mc: Double = cf_t / obj.collectionFreq
        docRanks(d._1) += log(((1 - lambda) * p_t_mc) + (lambda * p_t_md))
      }
    }

    watch.stop
    //println("Debug: LanguageModeler: Time taken to rank = " + watch.uptonow)
    return docRanks
  }

  def BiGramLanguageModeler(obj: SearchEngineZY): MHashMap[String, Double] = {

    var watch: StopWatch = new StopWatch()
    watch.start
    var lambda: Double = 0.8

    // MHashMap[DocName, MHashMap[query_term, occurance_in_document]]
    var docToQueryTermMap: MHashMap[String, MHashMap[String, Double]] = MHashMap.empty

    // Find document to terms in query mapping
    for (q <- obj.queryResult) {
      for (doc <- q._2) {
        if (docToQueryTermMap.contains(doc._1)) {
          if (docToQueryTermMap(doc._1).contains(q._1)) {
            docToQueryTermMap(doc._1)(q._1) += doc._2
          } else {
            docToQueryTermMap(doc._1) += (q._1 -> doc._2)
          }
        } else {
          docToQueryTermMap += (doc._1 -> MHashMap(q._1 -> doc._2))
        }
      }
    }

    //println("Debug: LanguageModeler: docToQueryTermMap:")
    //docToQueryTermMap.foreach(println)

    // MHashMap[doc_name, rank]
    var docRanks: MHashMap[String, Double] = MHashMap.empty
    // Initialize for each document
    for (d <- docToQueryTermMap) {
      docRanks += (d._1 -> 0.0)
    }

    // For each candidate document, calculate rank score for the query
    for (d <- docToQueryTermMap) {
      if (obj.query.size == 1) {
        var t1: String = obj.query(0)

        var tfT1T2: Double = 0.0
        if (d._2.contains(t1)) {
          tfT1T2 = d._2(t1)
        }

        var p_tfT1T2: Double = tfT1T2 / obj.docTokenSize(d._1)

        var cfT1T2: Double = 0.0
        if (obj.invIndex.contains(t1)) {
          cfT1T2 = obj.invIndex(t1).foldLeft(0.0) {
            (sum, elem) =>
              sum + elem._2
          }
        }

        var p_cfT1T2: Double = cfT1T2 / obj.collectionFreq
        docRanks(d._1) += log(((1 - lambda) * p_cfT1T2) + (lambda * p_tfT1T2))
      } else {

        var tIndex: Int = 0

        while (tIndex < obj.query.size - 1) {
          var t1: String = obj.query(tIndex)
          var t2: String = obj.query(tIndex + 1)

          var tfT1T2: Double = 999999.0
          if (d._2.contains(t1)) {
            tfT1T2 = math.min(tfT1T2, d._2(t1))
          } else {
            tfT1T2 = 0.0
          }

          if (d._2.contains(t2)) {
            tfT1T2 = math.min(tfT1T2, d._2(t2))
          } else {
            tfT1T2 = 0.0
          }
          var p_tfT1T2: Double = tfT1T2 / obj.docTokenSize(d._1)

          var tfT1: Double = 0.0
          if (d._2.contains(t1)) {
            tfT1 = d._2(t1)
          }
          var p_tfT1: Double = tfT1 / obj.docTokenSize(d._1)

          var cfT1: Double = 0.0
          if (obj.invIndex.contains(t1)) {
            cfT1 = obj.invIndex(t1).foldLeft(0.0) {
              (sum, elem) =>
                sum + elem._2
            }
          }
          var p_cfT1: Double = cfT1 / obj.collectionFreq

          docRanks(d._1) += log((0.5 * p_tfT1T2) + (0.3 * p_tfT1) + (0.2 * p_cfT1))
          tIndex += 1
        }
      }
    }

    watch.stop
    //println("Debug: LanguageModeler: Time taken to rank = " + watch.uptonow)
    return docRanks
  }
}
