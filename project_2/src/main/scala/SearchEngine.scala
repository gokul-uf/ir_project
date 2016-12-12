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
import java.util.Calendar
import java.util.Date
import java.text.SimpleDateFormat

// auxiliary classes
class DocToTokensMap(val name: String, val tokens: Seq[(String, Int)]) {
  /* Map of document name, token and tf */
}

// search engine class
class SearchEngine {

  val DEBUG = 3
  val SPLITTERS: String = "\"/-,&-\n()"

  // Paths
  val OUTPUT_PATH       : String = "outputZY.txt"
  val DOCS_PATH         : String = "src/main/resources/docs/"
  val QUERIES_PATH      : String = "src/main/resources/queries/test-questions.txt"
  val QUERIES_REL_PATH  : String = "src/main/resources/rel/relevance-judgements.csv"

  // variable init
  var invIndex     = MHashMap[String, ListBuffer[(String, Int)]]()
  var docTokenSize = MHashMap[String, Double]()

  var collectionFreq: Double = 0
  var totalDocs     : Int = 0

  var queries     : Array[Seq[String]]  = null
  var queriesNum  : Array[String]       = null
  var query       : Seq[String]         = null

  var queryResult : MHashMap[String, ListBuffer[(String, Int)]] = MHashMap.empty

  // functions
  def p(output : String, rel: Int = 4){
    /* auxiliary print function */
    if(rel <= DEBUG){
      println(output)
    }
  }

  def collectionFrequency() {
    for (item <- this.invIndex) {
      for (elem <- item._2) {
        this.collectionFreq += elem._2
      }
    }

    println("Debug: collectionFrequency: Collection frequency is: " + this.collectionFreq)
  }

  def writeToFile(output: String, filename: String = this.OUTPUT_PATH) {
    /* Function to write to an output file */
    if (output.trim() != "") {
      val fw = new FileWriter(filename, true)
      try {
        fw.write("\r\n" + output + "\r\n")
      } finally fw.close()
    }
  }

  def readFromFile(path: String): String = {
    /* Function to read from file */
    val source = scala.io.Source.fromFile(path)
    try source.mkString finally source.close()
  }

  def readQueriesFromFile() {
    /* Function to read query from query description file */
    val source = scala.io.Source.fromFile(this.QUERIES_PATH)
    val lines = try source.mkString finally source.close()
    val preprocessor: Preprocessing = new Preprocessing()
    /* parsing of queries from tipster dataset
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
    */
    val queriesStrings = lines
                        .split("Topic:")
                        .drop(1)
                        .map { x => x.substring(0, x.indexOf("</top>")).trim() }

    this.queries = preprocessor.transform(queriesStrings)

    this.queriesNum = lines
                      .split("Number:")
                      .drop(1)
                      .map { x => x.substring(0, x.indexOf("<title>")).trim() }
                      .filter { x => x.trim() != "" }

    this.queriesNum.foreach(println)
    this.queries.foreach(x => println(x.mkString(" ")))
  }

  def QueryInvIndex(): MHashMap[String, ListBuffer[(String, Int)]] = {
    /* gets pairs : token -> (doc_name, token_freq) for each token in the query */
    val result = MHashMap[String, ListBuffer[(String, Int)]]()

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
    /* creates an inverted index with pairs: token -> (doc_name, token_freq) */

    var watch: StopWatch = new StopWatch()
    watch.start

    val invIndex = new MHashMap[String, ListBuffer[(String, Int)]]()

    // parse documents to list of pairs: doc_name -> (token, frequency)
    val docTokens: ListBuffer[DocToTokensMap] = tipsterParse()

    for (doc <- docTokens) {
      for (token <- doc.tokens) {
        if (invIndex.contains(token._1)) {
          invIndex(token._1) += (doc.name -> token._2)
        } else {
          invIndex += (token._1 -> ListBuffer((doc.name, token._2)))
        }
      }
    }

    watch.stop
    p("Debug: createInvIndex: Total index building time = " + watch.uptonow, 0)

    this.invIndex =  invIndex
    invIndex
  }

  def tipsterParse(): ListBuffer[DocToTokensMap] = {
    /* parses tipster data set to a map of document tokens */

    var watch: StopWatch = new StopWatch()
    watch.start

    val tipster     = new TipsterStream(DOCS_PATH)
    var docTokens   = ListBuffer.empty[DocToTokensMap]
    var index: Int  = 1

    p("Debug: tipsterParse: Number of files in zips = " + tipster.length, 0)

    for (doc <- tipster.stream) { // .take(100000)
      var curDocTokens = createDocToTokensMap(doc)
      docTokens += curDocTokens

      this.docTokenSize += (curDocTokens.name -> calculateDocTokenSize(curDocTokens.tokens))

      // print time
      if (index % 5000 == 0) {
        watch.stop
        println("Index=" + index + " ; Time = " + watch.uptonow)
        watch.start
      }
      index += 1
    }

    watch.stop
    p("Debug: tipsterParse: Total read time = " + watch.uptonow, 0)

    return docTokens
  }

  def createDocToTokensMap(doc: Document): DocToTokensMap = {
    /* creates a map from the document name to its tokens */

    def words: List[String] = Tokenizer.tokenize(doc.content)
    def wordsWithoutSW: Seq[String] = StopWords.filterOutSW(words)
    def porterStemmedList: Seq[String] = wordsWithoutSW.map(word => PorterStemmer.stem(word))

    return new DocToTokensMap(doc.name, porterStemmedList
                                          .groupBy(identity)
                                          .mapValues(_.size) //.filter(p => p._2 > 1)
                                          .toSeq)
  }

  def calculateDocTokenSize(arg: Seq[(String, Int)]): Double = {
    /* calculates total size of the document based on a seq. of term-fr */
    arg.foldLeft(0.0) {
      (sum, elem) =>
        sum + elem._2
    }
  }
}

object SearchEngine {
  val DEBUG = false

  def main(args: Array[String]): Unit = {
    var watch: StopWatch = new StopWatch()
    watch.start

    val obj: SearchEngine = new SearchEngine()
    var queryNum: Int = 0

    // gets all queries
    obj.readQueriesFromFile()
    // creates inverted index
    obj.createInvIndex()

    obj.collectionFrequency()
    var date: String = getDate()
    for (q <- obj.queries) {
      // pass query in question to search engine
      obj.query = q
      obj.QueryInvIndex()


      println("Debug: main: Result of query: " + q.mkString(" "))
      // obj.queryResult.foreach(println)

      val q_weight = 0.1
          println("Debug: \t" + q_weight)
          val docRanks_lm = LanguageModeler(obj, q_weight)
          val docRanks_tfidf = TFIDFModeler(obj)

          println("Debug: main: docRanks:")
          // docRanks.foreach(println)
          var rank_i : Int = 0
          for(docRanks <- List(docRanks_tfidf, docRanks_lm)){
            // Write output to file
            val sortedOutput: Seq[(String, Double)] = docRanks
                                                        .toSeq
                                                        .sortBy(-_._2)
                                                        .take(100)
            // create string to print for each query
            var outputString: String = ""
            var index: Int = 1
            for (o <- sortedOutput) {
              outputString += obj.queriesNum(queryNum) + " " + index.toString() + " " + o._1 + "\r\n"
              index += 1
            }

            var outputFileName : String = "final_queries_" + rank_i + "_" + date + "_" + q_weight.toString
            obj.writeToFile(outputString, outputFileName)
            rank_i += 1
          }


      queryNum += 1
    }

    watch.stop
    println("Debug: main: Time taken to rank all queries = " + watch.uptonow)
    println("Finished!")
  }

  def getDate() : String = {
    // (1) get today's date
    var today : Date= Calendar.getInstance().getTime();

    // (2) create a date "formatter" (the date format we want)
    var formatter : SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-hh.mm.ss");

    // (3) create a new String using the date format we want
    var folderName : String = formatter.format(today);
    folderName
  }

  def p(output : String){
    /* auxiliary print function */
    if(DEBUG){
      println(output)
    }
  }

  def LanguageModeler(obj: SearchEngine,
                      querySizeWeight: Double = 0.1): MHashMap[String, Double] = {

    var watch: StopWatch = new StopWatch()
    watch.start

    var lambda          : Double = 0.0

    // MHashMap[DocName, MHashMap[query_term, occurance_in_document]]
    var docToQueryTermMap = MHashMap[String, MHashMap[String, Double]]()

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

    // MHashMap[doc_name, rank]
    var docRanks: MHashMap[String, Double] = MHashMap.empty
    // Initialize to 0 for each document
    for (d <- docToQueryTermMap) { docRanks += (d._1 -> 0.0) }

    // For this query, calculate ranking of documents
    lambda = 1.0 - (obj.query.size * querySizeWeight)


    for (t <- obj.query) {
      // Proceed only if query term present in corpus
      if (obj.invIndex.contains(t)) {
        // P(w)
        var p_t_mc: Double = 0.0

        p_t_mc = obj.invIndex(t).foldLeft(0.0) {
          (sum, elem) =>
            sum + elem._2
        }

        p_t_mc /= obj.collectionFreq

        for (d <- docToQueryTermMap) {
          var p_t_md: Double = 0.0

          if (d._2.contains(t)) {
            p_t_md = d._2(t) / obj.docTokenSize(d._1)
          }
          docRanks(d._1) += log(((1 - lambda) * p_t_mc) + (lambda * p_t_md))
        }
      }
    }

    watch.stop
    //println("Debug: LanguageModeler: Time taken to rank = " + watch.uptonow)
    return docRanks
  }

  def TFIDFModeler(obj: SearchEngine): MHashMap[String, Double] = {

    var watch: StopWatch = new StopWatch()
    watch.start

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

    // MHashMap[doc_name, rank]
    var docRanks: MHashMap[String, Double] = MHashMap.empty
    // Initialize for each document
    for (d <- docToQueryTermMap) {
      docRanks += (d._1 -> 0.0)
    }

    for (t <- obj.query) {
      // Proceed only if query term present in corpus
      if (obj.queryResult.contains(t)) {
        var log_idf: Double = 0.0

        log_idf = obj.queryResult(t).size
        log_idf = log( obj.totalDocs/log_idf )

        for (d <- docToQueryTermMap) {
          var log_tf: Double = 0.0

          if (d._2.contains(t)) {
            log_tf = d._2(t) / obj.docTokenSize(d._1)
            log_tf = log( 1 + log_tf )
          }

          docRanks(d._1) += (log_tf * log_idf)
        }
      }
    }

    watch.stop
    //println("Debug: ScoreModeler: Time taken to rank = " + watch.uptonow)
    return docRanks
  }
}
