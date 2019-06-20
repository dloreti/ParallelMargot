// scalastyle:off println
package spark.examples


import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext
import java.io._
import java.util.Scanner
import java.util.concurrent.ConcurrentHashMap

import arg.{DictionaryManager, FeatureExtractor}
import edu.stanford.nlp.parser.lexparser.LexicalizedParser
import edu.stanford.nlp.process.{CoreLabelTokenFactory, PTBTokenizer}
import edu.stanford.nlp.trees.{PennTreebankLanguagePack, Tree, TreePrint, WordStemmer}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream

import scala.util.control.Breaks.{break, breakable}


class SentenceDetail(var sentence : String,
                     var FV : String,
                     var c_score : Double,
                     var e_score : Double) extends java.io.Serializable{
  override def toString: String = {
    var endS = if (sentence.length>=10) 9 else sentence.length
    val endF = if (FV.length>=10) 9 else FV.length
    return sentence.substring(0,endS)
  }
}
class LinkDetail(var claim : String,
                 var evidence : String,
                 var l_score : Double) extends java.io.Serializable{
  override def toString: String = {
    return "C:"+claim.substring(0,if (claim.length>=10) 9 else claim.length)+
      " E:"+evidence.substring(0,if (evidence.length>=10) 9 else evidence.length)+
      " ***ls:"+l_score
  }
}

object StreamingMargot {

  val bytesToPhrases = (inputStream : InputStream) => {
    val dataInputStream = new BufferedReader(new InputStreamReader(  inputStream , "UTF-8"))
    new PhraseNextIterator[(String,String)] {
      var currentFile=""
      protected override def getNext() = {
        var ch :Char = dataInputStream.read().toChar
        val phrase = new StringBuilder
        while (ch != '.'){
          phrase.append(ch)
          ch = dataInputStream.read().toChar
        }
        var nextValue = phrase.mkString
        if (nextValue == null) {
          finished = true
          nextValue
        }else if (nextValue.startsWith("FILE NAME:")){
          currentFile=nextValue.split("FILE NAME:")(1)
          nextValue=""
        }
        (currentFile,nextValue+'.')
      }
      protected override def close() {
        dataInputStream.close()
      }
    }
  }

  val linkPrediction = (iterator : Iterator[(String, ((String, String, Double, Double), (String, String, Double, Double)))],
                        svm_classify_path :String, link_model_path : String , DEBUG : Boolean) => {
    val myList = iterator.toList
    var myList1 : List[(String, ((String, String, Double, Double), (String, String, Double, Double)),Double)] = List()

    if (myList.size!=0) {
      println("\n\n*** 2nd PH. Thr: " + Thread.currentThread().getId()) //+ "\n" + iterator.mkString("\n"))

      //SEPARATE PROCESS TO PREDICT LINKS CLAIM -> EVIDENCE
      val pb_link = new java.lang.ProcessBuilder(svm_classify_path,
        "-v", "0", link_model_path)
      val proc_link: java.lang.Process = pb_link.start
      val in_link: Scanner = new Scanner(proc_link.getInputStream) //read from svm_classify stdout
      val err_link: Scanner = new Scanner(proc_link.getErrorStream) //read from svm_classify stderr
      val out_link: PrintWriter = new PrintWriter(proc_link.getOutputStream) //write to svm_classify stdin
      if (DEBUG) {
        new Thread() {
          override def run(): Unit = {
            while (err_link.hasNextLine) System.err.println("ERR:" + err_link.nextLine)
          }
        }.start()
      }


      myList.foreach(x => {
        val c = x._2._1
        val e = x._2._2
        var input_svm = "1  " + c._2 + " " + e._2 + " \n"
        //LINK:
        out_link.print(input_svm)
        out_link.flush()
        var r_link: Double = 0
        if (in_link.hasNextLine) {
          r_link = in_link.nextLine().toDouble
        }
        myList1 = (x._1, x._2, r_link) :: myList1
      })
      out_link.close()
    }
    myList1.iterator
  }

  val updateClaimColl = (values: Seq[SentenceDetail], state: Option[Seq[SentenceDetail]]) => {
    state match {
      case None =>{
        Some(values)
      }
      case Some(s) =>{
        Some(values ++ s)
      }
    }
  } : Option[Seq[SentenceDetail]]

  val updateEvidColl = (values: Seq[SentenceDetail], state: Option[Seq[SentenceDetail]]) => {
    state match {
      case None =>{
        Some(values)
      }
      case Some(s) =>{
        Some(values ++ s)
      }
    }
  } : Option[Seq[SentenceDetail]]



  def main(args: Array[String]) {
    if (args.length < 9) {
      System.err.println("Usage: BatchMargot <hostname> <port> <pmDir> <checkpoint>" +
        "ecThr=<evid-claim-threshold> lThr=<link-threshold> repar=<repar> " +
        "<batchs> <winMultiplier>[debug/run]\n\n" +

        "hostname : hostname of the server sending text \n" +
        "port : port of the server sending text \n" +
        "pmDir : path to ParallelMargot directory\n" +
        "checkpoint : checkpoint directory\n" +
        "ecThr : score threshold for evidence and claim classification, typically 0\n" +
        "lThr : score threshold for link classification, typically 0\n" +
        "repar : force the distributed system to create a certin number of partitions\n" +
        "batchs : microbatch period\n" +
        "winMultiplier : dimension of the join window. 0 for no window\n" +
        "[debug/run] : optional, prints control logs\n")
      System.exit(1)
    }
    /******** INPUT PARAMETERS *********/
    val hostname = args(0)
    val port : Int= args(1).toInt
    val stemmedDictionariesDirectory : String = args(2)+"/dict/sentencedetection/sdm"

    val claim_model_path : String  = args(2)+"/models/model.claim.detection"
    val evidence_model_path : String = args(2)+"/models/model.evidence.detection"
    val link_model_path : String = args(2)+"/models/model_structure_prediction3.svm"
    val svm_classify_path : String = args(2)  //+"/SVM-Light/svm_classify"

    val checkpoint : String = args(3)
    val ecThr : Double = java.lang.Double.parseDouble(args(4).split("=")(1))
    val lThr : Double = java.lang.Double.parseDouble(args(5).split("=")(1))
    val repar : Int = args(6).split("=")(1).toInt
    val batchs : Int= args(7).split("=")(1).toInt
    val winMultiplier : Int = args(8).split("=")(1).toInt

    val DEBUG: Boolean = if (args.length>9 && args(9)=="debug") true else false


    Logger.getLogger("org").setLevel(Level.ERROR)



    val sparkConf = new SparkConf().setAppName("StreamingMargot")
    sparkConf.set("spark.shuffle.blockTransferService", "nio")


    val ssc = new StreamingContext(sparkConf, Seconds(batchs))
    ssc.checkpoint(checkpoint)
    ssc.addStreamingListener(new MyListener)


    //***** JOB INIT *****
    val lp = LexicalizedParser.loadModel("edu/stanford/nlp/models/lexparser/englishPCFG.ser.gz")
    lp.setOptionFlags("-outputFormatOptions", "stem", "-retainTmpSubcategories","-outputFormat", "words,oneline")
    val dmStemmed = new DictionaryManager
    dmStemmed.loadFromTextFiles(stemmedDictionariesDirectory)


    var phrases1 = ssc
      .socketStream(hostname, port, bytesToPhrases, StorageLevel.MEMORY_ONLY_SER)
      .mapValues(x => x.trim)
      .filter(_._2.length > 5)
    if (repar!=0) {
      phrases1 = phrases1.repartition(repar)
      phrases1.foreachRDD(rdd => {
        println("Partitions: " + rdd.partitions.size)
      })
    }

    val phrases =  phrases1.mapValues( x => {
      val tokenizerFactory = PTBTokenizer.factory(new CoreLabelTokenFactory(), "")
      val tree = lp.apply(tokenizerFactory.getTokenizer(new StringReader(x + ".")).tokenize())
      new WordStemmer().visitTree(tree) // apply the stemmer to the tree

      val sw = new StringWriter
      val pw = new PrintWriter(sw)
      new TreePrint("words", "stem", new PennTreebankLanguagePack).printTree(tree, pw)
      val theFv = (new FeatureExtractor).createTFIDFBagOfWords(sw.toString, dmStemmed).toSVM
      (x,tree,theFv)
    })
      .mapPartitions(iterator => {
        val myList = iterator.toList
        var myList1: List[(String, SentenceDetail)] = List()
        if (myList.size!=0){

          //THIRD-PARTY SOFTWARE TO DETECT CLAIMS
          val pb_claim = new java.lang.ProcessBuilder(svm_classify_path,
            "-v", "0", claim_model_path)
          val proc_claim = pb_claim.start
          val in_claim = new Scanner(proc_claim.getInputStream) //read from svm_classify stdout
          val out_claim = new PrintWriter(proc_claim.getOutputStream) //write to svm_classify stdin

          if (DEBUG) {
            val err_claim = new Scanner(proc_claim.getErrorStream) //read from svm_classify stderr
            new Thread() {
              override def run(): Unit = {
                while ( err_claim.hasNextLine) System.err.println("ERR:" + err_claim.nextLine)
              }
            }.start()
          }

          //THIRD-PARTY SOFTWARE TO DETECT EVIDENCES
          val pb_evidence = new java.lang.ProcessBuilder(svm_classify_path,
            "-v", "0", evidence_model_path)
          val proc_evidence = pb_evidence.start
          val in_evidence = new Scanner(proc_evidence.getInputStream) //read from svm_classify stdout
          val out_evidence = new PrintWriter(proc_evidence.getOutputStream) //write to svm_classify stdin

          if (DEBUG) {
            val err_evidence = new Scanner(proc_evidence.getErrorStream) //read from svm_classify stderr
            new Thread() {
              override def run(): Unit = {
                while (err_evidence.hasNextLine) System.err.println("ERR:" + err_evidence.nextLine)
              }
            }.start()
          }

          myList.foreach(x => {
            val end=if (x._2._1.length < 10) x._2._1.length else 10
            var theTree = x._2._2
            var theFv = x._2._3
            var input_svm = "0  |BT| " + theTree + " |ET|\t" + theFv + " |EV|\n"
            //CLAIM and EVIDENCE print on stdin:
            out_claim.print(input_svm)
            out_claim.flush()
            out_evidence.print(input_svm)
            out_evidence.flush()
            //CLAIM and EVIDENCE score read from stdout :
            var r_claim: Double = 0
            var r_evidence: Double = 0
            breakable {
              try {
                if (in_claim.hasNextLine) r_claim = in_claim.nextLine().toDouble
                if (in_evidence.hasNextLine) r_evidence = in_evidence.nextLine().toDouble
              } catch {
                case foo: NumberFormatException => {
                  break
                }
              }
              myList1 = (x._1, new SentenceDetail(x._2._1, theFv, r_claim, r_evidence)) :: myList1
            }
          })
          out_claim.close()
          out_evidence.close()
        }
        myList1.iterator
      })
    phrases.cache()

    if (DEBUG) {
      phrases.map(el => (el._1, 1))
        .mapWithState(StateSpec.function((key: String, value: Option[Int], state: State[Long]) => {
          val sum = value.getOrElse(0).toLong + state.getOption.getOrElse(0L)
          val output = (key, sum)
          state.update(sum)
          output
        }))
        .reduceByKey(math.max(_,_))
        .foreachRDD(rdd => rdd.foreach(el => {
          println("\nIn FN:" + el._1 + " PHRASES ARE: " + el._2)
        }))
    }


    var claims = phrases.filter(_._2.c_score > ecThr )
    var evidences = phrases.filter(_._2.e_score > ecThr )
      .map(x => {
        x._2.FV = x._2.FV.split(" ").map( el =>{
          val arr = el.split(":")
          val idx = Integer.parseInt(arr(0)) + 49794
          idx.toString+":"+arr(1)
        }).mkString(" ")
        x
      })

    if (DEBUG) {
      claims
        .map(el => (el._1, 1))
        .mapWithState(StateSpec.function((key: String, value: Option[Int], state: State[Long]) => {
          val sum = value.getOrElse(0).toLong + state.getOption.getOrElse(0L)
          val output = (key, sum)
          state.update(sum)
          output
        }))
        .reduceByKey(math.max(_,_))
        .foreachRDD(rdd => rdd.foreach(el => {
          println("\nIn FN:" + el._1 + " CLAIMS ARE: " + el._2)
        }))
      evidences
        .map(el => (el._1, 1))
        .mapWithState(StateSpec.function((key: String, value: Option[Int], state: State[Long]) => {
          val sum = value.getOrElse(0).toLong + state.getOption.getOrElse(0L)
          val output = (key, sum)
          state.update(sum)
          output
        }))
        .reduceByKey(math.max(_,_))
        .foreachRDD(rdd => rdd.foreach(el => {
          println("\nIn FN:" + el._1 + " EVIDEN ARE: " + el._2)
        }))
    }


    var pair :  DStream[(String, (SentenceDetail, SentenceDetail))] = null

    if (winMultiplier!=0){
      pair = claims.window(Seconds(batchs*winMultiplier)).join(evidences.window(Seconds(batchs*winMultiplier)))
    }else{
      val claimsColl = claims
        .updateStateByKey(updateClaimColl)
        .flatMapValues( x => {x.iterator} )
      pair=claimsColl.join(evidences)
    }

    if (DEBUG) {
      pair.foreachRDD(rdd => rdd.foreach(el => {
        println("FN:" + el._1 + " -> PAIR: " + el._2)
      }))
      pair
        .map(el => (el._1, 1))
        .mapWithState(StateSpec.function((key: String, value: Option[Int], state: State[Long]) => {
        val sum = value.getOrElse(0).toLong + state.getOption.getOrElse(0L)
        val output = (key, sum)
        state.update(sum)
        output
      }))
        .reduceByKey(math.max(_,_))
        .foreachRDD(rdd => rdd.foreach(el => {
          println("\nIn FN:" + el._1 + " PAIRS ARE: " + el._2)
        }))
    }

    val links = pair
      .filter(x => {x._2._1.sentence != x._2._2.sentence})
      .mapPartitions(iterator => {
      val myList = iterator.toList
      var myList1 : List[(String, LinkDetail)] = List()

      if (myList.size!=0) {
        //THIRD-PARTY SOFTWARE TO PREDICT LINKS (CLAIM <-> EVIDENCE)
        val pb_link = new java.lang.ProcessBuilder(svm_classify_path,
          "-v", "0", link_model_path)
        val proc_link: java.lang.Process = pb_link.start
        val in_link: Scanner = new Scanner(proc_link.getInputStream) //read from svm_classify stdout
        val err_link: Scanner = new Scanner(proc_link.getErrorStream) //read from svm_classify stderr
        val out_link: PrintWriter = new PrintWriter(proc_link.getOutputStream) //write to svm_classify stdin
        if (DEBUG) {
          new Thread() {
            override def run(): Unit = {
              while (err_link.hasNextLine) System.err.println("ERR:" + err_link.nextLine)
            }
          }.start()
        }


        myList.foreach(x => {
          val c = x._2._1
          val e = x._2._2
          if (c.sentence!=e.sentence) {
            var input_svm = "1  " + c.FV + " " + e.FV + " \n"
            //LINK:
            out_link.print(input_svm)
            out_link.flush()
            var r_link: Double = 0
            if (in_link.hasNextLine) {
              r_link = in_link.nextLine().toDouble
            }
            myList1 = (x._1, new LinkDetail(c.sentence,e.sentence, r_link)) :: myList1
          }
        })
        out_link.close()
      }
      myList1.iterator
    })
      .filter(_._2.l_score>lThr)


    links
      .map(el => (el._1, 1))
      .mapWithState(StateSpec.function((key: String, value: Option[Int], state: State[Long]) => {
        val sum = value.getOrElse(0).toLong + state.getOption.getOrElse(0L)
        val output = (key, sum)
        state.update(sum)
        output
      }))
      .reduceByKey(math.max(_,_))
      .foreachRDD(rdd => rdd.collect().foreach(el => {
        println("\nIn FN:" + el._1 + " there are " + el._2+ " LINKS")
      }))



    links.foreachRDD(rdd =>{
      rdd.collect().foreach(el => println("In FN:" + el._1 + " -> LINK : " + el._2)
      )})


    ssc.start()
    ssc.awaitTermination()

  }


}



// scalastyle:on println
