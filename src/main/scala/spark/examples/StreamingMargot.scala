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
    return sentence.substring(0,endS) //+" FV:"+FV.substring(0,endF)+" cs:"+c_score+" es:"+e_score
  }
}
class LinkDetail(var claim : String,
                 var evidence : String,
                 var l_score : Double) extends java.io.Serializable{
  override def toString: String = {
    //return "C:"+claim+" E:"+evidence+ " ***ls:"+l_score
    return "C:"+claim.substring(0,if (claim.length>=10) 9 else claim.length)+
      " E:"+evidence.substring(0,if (evidence.length>=10) 9 else evidence.length)+
      " ***ls:"+l_score
  }
}

object StreamingMargot {

  /*
  * \n replacing and trim should be performed afterwards!! */
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
        var nextValue = phrase.mkString //.trim
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
            //System.err.println("ERR: Debugging thread ERROR started")
            while (err_link.hasNextLine) System.err.println("ERR:" + err_link.nextLine)
          }
        }.start()
      }


      myList.foreach(x => {
        val c = x._2._1
        val e = x._2._2
        /*println("*** Thr:"+Thread.currentThread().getId()+ " call svm_classify for link between " +
          "c="+c._1.substring(0,(if (c._1.length < 10) c._1.length else 10))+
          " and e="+e._1.substring(0,(if (e._1.length < 10) e._1.length else 10)) )*/
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
  /**
    * run as follows:
    * ssh -i /Users/daniela/Dropbox/ALMA-Dropbox/RICERCA/TOOLS/2018-02-pike-ip236/k-dani.priv ubuntu@12.8.0.78
    * ubuntu@parallelmargot-master-medium-0:~$ ./code/echoBs.sh 1000 Weapons-cleaned.txt | tee /dev/tty| nc -lk 9999
    * run from IntelliJ IDEA with arguments: 12.8.0.78 9999 testpath 1000 debug
    *
    */

  def main(args: Array[String]) {
    if (args.length < 4) {
      /*System.err.println("Usage: ParallelMargot <hostname> <port> <svm_bin_path> <swipl_path> <batchms> <repartitions>" +
                                                " <subtraceCompleteCheck[true/false]> <model_id> <nTraces> [debug/run]")*/
      System.err.println("Usage!")//: StreamingMargot <hostname> <port> <svm_bin_path> <batchms> [debug/run]")
      System.exit(1)
    }
    /******** LETTURA PARAMETRI *********/
    val hostname = args(0)
    val port : Int= args(1).toInt
    val stemmedDictionariesDirectory : String = args(2)+"/dict/sentencedetection/sdm"


    val claim_model_path : String  = args(2)+"/models/model.claim.detection"
    val evidence_model_path : String = args(2)+"/models/model.evidence.detection"
    val link_model_path : String = args(3) //+"/models/model_structure_prediction3.svm"
    val svm_classify_path : String = args(4)  //+"/SVM-Light-1.5-to-be-released/svm_classify"
    val checkpoint : String = args(5)

    val ecThr : Double = java.lang.Double.parseDouble(args(6).split("=")(1))
    val lThr : Double = java.lang.Double.parseDouble(args(7).split("=")(1))
    val repar : Int = args(8).split("=")(1).toInt
    val batchs : Int= args(9).split("=")(1).toInt
    val winMultiplier : Int = args(10).split("=")(1).toInt

    val DEBUG: Boolean = if (args.length>11 && args(11)=="debug") true else false
    println("debug = "+DEBUG)
    println("repar = "+repar)
    println("batchs = "+batchs)


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
    //ssc.sparkContext.broadcast(lp)
    //ssc.sparkContext.broadcast(dmStemmed)


    var phrases1 = ssc
      .socketStream(hostname, port, bytesToPhrases, StorageLevel.MEMORY_ONLY_SER)
      .mapValues(x => x.trim)
      //.filter(_._2!=".")
      .filter(_._2.length > 5)
    if (repar!=0) {
      phrases1 = phrases1.repartition(repar)
      phrases1.foreachRDD(rdd => {
        println("Partitions: " + rdd.partitions.size)
      })
    }

    //.mapValues(sentence => {sentence.trim().replace("\n", " ")})
    val phrases =  phrases1.mapValues( x => {
      val tokenizerFactory = PTBTokenizer.factory(new CoreLabelTokenFactory(), "")
      val tree = lp.apply(tokenizerFactory.getTokenizer(new StringReader(x + ".")).tokenize())  //x already has .?!
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
          //println("\n\n*** 1st PH. Thr:" + Thread.currentThread().getId) //+ "\n" + iterator.mkString("\n"))

          //SEPARATE PROCESS TO DETECT CLAIMS
          val pb_claim = new java.lang.ProcessBuilder(svm_classify_path,
            "-v", "0", claim_model_path)
          val proc_claim = pb_claim.start
          val in_claim = new Scanner(proc_claim.getInputStream) //read from svm_classify stdout
          val out_claim = new PrintWriter(proc_claim.getOutputStream) //write to svm_classify stdin

          if (DEBUG) {
            val err_claim = new Scanner(proc_claim.getErrorStream) //read from svm_classify stderr
            new Thread() {
              override def run(): Unit = {
                //System.err.println("ERR: Debugging thread ERROR started")
                while ( err_claim.hasNextLine) System.err.println("ERR:" + err_claim.nextLine)
              }
            }.start()
          }

          //SEPARATE PROCESS TO DETECT EVIDENCES
          val pb_evidence = new java.lang.ProcessBuilder(svm_classify_path,
            "-v", "0", evidence_model_path)
          val proc_evidence = pb_evidence.start
          val in_evidence = new Scanner(proc_evidence.getInputStream) //read from svm_classify stdout
          val out_evidence = new PrintWriter(proc_evidence.getOutputStream) //write to svm_classify stdin

          if (DEBUG) {
            val err_evidence = new Scanner(proc_evidence.getErrorStream) //read from svm_classify stderr
            new Thread() {
              override def run(): Unit = {
                //System.err.println("ERR: Debugging thread ERROR started")
                while (err_evidence.hasNextLine) System.err.println("ERR:" + err_evidence.nextLine)
              }
            }.start()
          }

          //var myList1: List[(String, (String, String, Double, Double))] = List()
          myList.foreach(x => {
            val end=if (x._2._1.length < 10) x._2._1.length else 10
            //println("*** 1st PH. Thr:"+Thread.currentThread().getId()+ " call svm_classify on sentence="+x._2._1.substring(0,end))
            var theTree = x._2._2
            var theFv = x._2._3
            var input_svm = "0  |BT| " + theTree + " |ET|\t" + theFv + " |EV|\n"
            //println(input_svm)
            //CLAIM and EVIDENCE print on stdin:
            out_claim.print(input_svm)
            out_claim.flush()
            out_evidence.print(input_svm)
            out_evidence.flush()
            //CLAIM and EVIDENCE score read from stdout :
            var r_claim: Double = 0
            var r_evidence: Double = 0
            //if (in_claim.hasNextLine) r_claim = in_claim.nextLine().toDouble
            //if (in_evidence.hasNextLine) r_evidence = in_evidence.nextLine().toDouble
            //myList1 = (x._1, new SentenceDetail(x._2._1, theFv, r_claim, r_evidence)) :: myList1
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
          val idx = Integer.parseInt(arr(0)) + 49794 //49586
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
      /*claims= claims.window(Seconds(batchs*winMultiplier))
      evidences=evidences.window(Seconds(batchs*winMultiplier))
      pair = claims.join(evidences)*/
      pair = claims.window(Seconds(batchs*winMultiplier)).join(evidences.window(Seconds(batchs*winMultiplier)))
    }else{
      /*var claimsColl : DStream[(String, SentenceDetail)] = null
      var evidColl : DStream[(String, SentenceDetail)] = null
      if (claimsColl!=null)
        pair=claims.join(evidColl).union(claimsColl.join(evidences)).union(claims.join(evidences))
      else
        pair=claims.join(evidences)
      claimsColl = claims
        .updateStateByKey(updateClaimColl).flatMapValues( x => {x.iterator} )
      evidColl = evidences
        .updateStateByKey(updateEvidColl).flatMapValues( x => {x.iterator} )*/
      val claimsColl = claims
        .updateStateByKey(updateClaimColl).flatMapValues( x => {x.iterator} )
      pair=claimsColl.join(evidences)
    }


    /*
        val claimsColl = claims
          .updateStateByKey(updateClaimColl)
          .flatMap( x =>{x._2.map( el => (x._1,el))})
        /*.flatMapValues(cList=>{
          cList.iterator
        })
        .mapWithState(StateSpec.function((key: String, value: Option[SentenceDetail], state: State[Iterator[SentenceDetail]]) => {
        val coll =  state.getOption.getOrElse(Iterator()) ++ Iterator(value.getOrElse(null))
        val output = (key, coll)
        state.update(coll)
        output
      }))
        .flatMap( x =>{x._2.map( cl => (x._1,cl))})*/
        claimsColl.foreachRDD(rdd =>{
          rdd.foreach(el => {
            println("FN:" + el._1 + " -> CLAIM : " + el._2)
          })
        })

        val evidColl = evidences
          .updateStateByKey(updateEvidColl)
          .flatMap( x =>{x._2.map( el => (x._1,el))})
          /*.mapWithState(StateSpec.function((key: String, value: Option[SentenceDetail], state: State[Iterator[SentenceDetail]]) => {
            val coll = state.getOption.getOrElse(Iterator()) ++ Iterator(value.getOrElse(null))
            val output = (key, value)
            state.update(coll)
            output}))
          .flatMap( x =>{x._2.map( ev => (x._1,ev))})*/
        evidColl.foreachRDD(rdd => rdd.foreach(el => {
          println("FN:" + el._1 + " -> EVID : " + el._2)
        }))


        val pair1 = claims //combine each claim in the current batch with all the past evidences
          .join(evidColl)
        val pair2 = evidences //combine each evidence in the current batch with all the past claims
          .join(claimsColl).mapValues(a => (a._2,a._1))
        val pair3 = claims
          .join(evidences)
        var pair = pair1
          .union(pair2)
          //.union(pair3)
            .transform(rdd => rdd.distinct())
        */



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
        //println("\n\n*** 2nd PH. Thr: " + Thread.currentThread().getId())

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
              //System.err.println("ERR: Debugging thread ERROR started")
              while (err_link.hasNextLine) System.err.println("ERR:" + err_link.nextLine)
            }
          }.start()
        }


        myList.foreach(x => {
          val c = x._2._1
          val e = x._2._2
          /*println("*** Thr:"+Thread.currentThread().getId()+ " call svm_classify for link between " +
            "c="+c._1.substring(0,(if (c._1.length < 10) c._1.length else 10))+
            " and e="+e._1.substring(0,(if (e._1.length < 10) e._1.length else 10)) )*/
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

    //if (DEBUG) {
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
    //}



    //links.print(10)
    links.foreachRDD(rdd =>{
      rdd.collect().foreach(el => println("In FN:" + el._1 + " -> LINK : " + el._2)
      )})


    ssc.start()
    ssc.awaitTermination()

  }


}



// scalastyle:on println
