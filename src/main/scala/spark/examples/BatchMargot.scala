// scalastyle:off println
package spark.examples

import java.io.{PrintWriter, _}
import java.net.InetAddress
import java.util.Scanner

import arg.{DictionaryManager, FeatureExtractor}
import edu.stanford.nlp.parser.lexparser.LexicalizedParser
import edu.stanford.nlp.process.{CoreLabelTokenFactory, PTBTokenizer}
import edu.stanford.nlp.trees.{PennTreebankLanguagePack, TreePrint, WordStemmer}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import util.control.Breaks._

object BatchMargot {

  def main(args: Array[String]) {
    if (args.length < 6) {
      System.err.println("Usage: BatchMargot <inputDir> <pmDir> <outputFile> " +
        "ecThr=<evid-claim-threshold> lThr=<link-threshold> repar=<repar> [debug/run]\n\n" +
        "inputDir : file system or HDFS path\n" +
        "pmDir : path to ParallelMargot directory\n" +
        "outputFile : output file\n" +
        "ecThr : score threshold for evidence and claim classification, typically 0\n" +
        "lThr : score threshold for link classification, typically 0\n" +
        "repar : force the distributed system to create a certin number of partitions\n" +
        "[debug/run] : optional, prints control logs\n")
      System.exit(1)
    }

    /******** INPUT PARAMETERS *********/

    val inputDir = args(0)
    val stemmedDictionariesDirectory: String = args(1)+"/dict/sentencedetection/sdm"
    val claim_model_path: String = args(1) + "/models/model.claim.detection"
    val evidence_model_path: String = args(1) + "/models/model.evidence.detection"
    val link_model_path: String = args(1) +"/models/model_structure_prediction3.svm"
    val svm_classify_path: String = args(1) +"/SVM-Light/svm_classify"
    val outputFile = args(2)
    val ecThr: Double = java.lang.Double.parseDouble(args(3).split("=")(1))
    val lThr: Double = java.lang.Double.parseDouble(args(4).split("=")(1))
    val repar: Integer = Integer.parseInt(args(5).split("=")(1))
    val DEBUG: Boolean = if (args.length > 6 && args(6) == "debug") true else false

    /***********************************/


    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConf = new SparkConf().setAppName("BatchMargot")
      .set("spark.hadoop.textinputformat.record.delimiter", ".")
      .set("spark.hadoop.validateOutputSpecs", "false")
      .set("spark.extraListeners", "spark.examples.CustomSparkListener")

    val sc = new SparkContext(sparkConf)


    /***** JOB INIT *****/
    val lp = LexicalizedParser.loadModel("edu/stanford/nlp/models/lexparser/englishPCFG.ser.gz")
    lp.setOptionFlags("-outputFormatOptions", "stem", "-retainTmpSubcategories", "-outputFormat", "words,oneline")
    val dmStemmed = new DictionaryManager
    dmStemmed.loadFromTextFiles(stemmedDictionariesDirectory)
    sc.broadcast(lp)
    sc.broadcast(dmStemmed)


    println("Input Folder: " + inputDir)

    /******BATCH PROCESSING CORE********/

    var phrases = sc
      .wholeTextFiles(inputDir).filter(x => !x._1.contains("DS_Store"))
      .flatMapValues(x => x.split("\\."))
    if (repar != 0) {
      phrases = phrases.repartition(repar)
    }
  val phrasesElab = phrases
      .filter(_._2.length > 5)
      .mapValues(x => {
        val tokenizerFactory = PTBTokenizer.factory(new CoreLabelTokenFactory(), "")
        val tree = lp.apply(tokenizerFactory.getTokenizer(new StringReader(x + ".")).tokenize())
        new WordStemmer().visitTree(tree) // apply the stemmer to the tree

        val sw = new StringWriter
        val pw = new PrintWriter(sw)
        new TreePrint("words", "stem", new PennTreebankLanguagePack).printTree(tree, pw)
        val theFv = (new FeatureExtractor).createTFIDFBagOfWords(sw.toString, dmStemmed).toSVM
        (x, tree, theFv)
      })

    println("debug = " + DEBUG)

    if (DEBUG) {
      phrasesElab.cache()
      println("PhrasesElab size: " +phrasesElab.count())
      val log = phrasesElab.mapPartitionsWithIndex(
        (index, iterator) => {
          var myList1: List[(Int, String, Int)] = List()
          myList1 = (index, InetAddress.getLocalHost.getHostName, iterator.size) :: myList1
          myList1.iterator
        }
      ).coalesce(1,true).saveAsTextFile(outputFile)
    }

    val out = phrasesElab
      .mapPartitionsWithIndex(
        (index, iterator) => {
          val myList = iterator.toList
          println("\n\n*** 1st PH. Thr:"+Thread.currentThread().getId+" Called in Partition -> " + index + " sentences: "+myList.size)

          /******THIRD_PARTY SOFTWARE TO DETECT CLAIMS******/

          val pb_claim = new java.lang.ProcessBuilder(svm_classify_path,
            "-v", "0",claim_model_path)
          val proc_claim = pb_claim.start
          val in_claim = new Scanner(proc_claim.getInputStream) //read from svm_classify stdout
          val out_claim = new PrintWriter(proc_claim.getOutputStream) //write to svm_classify stdin

          if (DEBUG) {
            val err_claim = new Scanner(proc_claim.getErrorStream) //read from svm_classify stderr
            new Thread() {
              override def run(): Unit = {
                System.err.println("ERR: Debugging thread ERROR started")
                while (err_claim.hasNextLine)
                  System.err.println("ERR:" + err_claim.nextLine)
                err_claim.close()
              }
            }.start()
          }

          /******THIRD_PARTY SOFTWARE TO DETECT EVIDENCE******/
          val pb_evidence = new java.lang.ProcessBuilder(svm_classify_path,
            "-v", "0",evidence_model_path)
          val proc_evidence = pb_evidence.start
          val in_evidence = new Scanner(proc_evidence.getInputStream) //read from svm_classify stdout
          val out_evidence = new PrintWriter(proc_evidence.getOutputStream) //write to svm_classify stdin

          if (DEBUG)  {
            val err_evidence = new Scanner(proc_evidence.getErrorStream) //read from svm_classify stderr
            new Thread() {
              override def run(): Unit = {
                System.err.println("ERR: Debugging thread ERROR started")
                while ( err_evidence.hasNextLine)
                  System.err.println("ERR:" + err_evidence.nextLine)
                err_evidence.close()
              }
            }.start()
          }

          var myList1 : List[(String, (String, String, Double, Double))] = List()
          myList.foreach(x => {
            val theTree = x._2._2
            val theFv = x._2._3
            val input_svm = "0  |BT| " + theTree + " |ET|\t" + theFv + " |EV|\n"
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
              myList1 = (x._1, (x._2._1, theFv, r_claim, r_evidence)) :: myList1
            }
          })
          out_claim.close()
          in_claim.close()
          out_evidence.close()
          in_evidence.close()
          myList1.iterator
        }
      )

    out.name="*******out"
    out.cache


    val claims = out.filter(_._2._3 > ecThr )
    val evidences = out.filter(_._2._4 > ecThr )
      .map(x => {
        val a = x._2._2.split(" ").map( el =>{
          val arr = el.split(":")
          val idx =Integer.parseInt(arr(0)) + 49794
          idx.toString+":"+arr(1)
        }).mkString(" ")

        (x._1,(x._2._1, a , x._2._3, x._2._4))
      })

    claims.name="****claims"
    claims.cache
    evidences.name="*evidences"
    evidences.cache
    println("\nCLAIMS ARE: "+claims.count())
    println("\nEVIDEN ARE: "+evidences.count())
    var pairs = claims.join(evidences,repar).filter{ x => x._2._1._1 != x._2._2._1}

    val links = pairs
      .mapPartitionsWithIndex(
      (index, iterator) => {
      println("\n\n*** 2nd PH. Thr: "+Thread.currentThread().getId+" Called in Partition -> " + index )//+ "\n" + iterator.mkString("\n"))
      val myList = iterator.toList

        /******THIRD_PARTY SOFTWARE TO PREDICT CLAIM-EVIDENCE LINKS******/

      val pb_link = new java.lang.ProcessBuilder(svm_classify_path,
        "-v", "0",link_model_path)
      val proc_link: java.lang.Process = pb_link.start
      val in_link: Scanner = new Scanner(proc_link.getInputStream) //read from svm_classify stdout
      val err_link: Scanner = new Scanner(proc_link.getErrorStream) //read from svm_classify stderr
      val out_link: PrintWriter = new PrintWriter(proc_link.getOutputStream) //write to svm_classify stdin
      if (DEBUG) { new Thread() {
          override def run(): Unit = {
            System.err.println("ERR: Debugging thread ERROR started")
            while (err_link.hasNextLine)
              System.err.println("ERR:" + err_link.nextLine)
            err_link.close()
          }
        }.start() }

      var myList1 : List[(String, (String, String),Double)] = List()
      myList.foreach( x => {
        val c = x._2._1
        val e = x._2._2
        val input_svm = "1  " + c._2 + " " + e._2  + " \n"
        //LINK:
        out_link.print(input_svm)
        out_link.flush()
        var r_link: Double = 0
        breakable {
          try {
            if (in_link.hasNextLine) r_link = in_link.nextLine().toDouble
          } catch {
            case foo: NumberFormatException => {
              break
            }
          }
          myList1 = (x._1, ("CLAIM:" + c._1, "EVID:" + e._1), r_link) :: myList1
        }
      })
        out_link.close()
        in_link.close()
      myList1.iterator
    })
      .filter(_._3>lThr)
    links.name = "*****links"
    links.coalesce(1,true).saveAsTextFile(outputFile)

    println("\nLINKS ARE: "+links.count())

    sc.getRDDStorageInfo.foreach(x => println(x))
  }


}
// scalastyle:on println
