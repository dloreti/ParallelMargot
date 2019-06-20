package spark.examples

import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}

/**
  * Created by utente on 25/05/17.
  */
class MyListener() extends StreamingListener {
  override def onBatchCompleted(batchStarted: StreamingListenerBatchCompleted) {
    println("\nTotal delay: " + batchStarted.batchInfo.totalDelay.get.toString)
    println("Total Processing time: " + batchStarted.batchInfo.processingDelay.get.toString)
    println("************************************************************** \n")
  }
}
