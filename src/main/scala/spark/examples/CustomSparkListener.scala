package spark.examples

import org.apache.spark.scheduler._

class CustomSparkListener extends SparkListener {

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    super.onApplicationStart(applicationStart)
    println(s"\nApp startedAt: ${applicationStart.time}")
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    super.onApplicationEnd(applicationEnd)
    println(s"\nApp endedAt: ${applicationEnd.time}")
  }


  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    super.onStageCompleted(stageCompleted)
    val exetime : Long = System.currentTimeMillis
    println(s"\nStage${stageCompleted.stageInfo.stageId} complitedAt: ${stageCompleted.stageInfo.completionTime.getOrElse(0)}")

  }
}