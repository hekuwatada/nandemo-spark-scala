package org.netarrow.app

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

trait SparkApp {
  def appName: String

  def run(jobBlock: SparkSession => Unit)(implicit config: Config): Unit = {
    val ss = createSparkSession(appName)

    try {
      jobBlock(ss)
    } finally {
      ss.stop()
    }
  }

  protected def createSparkSession(appName: String)(implicit config: Config): SparkSession =
    createSparkSessionBuilder(appName).getOrCreate()

  protected def createSparkSessionBuilder(appName: String)(implicit config: Config): SparkSession.Builder = {
    val sparkMaster = config.getString("spark.master")

    SparkSession.builder()
      .master(sparkMaster)
      .appName(appName)
  }
}
