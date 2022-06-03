package org.sunbird.lern.job.report

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobDriver}
import org.sunbird.lern.model.report.TextbookProgressModel

object TextbookProgressJob extends optional.Application with IJob {

  implicit val className = "org.sunbird.analytics.job.TextbookProgressJob"

  def main(config: String)(implicit sc: Option[SparkContext], fc: Option[FrameworkContext]): Unit = {
    implicit val sparkContext: SparkContext = sc.getOrElse(null);
    JobLogger.log("Started executing Job")
    JobDriver.run("batch", config, TextbookProgressModel);
    JobLogger.log("Job Completed.")
  }
}
