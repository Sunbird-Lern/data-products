package org.sunbird.lern.job.report

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobDriver}
import org.ekstep.analytics.framework.util.JobLogger
import org.sunbird.lern.model.report.ETBMetricsModel

object ETBMetricsJob extends optional.Application with IJob {
  implicit val className = "org.sunbird.analytics.job.report.ETBMetricsJob"

  def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {
    implicit val sparkContext: SparkContext = sc.getOrElse(null);
    JobLogger.log("Started executing Job")
    JobDriver.run("batch", config, ETBMetricsModel)
    JobLogger.log("Job Completed.")
  }

}
