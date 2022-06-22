package org.sunbird.lms.job.report

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobDriver}
import org.sunbird.lms.model.report.AssessmentCorrectionModel

object AssessmentCorrectionJob extends IJob {

  implicit val className = "org.ekstep.analytics.job.AssessmentCorrectionJob"

  def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {
    implicit val sparkContext: SparkContext = sc.orNull
    JobLogger.log("Started executing Job")
    JobDriver.run("batch", config, AssessmentCorrectionModel);
    JobLogger.log("Job Completed.")
  }

}
