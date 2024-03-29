package org.sunbird.lms.job.report

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobDriver}
import org.sunbird.lms.model.report.CourseEnrollmentModel

object CourseEnrollmentJob extends IJob {
  implicit val className = "org.ekstep.analytics.job.CourseEnrollmentJob"

  def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {
    implicit val sparkContext: SparkContext = sc.getOrElse(null);
    JobLogger.log("Started executing Job")
    JobDriver.run("batch", config, CourseEnrollmentModel)
    JobLogger.log("Job Completed.")
  }
}
