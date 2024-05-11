package com.test.analytics.member.run

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._

object TotalRun extends EligibilityAnalytics{
  def exec(memberMonth: DataFrame, baseDir: String): Unit = {
    memberMonth.groupBy("member_id")
      .agg(
        count("eligibility_member_month").as("Total member months"),
        min("name").as("Name"))
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("member_id")
      .json(baseDir + "member_eligibility_count/")
  }
}
