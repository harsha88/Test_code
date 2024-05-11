package com.test.analytics.member.run

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}

object TotalRunByYear extends EligibilityAnalytics {
  def exec(memberMonth: DataFrame, baseDir: String): Unit = {
    memberMonth.groupBy("member_id", "year")
      .agg(
        count("eligibility_member_month").as("Total member months"),
        min("name").as("Name"),
        min("year").as("ext_year"))
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("member_id", "ext_year")
      .json(baseDir + "member_eligibility_count_per_year/")
  }
}
