package com.test.analytics.member.run

import org.apache.spark.sql.DataFrame

trait EligibilityAnalytics {
  def exec(memberMonth: DataFrame, baseDir: String): Unit
}
