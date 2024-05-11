package com.test.analytics.member

import org.apache.spark.sql.{DataFrame, SparkSession}

case class MemberInfo()

object MemberInfo {
  def load()(implicit spark: SparkSession): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("member_eligibility.csv")
  }
}
