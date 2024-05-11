package com.test.analytics.member

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class MemberMonth()

object MemberMonth {
  def load()(implicit spark: SparkSession): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("member_months.csv")
      .withColumnRenamed("member_id", "member_id_mm")
  }

  implicit class DataframeExt(df: DataFrame)(implicit spark: SparkSession) {
    def withMemberInfo(): DataFrame = {
      val memberInfo = MemberInfo.load()
      memberInfo.join(df, memberInfo("member_id") === df("member_id_mm"))
        .drop("member_id_mm")
        .select(
          col("member_id"),
          concat_ws(" ", col("first_name"), col("middle_name"), col("last_name")).as("name"),
          col("eligibility_member_month"),
          col("eligibility_member_month").substr(0, 4).as("year")
        )
    }
  }
}
