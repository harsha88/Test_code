package com.test.analytics

import com.test.analytics.member.MemberMonth.DataframeExt
import com.test.analytics.member.MemberMonth
import com.test.analytics.member.run.{TotalRun, TotalRunByYear}
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

object EntryPoint {
  def main(args: Array[String]): Unit = {
    // Uncomment below and specify the hadoop.home.dir which
    // should contain winutils.exe if you are trying yo run this app on windows machine.
    // For linux based VMs/machines its not needed.
    //System.setProperty("hadoop.home.dir", "<Absolute path to winutils.exe>")

    if (args.length < 1) {
      println(
        """
          |Fewer arguments provided.
          |Usage:
          |Provide positional arguments as specified below
          |Position 1: Provide run type for this app. Allowed values are Total, Total_by_year, All
          |Position 2: Base location to store output, optional. Default: current directory
          |""".stripMargin)
      return
    }
    val runType = args(0)
    val baseDir = if (args.length > 1) args(1) else ""

    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("Member eligibility check")
      .master("local[4]") //For local run
      .config("spark.driver.host", "localhost")
      .getOrCreate()

    println(s"Run arguments: ${args.mkString("Array(", ", ", ")")}")

    Try {
      println("Loading Member data")
      val memberDf = MemberMonth.load().withMemberInfo() //May handle more exception here
      println("Loaded member data successfully")
      runType.toLowerCase() match {
        case "total" => TotalRun.exec(memberDf, baseDir)
        case "total_by_year" => TotalRunByYear.exec(memberDf, baseDir)
        case "all" =>
          TotalRun.exec(memberDf, baseDir)
          TotalRunByYear.exec(memberDf, baseDir)
        case _ => throw new IllegalArgumentException("Please specify a valid run type.")
      }
    } match {
      case Failure(ex) => println(s"Analytics failed. Exception $ex")
      case Success(_) => println("Process finished successfully.")
    }

  }
}
