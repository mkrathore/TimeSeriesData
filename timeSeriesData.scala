package com.rakuten.dps.dataplatform.common.ingestion

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.TimestampType



/*
    Part A
    Description : You are given a time series data, which is a clickstream of user activity. Perform Sessionization
    on the data {as per the session definition given below} and generate session ids.
    Expected Steps:
    1. Create in an input file with the data given below in any flat file format, preferably csv.
    2. Read the input data file into your program
    3. Use spark batch {PySpark/Spark-Scala} to add an additional column with name session_id and generate the session
    id's based on the following logic:
        Session expires after inactivity of 30 minutes; because of inactivity, no clickstream will be generated.
        Session remains active for a maximum duration of 2 hours (i.e., after every two hours a new session starts).
    4. Save the resultant data (original data, enriched with Session IDs) in a Parquet file format.
    Given Dataset:
    Timestamp             User_id
    2021-05-01T11:00:00Z      u1
    2021-05-01T13:13:00Z      u1
    2021-05-01T15:00:00Z      u2
    2021-05-01T11:25:00Z      u1
    2021-05-01T15:15:00Z      u2
    2021-05-01T02:13:00Z      u3
    2021-05-03T02:15:00Z      u4
    2021-05-02T11:45:00Z      u1
    2021-05-02T11:00:00Z      u3
    2021-05-03T12:15:00Z      u3
    2021-05-03T11:00:00Z      u4
    2021-05-03T21:00:00Z      u4
    2021-05-04T19:00:00Z      u2
    2021-05-04T09:00:00Z      u3
    2021-05-04T08:15:00Z      u1


    Expected Output:
    TimeStamp             User_id  Session_id(<userid>_<session_number>)
    2021-05-01T11:00:00Z    u1     For example, u1_s1
    2021-05-01T13:13:00Z    u1     For example, u1_s2
    2021-05-01T15:00:00Z    u2     For example, u2_s1
    2021-05-01T11:25:00Z    u1     Please derive based on given logic
    2021-05-01T15:15:00Z    u2     Please derive based on given logic
    2021-05-01T02:13:00Z    u3     Please derive based on given logic
    2021-05-03T02:15:00Z    u4     Please derive based on given logic
    2021-05-02T11:45:00Z    u1     Please derive based on given logic
    2021-05-02T11:00:00Z    u3     Please derive based on given logic
    2021-05-03T12:15:00Z    u3     Please derive based on given logic
    2021-05-03T11:00:00Z    u4     Please derive based on given logic
    2021-05-03T21:00:00Z    u4     Please derive based on given logic
    2021-05-04T19:00:00Z    u2     Please derive based on given logic
    2021-05-04T09:00:00Z    u3     Please derive based on given logic
    2021-05-04T08:15:00Z    u1     Please derive based on given logic

    Part B
    Description: In addition to the above result, consider the following requirement/scenario and give the results
      Get the Number of sessions generated for each day.
      Total time spend by a user in a day.
    Here are the guidelines and instructions for the expected solution:
      Write all the queries for the different requirements in Spark-sql.
      Think in the direction of using partitioning, bucketing, etc.

    Note:
    1. Please do not use direct Spark-sql in section A.
    2. Please use sbt to build the Scala code into an executable jar as follows:
      a. Open a terminal in QuizMe and cd to bse directory of your Scala project that contains build.sbt file.
      b. Make sure that your sbt file has all the required dependencies
      c. Run 'sbt compile' followed by 'sbt package'.
      d. After successful sbt package command, the executable jar is generated in target folder
      e. Execute the jar using command `spark-submit target/jar-file-name.jar`
    3. Note that some exceptions may occur in running the sbt commands but that should not halt the jar creations
    4. sbt compile may take a bit longer to execute for the first time as it downloads all the dependencies

 */
object timeSeriesData extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("timeSeriesData")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()

  import spark.implicits._
  val newDF = Seq(
    ("2021-05-01T11:00:00Z","u1"),
    ("2021-05-01T13:13:00Z","u1"),
    ("2021-05-01T15:00:00Z","u2"),
    ("2021-05-01T11:25:00Z","u1"),
    ("2021-05-01T15:15:00Z","u2"),
    ("2021-05-01T02:13:00Z","u3"),
    ("2021-05-03T02:15:00Z","u4"),
    ("2021-05-02T11:45:00Z","u1"),
    ("2021-05-02T11:00:00Z","u3"),
    ("2021-05-03T12:15:00Z","u3"),
    ("2021-05-03T11:00:00Z","u4"),
    ("2021-05-03T21:00:00Z","u4"),
    ("2021-05-04T19:00:00Z","u2"),
    ("2021-05-04T09:00:00Z","u3"),
    ("2021-05-04T08:15:00Z","u1")).toDF("NewTimeStamp","User_id")


  val interimSessionThreshold=60 * 30
  val totalSessionTimeThreshold=2 * 60 * 60



  /*
  val newDF2 = spark.read
    .format("csv")
    .option("header", "true")
    .load("/Users/mayank.rathore/Downloads/userActivity.csv")
    */

  val newDF1=newDF.withColumn("TimeStamp",unix_timestamp(col("NewTimeStamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'").cast(TimestampType)
    .as("timestamp")).drop("NewTimeStamp")
  def clickSessList(tmo: Long) = udf { (uid: String, clickList: Seq[String], tsList: Seq[Long]) =>
    def sid(n: Long) = s"$uid"+"_s"+ s"$n"

    val sessList = tsList.foldLeft((List[String](), 0L, 0L)) { case ((ls, j, k), i) =>
      if (i == 0 || j + i >= tmo) (sid(k + 1) :: ls, 0L, k + 1) else
        (sid(k) :: ls, j + i, k)
    }._1.reverse

    //clickList zip sessList
  }


  val x = Window.partitionBy("User_id").orderBy(col("TimeStamp").asc)
  val lagTest = lag("TimeStamp", 1, "0000-00-00 00:00:00").over(x)
  //create col with time difference per user session
  val df_test = newDF1
    .withColumn("diff_val_with_previous", unix_timestamp(col("TimeStamp")) - unix_timestamp(lag("TimeStamp", 1).over(x)))
    .withColumn("diff_val_with_previous", when(row_number.over(x) === 1 || col("diff_val_with_previous") >= interimSessionThreshold, 0L)
      .otherwise(col("diff_val_with_previous"))


    )
  //df_test.show()

  val df_test2 = df_test
    .groupBy("User_id").agg(
    collect_list("TimeStamp").as("click_list"), collect_list("diff_val_with_previous").as("ts_list")
  ).withColumn("Session_id",
    explode(clickSessList(totalSessionTimeThreshold)(col("User_id"),col("click_list"),col("ts_list"))
    )).select(col("User_id"), col("Session_id._1").as("click_time"), col("Session_id._2").as("sess_id"))
    df_test2.show()

    //show df with number of sessions created per day
    df_test2.withColumn("Date",to_date(col("click_time"), "yyyy-MM-dd")).drop("click_time")
    .groupBy("Date").agg(countDistinct("sess_id")).as("No. of sessions").show()

  }







