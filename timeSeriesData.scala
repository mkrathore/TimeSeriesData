import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.TimestampType



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







