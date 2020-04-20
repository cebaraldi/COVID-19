package covid19

import java.text.SimpleDateFormat
import java.util.Date
import java.io.File

//import org.apache.spark
import org.apache.spark.sql.{DataFrame, Encoders, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

//Province/State,Country/Region,Last Update,Confirmed,Deaths,Recovered
case class CovidData1(State: String,
                      Country: String,
                      Last_Update: Option[Date],
                      Lat: Option[Double],
                      Lon: Option[Double],
                      Confirmed: Option[Int],
                      Deaths: Option[Int],
                      Recovered: Option[Int]
                     )

//Province/State,Country/Region,Last Update,Confirmed,Deaths,Recovered,Latitude,Longitude
case class CovidData2(State: String,
                      Country: String,
                      Last_Update: Option[Date],
                      Lat: Option[Double],
                      Lon: Option[Double],
                      Confirmed: Option[Int],
                      Deaths: Option[Int],
                      Recovered: Option[Int]
                     )

//FIPS,Admin2,Province_State,Country_Region,Last_Update,Lat,Long_,Confirmed,Deaths,Recovered,Active,Combined_Key
case class CovidData3(FIPS: String,
                      Admin2: String,
                      State: String,
                      Country: String,
                      Last_Update: Option[Date],
                      Lat: Option[Double],
                      Lon: Option[Double],
                      Confirmed: Option[Int],
                      Deaths: Option[Int],
                      Recovered: Option[Int],
                      Active: Option[Int],
                      Combined_Key: String
                     )

object ReadDailyCSV {

  val DATE_FORMAT = "MM-dd-yyyy"
  val PATH = "/media/datalake"

  def getDateAsString(d: Date): String = {
    val dateFormat = new SimpleDateFormat(DATE_FORMAT)
    dateFormat.format(d)
  }

  def convertStringToDate(s: String): Date = {
    val dateFormat = new SimpleDateFormat(DATE_FORMAT)
    dateFormat.parse(s)
  }

  def getListOfFiles(dir: File, extensions: List[String]): List[File] = {
    dir.listFiles.filter(_.isFile).toList.filter { file =>
      extensions.exists(file.getName.endsWith(_))
    }
  }

  def toIntOrNull(s: String): Option[Int] = {
    try {
      Some(s.trim.toInt)
    } catch {
      case e: NumberFormatException => None
    }
  }

  def toDblOrNull(s: String): Option[Double] = {
    try {
      Some(s.trim.toDouble)
    } catch {
      case e: NumberFormatException => None
    }
  }

  def toDateTimeOrNull(s: String, dfmt: String): Option[Date] = {
    try {
      val dateFormat = new SimpleDateFormat(dfmt)
      Some(dateFormat.parse(s.trim))
    } catch {
      case e: NumberFormatException => None
    }
  }

//    def readCSVFile(f: File): DataFrame = {
//      println("I - Reading csv files.")
//      val lines: DataFrame = spark.read
//        .format("csv")
//        .option("sep", ";")
//        .option("inferSchema", "true")
//        .option("header", "true")
//        .load(path = f.getAbsolutePath)
//    }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master(master = "local[*]")
      .appName(name = "ReadDailyCSV")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    //val okFileExtensions = List("wav", "mp3")
    val csvExtensions = List("csv")
    val files = getListOfFiles(new File(PATH), csvExtensions)

    println("I - Reading csv files.")
    import scala.math.Ordering.Implicits._
    for (f <- files) {
      val fndate = convertStringToDate(f.getName.split('.')(0))
      if (fndate < convertStringToDate("03-01-2020")) {
        println(s"3rd period: $fndate <- ${f.getName}")
        val lines: DataFrame = spark.read
          .format(source = "csv")
          .option("sep", ";")
          .option("inferSchema", "true")
          .option("header", "true")
          .load(path = f.getAbsolutePath)
      }
      else if (fndate == convertStringToDate("03-01-2020") && fndate < convertStringToDate("03-22-2020")) {
        println(s"3rd period: $fndate <- ${f.getName}")
        val lines: DataFrame = spark.read
          .format(source = "csv")
          .option("sep", ";")
          .option("inferSchema", "true")
          .option("header", "true")
          .load(path = f.getAbsolutePath)
      }
      else {
        println(s"3rd period: $fndate <- ${f.getName}")
        val lines: DataFrame = spark.read
          .format(source = "csv")
          .option("sep", ";")
          .option("inferSchema", "true")
          .option("header", "true")
          .load(path = f.getAbsolutePath)
      }
    }
    println("I - All csv files read.")


    //    val lines = sc.textFile("/media/datalake/04-13-2020.csv").filter(!_.contains("Country_Region"))

    /*import org.apache.spark.sql.SparkSession
    https://stackoverflow.com/questions/18893390/splitting-on-comma-outside-quotes
    "[^"]*"|[^,]+
    With "[^"]*", we match complete "double-quoted strings" |
    we match [^,]+ any characters that are not a comma.
    E.g. string str containing commas inside and outside quotes:

    val csvar = """\"[^\"]*\"|[^,]+""".r
    for (s <- csvar findAllIn str) println(s)
    */

    /*
    val csvar = """\"[^\"]*\"|[^,]+|[^,]+$|^[^,]*""".r
    val regex = "(,)(?=(,))".r // regular expression with lookahead (?=)
    val DATE_FORMAT = "yyyy-MM-dd hh:mm:ss"
    val data = lines.flatMap { line =>
      val line1 = regex.replaceAllIn(line,", ")
      val p = (for (s <- csvar findAllIn line1) yield s.trim()).toList
      //      p foreach println
      Seq(CovidData(
        p(0), p(1), p(2), p(3),
        toDateTimeOrNull(p(4), DATE_FORMAT),
        toDblOrNull(p(5)), toDblOrNull(p(6)),
        toIntOrNull(p(7)), toIntOrNull(p(8)), toIntOrNull(p(9)), toIntOrNull(p(10)),
        p(11)))
    }
    data foreach println
    println(data.count())
  */

    spark.stop()
    println("I - Spark stopped.")
  }
}