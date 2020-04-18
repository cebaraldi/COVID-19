package covid19

import org.apache.spark.{SparkConf, SparkContext}
import java.text.SimpleDateFormat
import java.util.Date

case class CovidData(FIPS: String,
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

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Read Daily CSV").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val lines = sc.textFile("/media/datalake/04-13-2020.csv").filter(!_.contains("Country_Region"))

    /*
    https://stackoverflow.com/questions/18893390/splitting-on-comma-outside-quotes
    "[^"]*"|[^,]+
    With "[^"]*", we match complete "double-quoted strings" |
    we match [^,]+ any characters that are not a comma.
    E.g. string str containing commas inside and outside quotes:

    val csvar = """\"[^\"]*\"|[^,]+""".r
    for (s <- csvar findAllIn str) println(s)
    */
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
  }
}