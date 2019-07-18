package mediaMath
import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {
  lazy val  spark = SparkSession
    .builder()
    .master("local[*]")
    .config("spark.cores.max", "4")
    .appName("userEvents")
    .getOrCreate()


}
