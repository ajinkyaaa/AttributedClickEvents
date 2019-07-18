package mediaMath
import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {
  lazy val  spark = SparkSession
    .builder()
    .master("spark://Ajinkyas-MBP.fios-router.home:7077")
    .config("spark.cores.max", "4")
    .appName("userEvents")
    .getOrCreate()


}
