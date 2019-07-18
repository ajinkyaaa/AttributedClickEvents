package mediaMath
import org.apache.hadoop.fs._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
object mediaEvents extends App {

  //Create a spark session
    val spark = SparkSession
      .builder()
      .master("spark://Ajinkyas-MBP.fios-router.home:7077")
      .config("spark.cores.max", "4")
      .appName("userEvents")
      .getOrCreate()


    import spark.implicits._

  //Load impression.csv Data
    implicit val impressionsDF = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("./data/impressions.csv").toDF("Itimestamp","Iadvertiser_id","Icreative_id","Iuser_id").as[caseClasses.impressions]

  //Load events.csv data
    implicit val eventsDF = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "false")
      .load("./data/events.csv").toDF("timestamp","event_id","advertiser_id","user_id","event_type").as[caseClasses.events]


  //Join Impression and events dataframes
    val joinedDF = joinDF(impressionsDF, eventsDF)

  //Extract attributed events
    val attributedEventsDF = extractAttributedEvents(joinedDF)


  //Remove duplicate events which occur in 60 seconds
    val filteredAttributedEventsDF = removeDuplicateEvents(attributedEventsDF)


  //Use Case 1:-  **count of events for each advertiser, grouped by event type**.
    val count_of_events = filteredAttributedEventsDF.groupBy("advertiser_id","event_type").count().orderBy("advertiser_id","event_type")

  //Use Case 2:-  **count of unique users for each advertiser, grouped by event type**.
    val count_of_users = filteredAttributedEventsDF.groupBy("advertiser_id","event_type").agg(countDistinct("user_id")).alias("count")
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration);


    fs.delete(new Path("count_of_users.csv"), true);
    fs.delete(new Path("count_of_events.csv"), true);

  //Save to csv files
    saveDataframeToCsv(count_of_events,"count_of_events")
    saveDataframeToCsv(count_of_users,"count_of_users")



    spark.stop()
    print("completed!!!!!!!!!!!")


  //Join events and Impression dataframes
  def joinDF(impressionsDF:Dataset[caseClasses.impressions] , eventsDF:Dataset[caseClasses.events]):Dataset[caseClasses.joinEventsImpressions] = {

     implicit val joinedDF = eventsDF.join(impressionsDF,
        eventsDF("advertiser_id")  === impressionsDF("Iadvertiser_id") &&
        eventsDF("user_id")  === impressionsDF("Iuser_id") &&
        eventsDF("timestamp")  >= impressionsDF("Itimestamp"),"inner")
        .withColumn("timestampDiff",expr("timestamp-Itimestamp"))
        .drop("Iadvertiser_id","Iuser_id","Itimestamp")



    return joinedDF.as[caseClasses.joinEventsImpressions]
  }

  //Extract attributed events
  def extractAttributedEvents(joinedDF:Dataset[caseClasses.joinEventsImpressions]):Dataset[caseClasses.attributedEvents] = {

    val attributedEventsDF = joinedDF.groupBy("timestamp","advertiser_id","event_type", "user_id")
                             .agg(min("timestampDiff") as "closestEventOccurance")
                             .orderBy("advertiser_id","user_id","event_type")



    return attributedEventsDF.as[caseClasses.attributedEvents]
  }

  //Remove duplicates which occur in 60 seconds
  def removeDuplicateEvents(attributedEventsDF:Dataset[caseClasses.attributedEvents]):Dataset[caseClasses.filteredAttributedEvents] = {
    attributedEventsDF.createOrReplaceTempView("attributedEventsDF")
    val partitionedLagDF = spark.sql("select *, lag(timestamp,1) over(partition by advertiser_id,event_type,user_id order by timestamp) as lag  from attributedEventsDF")
                                                      .withColumn("rowTimeDiff",expr("timestamp-lag"))
                                                      .orderBy("advertiser_id","user_id","event_type","timestamp","lag")

    val filteredAttributedEventsDF = partitionedLagDF.where($"rowTimeDiff" >=60 || $"rowTimeDiff".isNull)
                                                     .orderBy("advertiser_id","user_id","event_type","timestamp")

    return filteredAttributedEventsDF.as[caseClasses.filteredAttributedEvents]
  }

  //Generates dataframe
  def saveDataframeToCsv(dataframe:DataFrame,fileName:String) = {

    dataframe.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save("tmp")



    val file = fs.globStatus(new Path("tmp/part*"))(0).getPath().getName();
    fs.rename(new Path("tmp/" + file), new Path(fileName+".csv"));
    fs.delete(new Path("./tmp"), true);

  }


}
