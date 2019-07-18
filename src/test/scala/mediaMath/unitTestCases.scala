package mediaMath
import mediaMath.mediaEvents.spark
import org.apache.spark.sql.functions.{expr, min}
import org.scalatest.FunSuite
class unitTestCases extends FunSuite
  with SparkSessionTestWrapper {

/*
  | Timestamp | Advertiser ID | User ID | Should it be attributed?
    ------------- | ------------- | ------------- | ------------- | -------------
  event | 1450631448 | 1 | 60b74052-fd7e-48e4-aa61-3c14c9c714d5 | No, because there is no impression for the same advertiser and user before this event.
  impression | 1450631450 | 1 | 60b74052-fd7e-48e4-aa61-3c14c9c714d5 | No, only events get attributed.
  event | 1450631452 | 1 | 60b74052-fd7e-48e4-aa61-3c14c9c714d5 | Yes, because there is an impression for the same advertiser and user before this event.
  event | 1450631464 | 1 | 16340204-80e3-411f-82a1-e154c0845cae | No because there is no impression for this user before.
    event | 1450631466 | 2 | 60b74052-fd7e-48e4-aa61-3c14c9c714d5 | No because there is no impression for this user and advertiser before (the advertiser is different).
    event | 1450631468 | 1 | 60b74052-fd7e-48e4-aa61-3c14c9c714d5 | No, because the matching impression (with timestamp 1450631450) has already been matched with event 1450631452.

    */
  test("Should return event with timestamp 1450631452 as attributed event") {
    import spark.sqlContext.implicits._
    val impressionsDF = Seq((1450631450,1,1,"60b74052-fd7e-48e4-aa61-3c14c9c714d5"))
                        .toDF("Itimestamp","Iadvertiser_id","Icreative_id","Iuser_id").as[caseClasses.impressions]

    val eventsDF = Seq((1450631448 , "" ,1, "60b74052-fd7e-48e4-aa61-3c14c9c714d5","click" ),
                      (1450631452 , "" ,1, "60b74052-fd7e-48e4-aa61-3c14c9c714d5","click" ),
                      (1450631464 , "" ,1, "16340204-80e3-411f-82a1-e154c0845cae","click" ),
                      (1450631466 , "" ,1, "60b74052-fd7e-48e4-aa61-3c14c9c714d5","click" ))
                        .toDF("timestamp","event_id","advertiser_id","user_id","event_type").as[caseClasses.events]

    print(eventsDF.getClass)
    implicit val joinedDF = eventsDF.join(impressionsDF,
      eventsDF("advertiser_id")  === impressionsDF("Iadvertiser_id") &&
        eventsDF("user_id")  === impressionsDF("Iuser_id") &&
        eventsDF("timestamp")  >= impressionsDF("Itimestamp"),"inner")
      .withColumn("timestampDiff",expr("timestamp-Itimestamp"))
      .drop("Iadvertiser_id","Iuser_id","Itimestamp")


    val attributedEventsDF = joinedDF.groupBy("timestamp","advertiser_id","event_type", "user_id")
      .agg(min("timestampDiff") as "closestEventOccurance")
      .orderBy("advertiser_id","user_id","event_type")

    attributedEventsDF.createOrReplaceTempView("attributedEventsDF")
    val partitionedLagDF = spark.sql("select *, lag(timestamp,1) over(partition by advertiser_id,event_type,user_id order by timestamp) as lag  from attributedEventsDF")
      .withColumn("rowTimeDiff",expr("timestamp-lag"))
      .orderBy("advertiser_id","user_id","event_type","timestamp","lag")

    val filteredAttributedEventsDF = partitionedLagDF.where($"rowTimeDiff" >=60 || $"rowTimeDiff".isNull)
      .orderBy("advertiser_id","user_id","event_type","timestamp")


    //filteredAttributedEventsDF.show()
    val result = filteredAttributedEventsDF.select("timestamp").as[Int].collect()


    // Check if right value is returned
    assert(result(0) === 1450631452)

    //Check if only one event is attributed
    assert(result.length === 1)
  }



  /*
  Timestamp | Event ID | Advertiser ID | User ID | Event Type
------------- | ------------- | ------------- | ------------- | -------------
1450631450 | 5bb2b119-226d-4bdf-95ad-a1cdf9659789 | 1 | 60b74052-fd7e-48e4-aa61-3c14c9c714d5 | click
1450631452 | 23aa6216-3997-4255-9e10-7e37a1f07060 | 1 | 60b74052-fd7e-48e4-aa61-3c14c9c714d5 | click
1450631464 | 61c3ed32-01f9-43c4-8f54-eee3857104cc | 1 | 60b74052-fd7e-48e4-aa61-3c14c9c714d5 | purchase
1450631466 | 20702cb7-60ca-413a-8244-d22353e2be49 | 1 | 60b74052-fd7e-48e4-aa61-3c14c9c714d5 | click

   */
  test("Should remove 23aa6216-3997-4255-9e10-7e37a1f07060 and 20702cb7-60ca-413a-8244-d22353e2be49 and keep events with timestamps 1450631450 and 1450631464") {
    import spark.sqlContext.implicits._
    val impressionsDF = Seq((1450631449,1,1,"60b74052-fd7e-48e4-aa61-3c14c9c714d5"))
      .toDF("Itimestamp","Iadvertiser_id","Icreative_id","Iuser_id").as[caseClasses.impressions]

    val eventsDF = Seq((1450631450 , "5bb2b119-226d-4bdf-95ad-a1cdf9659789" ,1, "60b74052-fd7e-48e4-aa61-3c14c9c714d5","click" ),
      (1450631452 , "23aa6216-3997-4255-9e10-7e37a1f07060" ,1, "60b74052-fd7e-48e4-aa61-3c14c9c714d5","click" ),
      (1450631464 , "61c3ed32-01f9-43c4-8f54-eee3857104cc" ,1, "60b74052-fd7e-48e4-aa61-3c14c9c714d5","purchase" ),
      (1450631466 , "20702cb7-60ca-413a-8244-d22353e2be49" ,1, "60b74052-fd7e-48e4-aa61-3c14c9c714d5","click" ))
      .toDF("timestamp","event_id","advertiser_id","user_id","event_type").as[caseClasses.events]

    print(eventsDF.getClass)
    implicit val joinedDF = eventsDF.join(impressionsDF,
      eventsDF("advertiser_id")  === impressionsDF("Iadvertiser_id") &&
        eventsDF("user_id")  === impressionsDF("Iuser_id") &&
        eventsDF("timestamp")  >= impressionsDF("Itimestamp"),"inner")
      .withColumn("timestampDiff",expr("timestamp-Itimestamp"))
      .drop("Iadvertiser_id","Iuser_id","Itimestamp")


    val attributedEventsDF = joinedDF.groupBy("timestamp","advertiser_id","event_type", "user_id")
      .agg(min("timestampDiff") as "closestEventOccurance")
      .orderBy("advertiser_id","user_id","event_type")

    attributedEventsDF.createOrReplaceTempView("attributedEventsDF")
    val partitionedLagDF = spark.sql("select *, lag(timestamp,1) over(partition by advertiser_id,event_type,user_id order by timestamp) as lag  from attributedEventsDF")
      .withColumn("rowTimeDiff",expr("timestamp-lag"))
      .orderBy("advertiser_id","user_id","event_type","timestamp","lag")

    val filteredAttributedEventsDF = partitionedLagDF.where($"rowTimeDiff" >=60 || $"rowTimeDiff".isNull)
      .orderBy("advertiser_id","user_id","event_type","timestamp")


    filteredAttributedEventsDF.show()
    val result = filteredAttributedEventsDF.select("timestamp").as[Int].collect()


    // Check if right value is returned
    assert(result(0) === 1450631450)
    assert(result(1) === 1450631464)

    //Check if only one event is attributed
    assert(result.length === 2)
  }


}
