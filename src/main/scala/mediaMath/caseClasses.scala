package mediaMath

object caseClasses {
    case class impressions(Itimestamp:Int,
                           Iadvertiser_id:Int,
                           Icreative_id:Int,
                           Iuser_id:String)

    case class events(timestamp:Int,
                      event_id:String,
                      advertiser_id:Int,
                      user_id:String,
                      event_type:String)

    case class joinEventsImpressions(timestamp:Int,
                                     event_id:String,
                                     advertiser_id:Int,
                                     user_id:String,
                                     event_type:String,
                                     Icreative_id:Int,
                                     timestampDiff:Int)

    case class attributedEvents(timestamp:Int,
                                    advertiser_id:Int,
                                    event_type:String,
                                    user_id:String,
                                    closestEventOccurance:Int)

    case class filteredAttributedEvents(timestamp:Int,
                                        advertiser_id:Int,
                                        event_type:String,
                                        user_id:String,
                                        closestEventOccurance:Int,
                                        lag:Int,
                                        rowTimeDiff:Int)

}
