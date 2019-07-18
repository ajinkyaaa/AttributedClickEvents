# AttributedClickEvents

##*Project Description*
- Event: a user's interaction with a website.
- Impression: an occurrence of an ad displayed to a user on a website.
- Attributed event: an event that happened chronologically after an impression and is considered to be the result of that impression.

The advertiser and the user of both the impression and the event have to be the same for the event to be attributable. Example: a user buying an object after seeing an ad from an advertiser.

 

The goal of the application if to attribute events with relevant impressions.

##*Steps for running the report*

- open terminal and run "sbt clean"
- Get root access and navigate to the project folder. Root access is given because sometimes it has issues writing csv files.
- "sbt run"
- count_of_events.csv and count_of_users.csv are generated.
- run command "sbt test" to run unit test cases.


