export PATH=/usr/lib/kafka/bin:$PATH
kafka-streams-application-reset.sh --application-id cot-aq-ingestion --to-earliest --input-topics cot.airquality
kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic raw-airquality.no2.number
kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic view-airquality.no2.number-gh5-min
kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic view-airquality.no2.number-gh5-hour
kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic view-airquality.no2.number-gh5-day
kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic view-airquality.no2.number-gh5-month
kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic view-airquality.no2.number-gh5-year
kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic view-airquality.no2.number-gh6-min
kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic view-airquality.no2.number-gh6-hour
kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic view-airquality.no2.number-gh6-day
kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic view-airquality.no2.number-gh6-month
kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic view-airquality.no2.number-gh6-year
kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic view-airquality.no2.number-gh7-min
kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic view-airquality.no2.number-gh7-hour
kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic view-airquality.no2.number-gh7-day
kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic view-airquality.no2.number-gh7-month
kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic view-airquality.no2.number-gh7-year
