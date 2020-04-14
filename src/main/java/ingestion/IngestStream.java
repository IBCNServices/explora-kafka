/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ingestion;

import model.AggregateValueTuple;
import model.AirQualityKeyedReading;
import model.AirQualityReading;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.HostInfo;
import querying.QueryingService;
import util.AppConfig;
import util.TSExtractor;
import util.serdes.JsonPOJODeserializer;
import util.serdes.JsonPOJOSerializer;
import org.apache.commons.cli.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.commons.lang3.time.DateUtils.truncate;

public class IngestStream {

    public static final List<String> METRICS = AppConfig.SUPPORTED_METRICS;    public static final String READINGS_TOPIC = System.getenv("READINGS_TOPIC") != null ? System.getenv("READINGS_TOPIC") : "cot.airquality";
    public static final String APP_NAME = System.getenv("APP_NAME") != null ? System.getenv("APP_NAME") : "explora-ingestion";
    public static final String KBROKERS = System.getenv("KBROKERS") != null ? System.getenv("KBROKERS") : "10.10.139.32:9092";
    public static final String REST_ENDPOINT_HOSTNAME = System.getenv("REST_ENDPOINT_HOSTNAME") != null ? System.getenv("REST_ENDPOINT_HOSTNAME") : "localhost";
    public static final int REST_ENDPOINT_PORT = System.getenv("REST_ENDPOINT_PORT") != null ? Integer.parseInt(System.getenv("REST_ENDPOINT_PORT")) : 7070;
    public static final List<Integer> GEOHASH_PRECISION_LIST = AppConfig.SUPPORTED_GH_PRECISION;
    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd:HHmmss:SSS");

    private static AggregateValueTuple airQReadingAggregator(String key, AirQualityReading value, AggregateValueTuple aggregate) {
        aggregate.gh_ts = key;
        aggregate.gh = key.split("#")[0];
        aggregate.ts = LocalDateTime.parse(key.split("#")[1], DateTimeFormatter.ofPattern("yyyyMMdd:HHmmss:SSS")).toInstant(ZoneId.systemDefault().getRules().getOffset(Instant.now())).toEpochMilli();
        aggregate.count = aggregate.count + 1;
        aggregate.sum = aggregate.sum + (Double) value.getValue();
        aggregate.avg = aggregate.sum / aggregate.count;
        return aggregate;
    }

    public static void main(String[] args) throws Exception {
        List<String> aQMetrics = null;
        String endpointHost = null;
        String readingsTopic = null;
        int endpointPort = 0;
        List<Integer> geohashPrecisionList = new ArrayList<>();
        boolean cleanup = false;

        // create the command line parser
        CommandLineParser parser = new DefaultParser();

        // create the Options
        Options options = new Options();
        options.addOption( "m", "metric-list", true, "Air quality Metrics as registered in Obelisk. Defaults to '" + METRICS + "'");
        options.addOption( "t", "readings-topic", true, "Topic the air quality metric is being registered to in Obelisk. Defaults to '" + READINGS_TOPIC + "'");
        options.addOption( "gh", "geohash-precision", true, "Geohash precision used to perform the continuous aggregation. Defaults to the application " + GEOHASH_PRECISION_LIST);
        options.addOption( "h", "endpoint-host", true, "REST endpoint hostname. Defaults to " + REST_ENDPOINT_HOSTNAME);
        options.addOption( "p", "endpoint-port", true, "REST endpoint port. Defaults to " + REST_ENDPOINT_PORT);
        options.addOption( "cl", "cleanup", false, "Should a cleanup be performed before staring. Defaults to false" );

        try {
            // parse the command line arguments
            CommandLine line = parser.parse( options, args );

            if( line.hasOption( "metric-list" ) ) {
                aQMetrics = Stream.of(line.getOptionValue("metric-list").split(",")).collect(Collectors.toList());
            } else {
                aQMetrics = METRICS;
            }
            if( line.hasOption( "readings-topic" ) ) {
                readingsTopic = line.getOptionValue("readings-topic");
            } else {
                readingsTopic = READINGS_TOPIC;
            }
            if( line.hasOption( "geohash-precision" ) ) {
                geohashPrecisionList = Stream.of(line.getOptionValue("geohash-precision").split(",")).map(gh -> Integer.parseInt(gh)).collect(Collectors.toList());
            } else {
                geohashPrecisionList = GEOHASH_PRECISION_LIST;
            }
            if( line.hasOption( "endpoint-host" ) ) {
                endpointHost = line.getOptionValue("endpoint-host");
            } else {
                endpointHost = REST_ENDPOINT_HOSTNAME;
            }
            if( line.hasOption( "endpoint-host" ) ) {
                endpointPort = Integer.parseInt(line.getOptionValue("endpoint-port"));
            } else {
                endpointPort = REST_ENDPOINT_PORT;
            }
            if( line.hasOption( "cleanup" ) ) {
                cleanup = true;
            }
        }
        catch( Exception exp ) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("CotIngestStream", exp.getMessage(), options,null, true);
        }

        final HostInfo restEndpoint = new HostInfo(endpointHost, endpointPort);

        System.out.println("Connecting to Kafka cluster via bootstrap servers " + KBROKERS);
        System.out.println("REST endpoint at http://" + endpointHost + ":" + endpointPort);

        final KafkaStreams streams = new KafkaStreams(buildTopology(aQMetrics, readingsTopic, geohashPrecisionList), streamsConfig("/tmp/cot-airquality"));


        if(cleanup) {
            streams.cleanUp();
        }
        streams.start();
        // Start the Restful proxy for servicing remote access to state stores
        final QueryingService queryingService = startRestProxy(streams, restEndpoint);

        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                try {
                    streams.close();
                    queryingService.stop();
                } catch (Throwable e) {
                    System.exit(1);
                }
                latch.countDown();
            }
        });

        latch.await();
        System.exit(0);
    }

    private static String toFormattedTimestamp(Long timestamp, ZoneId zoneId) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), zoneId).toLocalDateTime().format(DATE_TIME_FORMATTER);
    }

    private static QueryingService startRestProxy(final KafkaStreams streams, final HostInfo hostInfo)
            throws Exception {
        final QueryingService
                interactiveQueriesRestService = new QueryingService(streams, hostInfo);
        interactiveQueriesRestService.start();
        return interactiveQueriesRestService;
    }

    private static Properties streamsConfig(final String stateDir) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KBROKERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, REST_ENDPOINT_HOSTNAME + ":" + REST_ENDPOINT_PORT);
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TSExtractor.class);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024L);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10000);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    private static Topology buildTopology(List<String> aQMetrics, String readingsTopic, List<Integer> geohashPrecisionList) {
        final StreamsBuilder builder = new StreamsBuilder();

        // Set up Serializers and Deserializers

        Map<String, Object> serdeProps = new HashMap<>();
        final Serializer<AirQualityReading> aQSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", AirQualityReading.class);
        aQSerializer.configure(serdeProps, false);

        final Deserializer<AirQualityReading> aQDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", AirQualityReading.class);
        aQDeserializer.configure(serdeProps, false);

        final Serde<AirQualityReading> aQSerde = Serdes.serdeFrom(aQSerializer, aQDeserializer);

        serdeProps = new HashMap<>();
        final Serializer<AirQualityKeyedReading> aQKSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", AirQualityKeyedReading.class);
        aQKSerializer.configure(serdeProps, false);

        final Deserializer<AirQualityKeyedReading> aQKDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", AirQualityKeyedReading.class);
        aQKDeserializer.configure(serdeProps, false);

        final Serde<AirQualityKeyedReading> aQKSerde = Serdes.serdeFrom(aQKSerializer, aQKDeserializer);

        serdeProps = new HashMap<>();
        final Serializer<AggregateValueTuple> aggSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", AggregateValueTuple.class);
        aggSerializer.configure(serdeProps, false);

        final Deserializer<AggregateValueTuple> aggDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", AggregateValueTuple.class);
        aggDeserializer.configure(serdeProps, false);

        final Serde<AggregateValueTuple> aggSerde = Serdes.serdeFrom(aggSerializer, aggDeserializer);

        // Set streaming topology and transformations

        KStream<byte[], AirQualityReading> source = builder.stream(readingsTopic, Consumed.with(Serdes.ByteArray(), aQSerde));
        //final String finalMetricId = aQMetricId;
        KStream<String, AirQualityReading> filteredStream = source.selectKey(
                (key, reading) -> reading.getMetricId()
        ).filter(
                (metricId, reading) -> aQMetrics.contains(metricId)
        );
        //.to("cot."+METRIC_ID.replace("::","."), Produced.with(Serdes.String(), aQSerde));
        //filteredStream.to("cot.airquality-metric-key", Produced.with(Serdes.String(), aQSerde));
        //filteredStream.peek((key, reading) -> System.out.println(key + ": " + reading));
        //.print(Printed.toSysOut());

        //final int finalGeohashPrecision = geohashPrecision;

//        KStream<String, AirQualityKeyedReading> airQualityKeyedStream = filteredStream.map(
//                (metricId, reading) -> KeyValue.pair(reading.getGeohash() + "#" + toFormattedTimestamp(reading.getTimestamp(), ZoneId.systemDefault()), new AirQualityKeyedReading(
//                        reading.getTsReceivedMs(),
//                        reading.getMetricId(),
//                        reading.getTimestamp(),
//                        reading.getSourceId(),
//                        reading.getGeohash(),
//                        reading.getH3Index(),
//                        reading.getElevation(),
//                        reading.getValue(),
//                        reading.getTimeUnit(),
//                        reading.getGeohash() + "#" + toFormattedTimestamp(reading.getTimestamp(), ZoneId.systemDefault())
//                ))
//        );

        assert aQMetrics != null;

        aQMetrics.forEach(aQMetricId -> {
            geohashPrecisionList.forEach(gh -> {
                KGroupedStream<String, AirQualityReading> perMinKeyedStream = filteredStream
                        .filter(
                                (metricId, reading) -> metricId.equals(aQMetricId)
                        )
                        .selectKey(
                        (metricId, reading) -> {
                            ZonedDateTime readingDate = ZonedDateTime.ofInstant(Instant.ofEpochMilli(reading.getTimestamp()), ZoneId.systemDefault());
                            String minTimestamp = readingDate.truncatedTo(ChronoUnit.MINUTES).toLocalDateTime().format(DATE_TIME_FORMATTER);
                            return reading.getGeohash().substring(0, gh) + "#" + minTimestamp;
                        }
                ).groupByKey();

                KGroupedStream<String, AirQualityReading> perHourKeyedStream = filteredStream
                        .filter(
                                (metricId, reading) -> metricId.equals(aQMetricId)
                        )
                        .selectKey(
                        (metricId, reading) -> {
                            ZonedDateTime readingDate = ZonedDateTime.ofInstant(Instant.ofEpochMilli(reading.getTimestamp()), ZoneId.systemDefault());
                            String hourTimestamp = readingDate.truncatedTo(ChronoUnit.HOURS).toLocalDateTime().format(DATE_TIME_FORMATTER);
                            return reading.getGeohash().substring(0, gh) + "#" + hourTimestamp;
                        }
                ).groupByKey();

                KGroupedStream<String, AirQualityReading> perDayKeyedStream = filteredStream
                        .filter(
                                (metricId, reading) -> metricId.equals(aQMetricId)
                        )
                        .selectKey(
                        (metricId, reading) -> {
                            ZonedDateTime readingDate = ZonedDateTime.ofInstant(Instant.ofEpochMilli(reading.getTimestamp()), ZoneId.systemDefault());
                            String dayTimestamp = readingDate.truncatedTo(ChronoUnit.DAYS).toLocalDateTime().format(DATE_TIME_FORMATTER);
                            return reading.getGeohash().substring(0, gh) + "#" + dayTimestamp;
                        }
                ).groupByKey();

                KGroupedStream<String, AirQualityReading> perMonthKeyedStream = filteredStream
                        .filter(
                                (metricId, reading) -> metricId.equals(aQMetricId)
                        )
                        .selectKey(
                        (metricId, reading) -> {
                            ZonedDateTime readingDate = ZonedDateTime.ofInstant(Instant.ofEpochMilli(reading.getTimestamp()), ZoneId.systemDefault());
                            String monthTimestamp = readingDate.truncatedTo(ChronoUnit.DAYS).withDayOfMonth(1).toLocalDateTime().format(DATE_TIME_FORMATTER);
                            return reading.getGeohash().substring(0, gh) + "#" + monthTimestamp;
                        }
                ).groupByKey();

//                KGroupedStream<String, AirQualityReading> perYearKeyedStream = filteredStream.selectKey(
//                        (metricId, reading) -> {
//                            ZonedDateTime readingDate = ZonedDateTime.ofInstant(Instant.ofEpochMilli(reading.getTimestamp()), ZoneId.systemDefault());
//                            String yearTimestamp = readingDate.truncatedTo(ChronoUnit.DAYS).withDayOfYear(1).toLocalDateTime().format(DATE_TIME_FORMATTER);
//                            return reading.getGeohash().substring(0, gh) + "#" + yearTimestamp;
//                        }
//                ).groupByKey();

                //perMinKeyedStream.peek((key, reading) -> System.out.println(key + ": " + reading));
                //perHourKeyedStream.peek((key, reading) -> System.out.println(key + ": " + reading));
                //perDayKeyedStream.peek((key, reading) -> System.out.println(key + ": " + reading));
                //perMonthKeyedStream.peek((key, reading) -> System.out.println(key + ": " + reading));
                //perYearKeyedStream.peek((key, reading) -> System.out.println(key + ": " + reading));

                // Generate KTables with continuous aggregates for each time resolution
        /*KTable<String, AirQualityReading> airQualityKTable = airQualityKeyedStream.groupByKey().reduce(
                (aggReading, newReading) -> newReading,
                Materialized.<String, AirQualityReading, KeyValueStore<Bytes, byte[]>>as("raw-" + finalMetricId.replace("::", ".")).withValueSerde(aQSerde)
        );*/

                KTable<String, AggregateValueTuple> perMinAggregate = perMinKeyedStream.aggregate(
                        () -> new AggregateValueTuple("", "", 0L, 0L, 0.0, 0.0),
                        (key, value, aggregate) -> airQReadingAggregator(key, value, aggregate),
                        Materialized.<String, AggregateValueTuple, KeyValueStore<Bytes, byte[]>>as("view-" + aQMetricId.replace("::", ".") + "-gh" + gh + "-min").withValueSerde(aggSerde).withCachingEnabled()
                );

                KTable<String, AggregateValueTuple> perHourAggregate = perHourKeyedStream.aggregate(
                        () -> new AggregateValueTuple("", "", 0L, 0L, 0.0, 0.0),
                        (key, value, aggregate) -> airQReadingAggregator(key, value, aggregate),
                        Materialized.<String, AggregateValueTuple, KeyValueStore<Bytes, byte[]>>as("view-" + aQMetricId.replace("::", ".") + "-gh" + gh + "-hour").withValueSerde(aggSerde).withCachingEnabled()
                );

                KTable<String, AggregateValueTuple> perDayAggregate = perDayKeyedStream.aggregate(
                        () -> new AggregateValueTuple("", "", 0L, 0L, 0.0, 0.0),
                        (key, value, aggregate) -> airQReadingAggregator(key, value, aggregate),
                        Materialized.<String, AggregateValueTuple, KeyValueStore<Bytes, byte[]>>as("view-" + aQMetricId.replace("::", ".") + "-gh" + gh + "-day").withValueSerde(aggSerde).withCachingEnabled()
                );

                KTable<String, AggregateValueTuple> perMonthAggregate = perMonthKeyedStream.aggregate(
                        () -> new AggregateValueTuple("", "", 0L, 0L, 0.0, 0.0),
                        (key, value, aggregate) -> airQReadingAggregator(key, value, aggregate),
                        Materialized.<String, AggregateValueTuple, KeyValueStore<Bytes, byte[]>>as("view-" + aQMetricId.replace("::", ".") + "-gh" + gh + "-month").withValueSerde(aggSerde).withCachingEnabled()
                );

//            KTable<String, AggregateValueTuple> perYearAggregate = perYearKeyedStream.aggregate(
//                    () -> new AggregateValueTuple("", "", 0L, 0L, 0.0, 0.0),
//                    (key, value, aggregate) -> airQReadingAggregator(key, value, aggregate),
//                    Materialized.<String, AggregateValueTuple, KeyValueStore<Bytes, byte[]>>as("view-" + aQMetricId.replace("::", ".") + "-gh" + gh + "-year").withValueSerde(aggSerde).withCachingEnabled()
//            );

                // Get streams from KTables to peek into them (to check if they are working as expected)

            /*airQualityKTable.toStream().peek((key, reading) -> System.out.println("[RAW] --" + key + ": " + reading));
            perMinAggregate.toStream().peek((key, aggregate) -> System.out.println("[MIN] --" + key + ": " + aggregate));
            perHourAggregate.toStream().peek((key, aggregate) -> System.out.println("[HOUR] --" + key + ": " + aggregate));
            perDayAggregate.toStream().peek((key, aggregate) -> System.out.println("[DAY] --" + key + ": " + aggregate));
            perMonthAggregate.toStream().peek((key, aggregate) -> System.out.println("[MONTH] --" + key + ": " + aggregate));
            perYearAggregate.toStream().peek((key, aggregate) -> System.out.println("[YEAR] --" + key + ": " + aggregate));*/

                // Store KTables as kafka topics (changelog stream)

//            airQualityKeyedStream.to("raw-" + aQMetricId.replace("::", "."), Produced.with(Serdes.String(), aQKSerde));
//            perMinAggregate.toStream().to("view-" + aQMetricId.replace("::", ".") + "-gh" + gh + "-min", Produced.with(Serdes.String(), aggSerde));
//            perHourAggregate.toStream().to("view-" + aQMetricId.replace("::", ".") + "-gh" + gh + "-hour", Produced.with(Serdes.String(), aggSerde));
//            perDayAggregate.toStream().to("view-" + aQMetricId.replace("::", ".") + "-gh" + gh + "-day", Produced.with(Serdes.String(), aggSerde));
//            perMonthAggregate.toStream().to("view-" + aQMetricId.replace("::", ".") + "-gh" + gh + "-month", Produced.with(Serdes.String(), aggSerde));
//            perYearAggregate.toStream().to("view-" + aQMetricId.replace("::", ".") + "-gh" + gh + "-year", Produced.with(Serdes.String(), aggSerde));
            });
        });

        Topology topology = builder.build();
        System.out.println(topology.describe());
        return topology;
    }
}