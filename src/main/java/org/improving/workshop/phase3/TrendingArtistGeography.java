package org.improving.workshop.phase3;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.msse.demo.mockdata.customer.address.Address;
import org.msse.demo.mockdata.customer.profile.Customer;
import org.msse.demo.mockdata.music.artist.Artist;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Collections.reverseOrder;
import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.apache.kafka.streams.state.Stores.persistentWindowStore;
import static org.improving.workshop.Streams.*;

/**
 * Find the 3 most trending streamed artists currently and then identify their top 5 states with the highest number of
 * unique customers and also their top customer with most stream counts from each of those 5 states.
 * Trending here means an artist has the highest unique customers in the US in the past 10 minutes.
 * If there is tie in the top 5 states it picks the state with the highest number of streams for that artist in
 * that window.
 * Example:
 *
 * Taylor Swift
 *  MN - Moti
 *  NY - Marques
 *  NV - John
 *  ND - Rachel
 *  WI - Mark
 *
 * Bruno Mars
 *  CA - Bob
 *  .
 *  .
 *  .
 *  .
 *
 * Bleachers
 *  MN - Rohit
 *  .
 *  .
 *  .
 *  .
 *  .
 */
@SuppressWarnings("JavadocBlankLines")
@Slf4j
public class TrendingArtistGeography {

    public static final String OUTPUT_TOPIC = "kafka-trending-artist-geography";
    public static final JsonSerde<StreamWithAddress> STREAM_WITH_ADDRESS_JSON_SERDE =
            new JsonSerde<>(StreamWithAddress.class);
    public static final JsonSerde<StreamWithCustomerData> STREAM_WITH_CUSTOMER_DATA_JSON_SERDE =
            new JsonSerde<>(StreamWithCustomerData.class);
    public static final JsonSerde<StreamWithCustomerAndArtist> STREAM_WITH_CUSTOMER_AND_ARTIST_JSON_SERDE =
            new JsonSerde<>(StreamWithCustomerAndArtist.class);
    public static final JsonSerde<SortedCounterMap> SORTED_COUNTER_MAP_JSON_SERDE =
            new JsonSerde<>(SortedCounterMap.class);
    public static final JsonSerde<TrendingArtistAggregates> TRENDING_ARTIST_AGGREGATES_JSON_SERDE =
            new JsonSerde<>(TrendingArtistAggregates.class);

    public static void main(final String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();

        // configure the processing topology
        configureTopology(builder);

        // fire up the engines
        startStreams(builder);
    }

    public static void configureTopology(final StreamsBuilder builder) {
        KTable<String, Address> addressTable = builder
                .stream(TOPIC_DATA_DEMO_ADDRESSES, Consumed.with(Serdes.String(), SERDE_ADDRESS_JSON))
                .filter((k, v) -> v.customerid() != null)
                .selectKey((k, v) -> v.customerid())
                .toTable(
                        Materialized
                                .<String, Address>as(persistentKeyValueStore("address"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SERDE_ADDRESS_JSON)
                );

        // store artist in a table so that the stream can reference them to find artist
        KTable<String, Artist> artistTable = builder
                .table(
                        TOPIC_DATA_DEMO_ARTISTS,
                        Materialized
                                .<String, Artist>as(persistentKeyValueStore("artist"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SERDE_ARTIST_JSON)
                );

        // store artist in a table so that the stream can reference them to find artist
        KTable<String, Customer> customerTable = builder
                .table(
                        TOPIC_DATA_DEMO_CUSTOMERS,
                        Materialized
                                .<String, Customer>as(persistentKeyValueStore("customer"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SERDE_CUSTOMER_JSON)
                );

        builder.stream(TOPIC_DATA_DEMO_STREAMS, Consumed.with(Serdes.String(), SERDE_STREAM_JSON))
                .peek((streamId, stream) -> log.info("Stream Information: {}", stream))
                .selectKey((streamId, stream) -> stream.customerid())
                .join(
                        addressTable,
                        (customerId, stream, address) -> new StreamWithAddress(customerId, stream.artistid(), address),
                        Joined.with(Serdes.String(), SERDE_STREAM_JSON, SERDE_ADDRESS_JSON)
                )
                .join(
                        customerTable,
                        (customerId, streamWithAddress, customer) -> new StreamWithCustomerData(customer, streamWithAddress.artistId, streamWithAddress.address),
                        Joined.with(Serdes.String(), STREAM_WITH_ADDRESS_JSON_SERDE, SERDE_CUSTOMER_JSON)
                )
                .selectKey((customerId, streamWithCustomerData) -> streamWithCustomerData.artistId)
                .join(
                        artistTable,
                        (artistId, streamWithCustomerData, artist) -> new StreamWithCustomerAndArtist(streamWithCustomerData.customer, artist, streamWithCustomerData.address),
                        Joined.with(Serdes.String(), STREAM_WITH_CUSTOMER_DATA_JSON_SERDE, SERDE_ARTIST_JSON)
                )
                .selectKey((artistId, streamWithCustomerAndArtist) -> "GLOBAL")
                .groupByKey(Grouped.with(Serdes.String(), STREAM_WITH_CUSTOMER_AND_ARTIST_JSON_SERDE))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(10)))
                .aggregate(SortedCounterMap::new, trendingArtistAggregator(), trendingArtistAggregateMaterializedView())
                .toStream()
                .mapValues((globalKey, sortedCounterMap) -> sortedCounterMap.top(3))
                .selectKey((windowedKey, trendingArtistAggregates) -> windowedKey.key())
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), TRENDING_ARTIST_AGGREGATES_JSON_SERDE));
    }

    private static Aggregator<String, StreamWithCustomerAndArtist, SortedCounterMap> trendingArtistAggregator() {
        return (globalKey, streamWithCustomerAndArtist, sortedCounterMap) -> {
            sortedCounterMap.add(streamWithCustomerAndArtist);
            return sortedCounterMap;
        };
    }

    private static Materialized<String, SortedCounterMap, WindowStore<Bytes, byte[]>> trendingArtistAggregateMaterializedView() {
        // kTable (materialized) configuration
        return Materialized
                .<String, SortedCounterMap>as(persistentWindowStore("trending-artists-aggregate-table", Duration.ofMinutes(15), Duration.ofMinutes(10), false))
                .withKeySerde(Serdes.String())
                .withValueSerde(SORTED_COUNTER_MAP_JSON_SERDE);
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class StreamWithAddress{
        private String customerId;
        private String artistId;
        private Address address;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class StreamWithCustomerData{
        private Customer customer;
        private String artistId;
        private Address address;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class StreamWithCustomerAndArtist {
        private Customer customer;
        private Artist artist;
        private Address address;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CustomerStreamCount {
        private Customer customer;
        private Long streamCounts;
    }

    @Data
    @AllArgsConstructor
    public static class CustomerStreamCountMap {
        private Map<String, CustomerStreamCount> map;

        public CustomerStreamCountMap() { map = new HashMap<>(); }

        public long customerCount() {
            return this.map.keySet().size();
        }

        public long streamCount() {
            return this.map.values().stream()
                    .map(customerStreamCount -> customerStreamCount.streamCounts)
                    .mapToLong(Long::longValue)
                    .sum();
        }
    }


    @Data
    @AllArgsConstructor
    public static class StateAndCustomerStreamCountMap {

        // Key: State, Value: Customer and Stream Counts.
        private Map<String , CustomerStreamCountMap> map;

        public StateAndCustomerStreamCountMap() {
            map = new LinkedHashMap<>();
        }

        public void top(int limit) {
            // Sort by highest number of unique customers
            // If equal then sort by highest number of streams
            this.map = map.entrySet().stream()
                    .sorted(reverseOrder(Comparator.comparingLong((Map.Entry<String, CustomerStreamCountMap> o) ->
                            o.getValue().customerCount()).thenComparingLong(entry -> entry.getValue().streamCount())))
                    .limit(limit)
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class TrendingArtistAggregate {
        private Artist artist;
        private long uniqueCustomerCount;
        private StateAndCustomerStreamCountMap stateAndCustomerStreamCountMap;

        public TrendingArtistAggregate(Artist artist) {
            this.artist = artist;
            uniqueCustomerCount = 0L;
            stateAndCustomerStreamCountMap = new StateAndCustomerStreamCountMap();
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class TrendingArtistAggregates {
        private List<TrendingArtistAggregate> trendingArtistAggregates;
    }


    @Data
    @AllArgsConstructor
    public static class SortedCounterMap {
        private int maxSize;
        // Key: ArtistId, Value: Aggregate Data for that Trending Artist
        private LinkedHashMap<String, TrendingArtistAggregate> trendingArtistAggregatePerArtist;

        public SortedCounterMap(int maxSize) {
            this.maxSize = maxSize;
            this.trendingArtistAggregatePerArtist = new LinkedHashMap<>();
        }

        public SortedCounterMap() { this(1000000); }

        public void add(StreamWithCustomerAndArtist streamWithCustomerAndArtist) {
            // Get TrendingArtistAggregate for artistId if it exists. If not create a new one.
            TrendingArtistAggregate trendingArtistAggregate = trendingArtistAggregatePerArtist.computeIfAbsent(
                streamWithCustomerAndArtist.artist.id(),
                value -> new TrendingArtistAggregate(streamWithCustomerAndArtist.artist)
            );

            // Get the CustomerStreamCountMap for that artist and state if it exists. If not create a new one.
            StateAndCustomerStreamCountMap stateAndCustomerStreamCountMap = trendingArtistAggregate.stateAndCustomerStreamCountMap;
            CustomerStreamCountMap customerStreamCountMap = stateAndCustomerStreamCountMap.map.computeIfAbsent(
                    streamWithCustomerAndArtist.address.state(), value -> new CustomerStreamCountMap()
            );

            // Get the CustomerStreamCount for that artist and state and customer. If not create a new one.
            CustomerStreamCount customerStreamCount = customerStreamCountMap.map.computeIfAbsent(
                    streamWithCustomerAndArtist.customer.id(),
                    value -> new CustomerStreamCount(streamWithCustomerAndArtist.customer, 0L)
            );

            // increment the stream count.
            customerStreamCount.streamCounts++;

            // Calculate and update the unique customer count for each artist.
            long uniqueCount = 0L;
            Map<String, CustomerStreamCountMap> customerStreamCountMapsForAllStates = trendingArtistAggregate.stateAndCustomerStreamCountMap.map;
            for (Map.Entry<String, CustomerStreamCountMap> entry : customerStreamCountMapsForAllStates.entrySet()) {
                uniqueCount += entry.getValue().getMap().keySet().size();
            }
            trendingArtistAggregate.uniqueCustomerCount = uniqueCount;

            // Sort the map.
            this.trendingArtistAggregatePerArtist = trendingArtistAggregatePerArtist.entrySet().stream()
                    .sorted(reverseOrder(Map.Entry.comparingByValue(Comparator.comparing(TrendingArtistAggregate::getUniqueCustomerCount))))
                    .limit(maxSize)
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));


        }

        public TrendingArtistAggregates top(int limit) {
            TrendingArtistAggregates result = new TrendingArtistAggregates(this.trendingArtistAggregatePerArtist.entrySet().stream()
                    .limit(limit)
                    .map(Map.Entry::getValue)
                    .toList()
            );

            result.trendingArtistAggregates.forEach(trendingArtistAggregate -> trendingArtistAggregate.stateAndCustomerStreamCountMap.top(5));

            return result;
        }
    }
}