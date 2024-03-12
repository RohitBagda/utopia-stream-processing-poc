package org.improving.workshop.phase3;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.msse.demo.mockdata.customer.address.Address;
import org.msse.demo.mockdata.customer.profile.Customer;
import org.msse.demo.mockdata.music.artist.Artist;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;
import java.util.*;

import static java.util.Collections.reverseOrder;
import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.apache.kafka.streams.state.Stores.persistentWindowStore;
import static org.improving.workshop.Streams.*;

@Slf4j
public class TrendingArtistGeography {

    public static final String OUTPUT_TOPIC = "kafka-trending-artist-geography";
    public static final JsonSerde<StreamWithAddress> STREAM_WITH_ADDRESS_JSON_SERDE = new JsonSerde<>(StreamWithAddress.class);
    public static final JsonSerde<StreamWithCustomerData> STREAM_WITH_CUSTOMER_DATA_JSON_SERDE = new JsonSerde<>(StreamWithCustomerData.class);
    public static final JsonSerde<StreamWithCustomerAndArtist> STREAM_WITH_CUSTOMER_AND_ARTIST_JSON_SERDE = new JsonSerde<>(StreamWithCustomerAndArtist.class);
    public static final JsonSerde<SortedCounterMap> SORTED_COUNTER_MAP_JSON_SERDE = new JsonSerde<>(SortedCounterMap.class);
    public static final JsonSerde<TrendingArtistAggregates> TRENDING_ARTIST_AGGREGATES_JSON_SERDE = new JsonSerde<>(TrendingArtistAggregates.class);

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
                        TOPIC_DATA_DEMO_ARTISTS,
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
                .windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(2)))
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
                .<String, SortedCounterMap>as(persistentWindowStore("trending-artists-aggregate-table", Duration.ofMinutes(5), Duration.ofMinutes(2), false))
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

        public CustomerStreamCountMap() {
            map = new HashMap<>();
        }
    }


    @Data
    @AllArgsConstructor
    public static class StateAndCustomerStreamCountMap {

        // Key: State, Value: Customer and Stream Counts.
        private Map<String , CustomerStreamCountMap> map;

        public StateAndCustomerStreamCountMap() {
            map = new HashMap<>();
        }
    }

    @Data
    @AllArgsConstructor
    public static class TrendingArtistAggregate {
        private long uniqueCustomerCount;
        private StateAndCustomerStreamCountMap customerStreamCountMap;

        public TrendingArtistAggregate() {
            uniqueCustomerCount = 0L;
            customerStreamCountMap = new StateAndCustomerStreamCountMap();
        }
    }

    @Data
    @AllArgsConstructor
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
            TrendingArtistAggregate trendingArtistAggregate = trendingArtistAggregatePerArtist.computeIfAbsent(
                streamWithCustomerAndArtist.artist.id(),
                value -> new TrendingArtistAggregate()
            );
            StateAndCustomerStreamCountMap stateAndCustomerStreamCountMap = trendingArtistAggregate.customerStreamCountMap;
            CustomerStreamCountMap customerStreamCountMap = stateAndCustomerStreamCountMap.map.get(streamWithCustomerAndArtist.customer.id());
            if (customerStreamCountMap != null) {
                CustomerStreamCount customerStreamCount = customerStreamCountMap.map.computeIfAbsent(
                        streamWithCustomerAndArtist.customer.id(),
                        value -> new CustomerStreamCount()
                );
                customerStreamCount.streamCounts++;
            } else {
                stateAndCustomerStreamCountMap.map.put(streamWithCustomerAndArtist.customer.id(), new CustomerStreamCountMap());
            }

            trendingArtistAggregate.uniqueCustomerCount = trendingArtistAggregate.customerStreamCountMap.map.keySet().size();

            // Sort the map.
            this.trendingArtistAggregatePerArtist = trendingArtistAggregatePerArtist.entrySet().stream()
                    .sorted(reverseOrder(Map.Entry.comparingByValue(Comparator.comparing(TrendingArtistAggregate::getUniqueCustomerCount))))
                    .limit(maxSize)
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        }

        public TrendingArtistAggregates top(int limit) {
            return new TrendingArtistAggregates(this.trendingArtistAggregatePerArtist.entrySet().stream()
                    .limit(limit)
                    .map(Map.Entry::getValue)
                    .toList()
            );
        }
    }
}