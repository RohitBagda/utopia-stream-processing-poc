package org.improving.workshop.phase3

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.apache.kafka.streams.TopologyTestDriver
import org.improving.workshop.Streams
import org.junit.experimental.theories.internal.SpecificDataPointsSupplier
import org.msse.demo.mockdata.customer.address.Address
import org.msse.demo.mockdata.customer.profile.Customer
import org.msse.demo.mockdata.music.artist.Artist
import org.msse.demo.mockdata.music.stream.Stream
import spock.lang.Specification

import static org.improving.workshop.phase3.TrendingArtistGeography.*
import static org.improving.workshop.utils.DataFaker.ARTISTS
import static org.improving.workshop.utils.DataFaker.CUSTOMERS
import static org.improving.workshop.utils.DataFaker.STREAMS
import static org.improving.workshop.utils.DataFaker.ADDRESSES

class TrendingArtistsGeographySpec extends Specification {
    TopologyTestDriver driver

    // inputs
    TestInputTopic<String, Stream> streamsTopic
    TestInputTopic<String, Customer> customerTopic
    TestInputTopic<String, Artist> artistTopic
    TestInputTopic<String, Address> addressTopic

    // outputs
    TestOutputTopic<String, TrendingArtistGeography.StateAndCustomerStreamCountMap> outputTopic

    def 'setup'() {
        // instantiate new builder
        StreamsBuilder streamsBuilder = new StreamsBuilder()

        // build the CustomerStreamCount topology (by reference)
        configureTopology(streamsBuilder)

        // build the TopologyTestDriver
        driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties())

        streamsTopic = driver.createInputTopic(Streams.TOPIC_DATA_DEMO_STREAMS, Serdes.String().serializer(), Streams.SERDE_STREAM_JSON.serializer())
        customerTopic = driver.createInputTopic(Streams.TOPIC_DATA_DEMO_CUSTOMERS, Serdes.String().serializer(), Streams.SERDE_CUSTOMER_JSON.serializer())
        artistTopic = driver.createInputTopic(Streams.TOPIC_DATA_DEMO_ARTISTS, Serdes.String().serializer(), Streams.SERDE_ARTIST_JSON.serializer())
        addressTopic = driver.createInputTopic(Streams.TOPIC_DATA_DEMO_ADDRESSES, Serdes.String().serializer(), Streams.SERDE_ADDRESS_JSON.serializer())
        outputTopic = driver.createOutputTopic(OUTPUT_TOPIC, Serdes.String().deserializer(), TRENDING_ARTIST_AGGREGATES_JSON_SERDE.deserializer())
    }

    def 'cleanup'() {
        // if this close doesn't run (test fails halfway through), subsequent tests may start on old state
        // run the test and let it cleanup, then run the test again.
        driver.close()
    }

    def "trending artists geography"() {
        given: 'multiple customer streams (each with an address) for multiple artists received by the topology'

        /*
        * Customers: C1, C2, C3, C4, C5, C6, C7, C8, C9, C10
        * Address:
        *       MN: [C1, C2, C3]
        *       NY: [C4, C5]
        *       CA: [C6, C7]
        *       NV: [C8]
        *       FL: [C9]
        *       GA: [C10]
        *
        *
        * Artists: A1 (assigned to Stream 1), A2 (assigned to Stream 2), A3 (assigned to Stream 3),
        *           A4 (assigned to Stream 4), A5 (assigned to Stream 5), A6 (assigned to Stream 6)
        *
        * Artist statistics:
        *       A1 is streamed in 6 states - MN, NY, CA, NV, FL, GA
        *       A2 is streamed in 5 states - MN, NY, CA, NV, FL
        *       A3 is streamed in 3 states - MN, NY, CA
        *       A4 is streamed in 3 states - MN, NY, CA
        *       A5 is streamed in 3 states - MN, NV, CA
        *       A6 is streamed in 1 state - MN
        *
        * Streams:
        *       C1 - A1, A2, A4, A5
        *       C2 - A2, A3, A6
        *       C3 - A2             -> 6 total unique streams in Minnesota
        *
        *       C4 - A1, A3
        *       C5 - A2, A4, A5     -> 5 total unique streams in New York
        *
        *       C6 - A1, A2
        *       C7 - A3, A4, A1     -> 4 total unique streams in California
        *
        *       C8 - A1, A2, A5     -> 3 total unique streams in Nevada
        *
        *       C9 - A1, A2         -> 2 total unique streams in Florida
        *
        *       C10 - A1            -> 1 total unique stream in Georgia
        *
        * Top 2 streamed artists are A1 with 6 streams and A2 with 5 streams.
        *
        * Top 5 states are: MN, NY, CA, NV and FL
        *
        * Top customer from each of the top 5 states:
        *       Minnesota - C1 with 4 streams
        *       New York - C5 with 3 streams
        *       California - C7 with 3 streams
        *       Nevada - C8 with 3 streams
        *       Florida - C9 with 2 streams
        */

        def customers = new ArrayList<Customer>()
        for (int i = 0; i < 10; i++) {
            def customerId = "customer-" + (i+1)
            def customer = CUSTOMERS.generate(customerId)
            customers.add(customer)
            customerTopic.pipeInput(customerId, customer)
        }

        // Define 10 states
        // 3 customers from MN; 2 customers from NY; 2 customers from CA; and 1 customer each from NV, FL, GA
        def states = ["MN", "MN", "MN", "NY", "NY", "CA", "CA", "NV", "FL", "GA"]

        // Create addresses for each customer
        def addresses = new ArrayList<Address>()
        for (int i = 0; i < 10; i++){
            def addressID = "address-" + (i+1)
            //def address = ADDRESSES.generateCustomerAddress(addressID, customers.get(i).id())
            def address = new Address(addressID, customers.get(i).id(), "00", "Permanent", "1016", "SE",
                                    "NA", states.get(i), "00000", "00000", "USA", 1.00, 1.00)
            addresses.add(address)
            addressTopic.pipeInput(addressID, address)
        }

        // Create 6 artists (artist-1, artist-2, artist-3, artist-4, artist-5 and artist-6)
        def artists = new ArrayList<Artist>()
        for (int i = 0; i < 6; i++) {
            def artistId = "artist-" + (i+1)
            def artist = ARTISTS.generate(artistId)
            artists.add(artist)
            artistTopic.pipeInput(artistId, artist)
        }

        // Create all streams
        def streams = []

        // artist-1 is listened to by customers 1, 4, 6, 7, 8, 9 and 10, so create streams for them
        def artist_one_customers = ["customer-1", "customer-4", "customer-6", "customer-7", "customer-8", "customer-9", "customer-10"]
        for (int i = 0; i < artist_one_customers.size(); i++){
            def customerId = artist_one_customers.get(i)
            def stream = STREAMS.generate(customerId, "artist-1")
            streams.add(stream)
            streamsTopic.pipeInput(UUID.randomUUID().toString(), stream)
        }

        // artist-2 is listened to by customers 1, 2, 3, 5, 6, 8 and 9
        def artist_two_customers = ["customer-1", "customer-2", "customer-3", "customer-5", "customer-6", "customer-8", "customer-9"]
        for (int i = 0; i < artist_two_customers.size(); i++){
            def customerId = artist_two_customers.get(i)
            def stream = STREAMS.generate(customerId, "artist-2")
            streams.add(stream)
            streamsTopic.pipeInput(UUID.randomUUID().toString(), stream)
        }

        // artist-3 is listened to by customers 2, 4, 7
        def artist_three_customers = ["customer-2", "customer-4", "customer-7"]
        for (int i = 0; i < artist_three_customers.size(); i++){
            def customerId = artist_three_customers.get(i)
            def stream = STREAMS.generate(customerId, "artist-3")
            streams.add(stream)
            streamsTopic.pipeInput(UUID.randomUUID().toString(), stream)
        }

        // artist-4 is listened to by customers 1, 5, 7
        def artist_four_customers = ["customer-1", "customer-5", "customer-7"]
        for (int i = 0; i < artist_four_customers.size(); i++){
            def customerId = artist_four_customers.get(i)
            def stream = STREAMS.generate(customerId, "artist-4")
            streams.add(stream)
            streamsTopic.pipeInput(UUID.randomUUID().toString(), stream)
        }

        // artist-5 is listened to by customers 1, 5, 8
        def artist_five_customers = ["customer-1", "customer-5", "customer-8"]
        for (int i = 0; i < artist_five_customers.size(); i++){
            def customerId = artist_five_customers.get(i)
            def stream = STREAMS.generate(customerId, "artist-5")
            streams.add(stream)
            streamsTopic.pipeInput(UUID.randomUUID().toString(), stream)
        }

        // artist-6 is listened to by customer 2
        def artist_six_customers = ["customer-2"]
        def stream = STREAMS.generate("customer-2", "artist-6")
        streams.add(stream)
        streamsTopic.pipeInput(UUID.randomUUID().toString(), stream)


        when: 'reading the output records'
        def outputRecords = outputTopic.readValue()


        then: 'the expected number of records were received'
        //outputRecords.size() == streamCount
        def result = outputTopic.readValue()
        assert result.trendingArtistAggregates.size() == 4
    }
}
