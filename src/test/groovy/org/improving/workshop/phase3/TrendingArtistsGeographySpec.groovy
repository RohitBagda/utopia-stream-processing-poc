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
    TestOutputTopic<String, TrendingArtistAggregates> outputTopic

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
        * Customers: 10 per state and total of 6 states
        * Customers and Address:
        *   MN: [C1 ... C10]
        *   NY: [C11 ... C20]
        *   CA: [C21 ... C30]
        *   NV: [C31 ... C40]
        *   FL: [C41 ... C50]
        *   GA: [C51 ... C60]
        *
        * Artists: Each Artist gets a thousand streams
        *   A1 (assigned to S0001 ... S1000),
        *   A2 (assigned to S1001 ... S2000),
        *   A3 (assigned to S2001 ... S3000),
        *   A4 (assigned to S3001 ... S4000),
        *
        * Artists and their Streams per state
        * |    | MN  | NY  | CA  | NV  | FL  | GA  | Total |
        * |----|-----|-----|-----|-----|-----|-----|-------|
        * | A1 | 240 | 200 | 180 | 160 | 120 | 100 |  1000 |
        * | A2 | 200 | 150 | 300 | 350 | 100 | 100 |  1000 |
        * | A3 | 150 | 100 | 400 |  50 | 120 | 180 |  1000 |
        * | A4 | 100 | 120 | 140 | 160 | 180 | 300 |  1000 |
        *
        * Artists and their Stream per state Divided by Customer# for that state. So
        *   MN is C1, C2 ... C10.
        *   NY is C11, C12 ... C20.
        *   ...
        *   GA is C51, C52 ... C60
        *
        *
        * |A1/C#|  1  |  2 |  3 |  4 |  5 |  6 |  7 |  8 |  9 |  10 | Total | Number of unique customers |
        * |-----|-----|----|----|----|----|----|----|----|----|-----|-------|----------------------------|
        * | MN  | 100 | 40 | 15 | 15 | 15 | 15 | 15 | 10 | 10 |  5  |  240  |             10             |
        * | NY  |  80 | 40 | 20 | 20 | 10 |  8 |  7 |  5 |  5 |  5  |  200  |             10             |
        * | CA  | 100 | 30 | 10 | 10 |  5 |  5 |  5 |  5 |  5 |  5  |  180  |             10             |
        * | NV  |  75 | 15 | 20 |  5 | 10 |  5 | 10 |  5 | 10 |  5  |  160  |             10             |
        * | FL  |  45 | 15 |  5 | 10 |  5 | 10 |  5 | 10 | 10 |  5  |  120  |             10             |
        * | GA  |  20 | 40 | 10 |  5 |  5 |  5 |  5 |  5 |  5 |  0  |  100  |              9             |
        * |     |     |    |    |    |    |    |    |    |    |     | 1000  |             59             |
        *
        * |A2/C#|  1  |  2 |  3 |  4 |  5 |  6 |  7 |  8 |  9 |  10 | Total | Number of unique customers |
        * |-----|-----|----|----|----|----|----|----|----|----|-----|-------|----------------------------|
        * | MN  |  80 | 30 | 20 | 10 | 20 | 15 | 10 |  5 |  5 |  5  |  200  |             10             |
        * | NY  |  40 | 15 | 10 | 10 | 10 | 10 | 10 |  5 |  5 |  5  |  120  |             10             |
        * | CA  | 150 | 30 | 20 | 10 |  5 |  5 |  5 |  5 |  0 |  0  |  230  |              8             |
        * | NV  | 125 | 35 | 30 | 10 | 20 | 15 | 20 | 15 | 20 | 10  |  300  |             10             |
        * | FL  |  20 | 10 |  5 |  5 |  5 |  5 |  0 |  0 |  0 |  0  |   50  |              6             |
        * | GA  |  50 | 20 | 10 | 10 | 10 |  0 |  0 |  0 |  0 |  0  |  100  |              5             |
        * |     |     |    |    |    |    |    |    |    |    |     | 1000  |             49             |
        *
        * |A3/C#|  1  |  2 |  3 |  4 |  5 |  6 |  7 |  8 |  9 |  10 | Total | Number of unique customers |
        * |-----|-----|----|----|----|----|----|----|----|----|-----|-------|----------------------------|
        * | MN  |  60 | 25 | 25 | 25 |  5 |  5 |  5 |  0 |  0 |   0 |  150  |              7             |
        * | NY  |  30 | 10 | 20 | 10 |  5 |  5 |  5 |  5 |  5 |   5 |  100  |             10             |
        * | CA  | 160 | 40 | 80 | 40 | 40 | 20 | 10 |  5 |  5 |   0 |  400  |              9             |
        * | NV  |  10 |  5 |  5 |  5 |  5 |  5 |  5 |  5 |  5 |   0 |   50  |              9             |
        * | FL  |  60 | 15 |  5 | 10 |  5 | 10 |  5 |  5 |  5 |   0 |  120  |              9             |
        * | GA  | 100 | 25 | 25 |  5 | 10 |  5 | 10 |  0 |  0 |   0 |  180  |              7             |
        * |     |     |    |    |    |    |    |    |    |    |     | 1000  |             51             |
        *
        * |A4/C#|  1  |  2 |  3 |  4 |  5 |  6 |  7 |  8 |  9 |  10 | Total | Number of unique customers |
        * |-----|-----|----|----|----|----|----|----|----|----|-----|-------|----------------------------|
        * | MN  |  40 | 15 | 10 | 10 | 10 |  5 |  5 |  0 |  0 |  5  |  100  |              8             |
        * | NY  |  50 | 20 | 10 | 10 | 10 |  5 |  5 |  5 |  5 |  0  |  120  |              9             |
        * | CA  |  30 | 20 | 20 | 10 | 10 | 10 | 10 | 10 | 10 | 10  |  140  |             10             |
        * | NV  |  60 | 20 | 10 | 10 | 10 | 10 | 10 | 10 | 10 | 10  |  160  |             10             |
        * | FL  |  80 | 20 | 20 | 10 | 10 | 10 | 10 | 10 | 10 |  0  |  180  |              9             |
        * | GA  | 180 | 30 | 30 | 15 | 15 | 15 | 15 |  0 |  0 |  0  |  300  |              7             |
        * |     |     |    |    |    |    |    |    |    |    |     | 1000  |             53             |
        *
        * Expected Results:
        *
        *  A1
        *   - 59 unique customers
        *   - State - Top Customer in state - Customer Stream Count
        *   - MN - C1 - 100
        *   - CA - C21 - 100
        *   - NY - C11 - 80
        *   - NV - C31 - 75
        *   - FL - C41 - 45
        *
        * A4
        *   - 53 unique customers
        *   - State - Top Customer in state - Customer Stream Count
        *   - NV - C31 - 60
        *   - CA - C21 - 30
        *   - FL - C41 - 80
        *   - NY - C11 - 50
        *   - MN - C1 - 40
        *
        * A3
        *   - 51 unique customers
        *   - NY - C11 - 30
        *   - CA - C21 - 400
        *   - FL - C41 - 120
        *   - NV - C31 - 50
        *   - GA - C51 - 300
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
        for (int i = 0; i < 10; i++) {
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
        def stream = STREAMS.generate("customer-2", "artist-6")
        streams.add(stream)
        streamsTopic.pipeInput(UUID.randomUUID().toString(), stream)

        when: 'reading the output records'
        def outputRecords = outputTopic.readValuesToList().last()

        then: "asfnksejanf"
    }
}
