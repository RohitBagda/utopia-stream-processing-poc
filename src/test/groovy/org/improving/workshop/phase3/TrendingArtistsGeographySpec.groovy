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
        *   - NY - C11 - 80
        *   - CA - C21 - 100
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
        *   - CA - C21 - 160
        *   - FL - C41 - 60
        *   - NV - C31 - 10
        *   - GA - C51 - 100
        */

        // Create 60 customers
        def customers = new ArrayList<Customer>()
        for (int i = 0; i < 60; i++) {
            def customerId = "customer-" + (i+1)
            def customer = CUSTOMERS.generate(customerId)
            customers.add(customer)
            customerTopic.pipeInput(customerId, customer)
        }

        // Assign states to all 60 customers
        def states = ["MN", "NY", "CA", "NV", "FL", "GA"]
        def addresses = new ArrayList<Address>()
        def stateIdx = 0
        for (int i = 0; i < 60; i++) {
            // Assign addresses for the next state
            if ((i != 0) && (i % 10 == 0)){
                stateIdx += 1
            }

            // Create address
            def addressID = "address-" + (i+1)
            def address = new Address(addressID, customers.get(i).id(), "00", "Permanent", "1016", "SE",
                    "NA", states.get(stateIdx), "00000", "00000", "USA", 1.00, 1.00)
            addresses.add(address)
            addressTopic.pipeInput(addressID, address)
        }

        // Create 4 artists
        def artists = new ArrayList<Artist>()
        for (int i = 0; i < 4; i++) {
            def artistId = "artist-" + (i+1)
            def artist = ARTISTS.generate(artistId)
            artists.add(artist)
            artistTopic.pipeInput(artistId, artist)
        }

        // Create 1000 streams per artist
        def streams = new ArrayList<Stream>()
        createArtist1Streams(streams)
        createArtist2Streams(streams)
        createArtist3Streams(streams)
        createArtist4Streams(streams)

        when: 'reading the output records'
        def outputRecords = outputTopic.readValuesToList().last()

        then: "Verify that Top 3 artists are returned"
        def aggregate = outputRecords.trendingArtistAggregates;
        assert aggregate.size() == 3

        then: "Verify that top 3 artists are artist-1, artist-4 and artist-3"
        def artist1 = aggregate.get(0)
        def artist4 = aggregate.get(1)
        def artist3 = aggregate.get(2)
        assert artist1.artist.id() == "artist-1"
        assert artist4.artist.id() == "artist-4"
        assert artist3.artist.id() == "artist-3"

        then: "Verify total number of unique customers that stream each of the top-3 artists"
        assert artist1.getUniqueCustomerCount() == 59
        assert artist4.getUniqueCustomerCount() == 53
        assert artist3.getUniqueCustomerCount() == 51

        then: "Verify that each of the top 3 artists has 5 states in their top-5 list"
        def artist1Map = artist1.stateAndCustomerStreamCountMap.map
        def artist4Map = artist4.stateAndCustomerStreamCountMap.map
        def artist3Map = artist3.stateAndCustomerStreamCountMap.map
        assert artist1Map.keySet().size() == 5
        assert artist4Map.keySet().size() == 5
        assert artist3Map.keySet().size() == 5


        then: "Verify top-5 state names for artist-1"
        def artist1States = (ArrayList<String>) artist1Map.keySet()
        assert artist1States.get(0) == "MN"
        assert artist1States.get(1) == "NY"
        assert artist1States.get(2) == "CA"
        assert artist1States.get(3) == "NV"
        assert artist1States.get(4) == "FL"

        then: "Verify top-5 state names for artist-4"
        def artist4States = (ArrayList<String>) artist4Map.keySet()
        assert artist4States.get(0) == "NV"
        assert artist4States.get(1) == "CA"
        assert artist4States.get(2) == "FL"
        assert artist4States.get(3) == "NY"
        assert artist4States.get(4) == "MN"

        then: "Verify top-5 state names for artist-3"
        def artist3States = (ArrayList<String>) artist3Map.keySet()
        assert artist3States.get(0) == "NY"
        assert artist3States.get(1) == "CA"
        assert artist3States.get(2) == "FL"
        assert artist3States.get(3) == "NV"
        assert artist3States.get(4) == "GA"

        then: "Verify top customer names from each of the top-5 states and their stream counts for all top-3 artists"
        def artist1CustomerStreamCounts = [["customer-1", 100], ["customer-11", 80], ["customer-21", 100], ["customer-31", 75], ["customer-41", 45]]
        assert checkCorrectTopCustomerAndStreamCounts(artist1Map, artist1States, artist1CustomerStreamCounts)

        def artist4CustomerStreamCounts = [["customer-31", 60], ["customer-21", 30], ["customer-41", 80], ["customer-11", 50], ["customer-1", 40]]
        assert checkCorrectTopCustomerAndStreamCounts(artist4Map, artist4States, artist4CustomerStreamCounts)

        def artist3CustomerStreamCounts = [["customer-11", 30], ["customer-21", 160], ["customer-41", 60], ["customer-31", 10], ["customer-51", 100]]
        assert checkCorrectTopCustomerAndStreamCounts(artist3Map, artist3States, artist3CustomerStreamCounts)
    }

    def checkCorrectTopCustomerAndStreamCounts(Map<String, CustomerStreamCountMap> artistMap, ArrayList<String> artistStates, ArrayList<List<Serializable>> customerStreamCounts){
        for (int i = 0; i < artistStates.size(); i++){
            // Get state name
            def state = artistStates[i]

            // Get top-customer and stream count information for that state
            def customerStreamCountInfo = customerStreamCounts[i]
            def customerId = customerStreamCountInfo[0]
            def customerStreamCount = customerStreamCountInfo[1]

            def customerStreamCountMap = artistMap.get(state).map
            def customerIds = (ArrayList<String>) customerStreamCountMap.keySet()

            // If either of the expected customerId or customer stream counts are wrong, return false
            if (customerIds.get(0) != customerId || customerStreamCountMap.get(customerId).streamCounts != customerStreamCount){
                return false
            }
        }
        return true
    }

    def createArtist4Streams(ArrayList<Stream> streams){
        // Create Artist 4 streams for MN customers
        def streamCountsMN = [40, 15, 10, 10, 10, 5, 5, 0, 0, 5]
        createArtistStreams(streamCountsMN, streams, "artist-4", 1)

        // Create Artist 4 streams for NY customers
        def streamCountsNY = [50, 20, 10, 10, 10, 5, 5, 5, 5]
        createArtistStreams(streamCountsNY, streams, "artist-4", 11)

        // Create Artist 4 streams for CA customers
        def streamCountsCA = [30, 20, 20, 10, 10, 10, 10, 10, 10, 10]
        createArtistStreams(streamCountsCA, streams, "artist-4", 21)

        // Create Artist 4 streams for NV customers
        def streamCountsNV = [60, 20, 10, 10, 10, 10, 10, 10, 10, 10]
        createArtistStreams(streamCountsNV, streams, "artist-4", 31)

        // Create Artist 4 streams for FL customers
        def streamCountsFL = [80, 20, 20, 10, 10, 10, 10, 10, 10]
        createArtistStreams(streamCountsFL, streams, "artist-4", 41)

        // Create Artist 4 streams for GA customers
        def streamCountsGA = [180, 30, 30, 15, 15, 15, 15]
        createArtistStreams(streamCountsGA, streams, "artist-4", 51)
    }

    def createArtist3Streams(ArrayList<Stream> streams){
        // Create Artist 3 streams for MN customers
        def streamCountsMN = [60, 25, 25, 25, 5, 5, 5]
        createArtistStreams(streamCountsMN, streams, "artist-3", 1)

        // Create Artist 3 streams for NY customers
        def streamCountsNY = [30, 10, 20, 10, 5, 5, 5, 5, 5, 5]
        createArtistStreams(streamCountsNY, streams, "artist-3", 11)

        // Create Artist 3 streams for CA customers
        def streamCountsCA = [160, 40, 80, 40, 40, 20, 10, 5, 5]
        createArtistStreams(streamCountsCA, streams, "artist-3", 21)

        // Create Artist 3 streams for NV customers
        def streamCountsNV = [10, 5, 5, 5, 5, 5, 5, 5, 5]
        createArtistStreams(streamCountsNV, streams, "artist-3", 31)

        // Create Artist 3 streams for FL customers
        def streamCountsFL = [60, 15, 5, 10, 5, 10, 5, 5, 5]
        createArtistStreams(streamCountsFL, streams, "artist-3", 41)

        // Create Artist 3 streams for GA customers
        def streamCountsGA = [100, 25, 25, 5, 10, 5, 10]
        createArtistStreams(streamCountsGA, streams, "artist-3", 51)
    }

    def createArtist2Streams(ArrayList<Stream> streams){
        // Create Artist 2 streams for MN customers
        def streamCountsMN = [80, 30, 20, 10, 20, 15, 10, 5, 5, 5]
        createArtistStreams(streamCountsMN, streams, "artist-2", 1)

        // Create Artist 2 streams for NY customers
        def streamCountsNY = [40, 15, 10, 10, 10, 10, 10, 5, 5, 5]
        createArtistStreams(streamCountsNY, streams, "artist-2", 11)

        // Create Artist 2 streams for CA customers
        def streamCountsCA = [150, 30, 20, 10, 5, 5, 5, 5]
        createArtistStreams(streamCountsCA, streams, "artist-2", 21)

        // Create Artist 2 streams for NV customers
        def streamCountsNV = [125, 35, 30, 10, 20, 15, 20, 15, 20, 10]
        createArtistStreams(streamCountsNV, streams, "artist-2", 31)

        // Create Artist 2 streams for FL customers
        def streamCountsFL = [20, 10, 5, 5, 5, 5]
        createArtistStreams(streamCountsFL, streams, "artist-2", 41)

        // Create Artist 2 streams for GA customers
        def streamCountsGA = [50, 20, 10, 10, 10]
        createArtistStreams(streamCountsGA, streams, "artist-2", 51)
    }

    def createArtist1Streams(ArrayList<Stream> streams){
        // Create Artist 1 streams for MN customers
        def streamCountsMN = [100, 40, 15, 15, 15, 15, 15, 10, 10, 5]
        createArtistStreams(streamCountsMN, streams, "artist-1", 1)

        // Create Artist 1 streams for NY customers
        def streamCountsNY = [80, 40, 20, 20, 10, 8, 7, 5, 5, 5]
        createArtistStreams(streamCountsNY, streams, "artist-1", 11)

        // Create Artist 1 streams for CA customers
        def streamCountsCA = [100, 30, 10, 10, 5, 5, 5, 5, 5, 5]
        createArtistStreams(streamCountsCA, streams, "artist-1", 21)

        // Create Artist 1 streams for NV customers
        def streamCountsNV = [75, 15, 20, 5, 10, 5, 10, 5, 10, 5]
        createArtistStreams(streamCountsNV, streams, "artist-1", 31)

        // Create Artist 1 streams for FL customers
        def streamCountsFL = [45, 15, 5, 10, 5, 10, 5, 10, 10, 5]
        createArtistStreams(streamCountsFL, streams, "artist-1", 41)

        // Create Artist 1 streams for GA customers
        def streamCountsGA = [20, 40, 10, 5, 5, 5, 5, 5, 5]
        createArtistStreams(streamCountsGA, streams, "artist-1", 51)
    }

    def createArtistStreams(ArrayList<Integer> artistStreams, ArrayList<Stream> streams, String artistName, int customerIdx){
        for (int totalStreamCountsPerCustomer : artistStreams){
            if (totalStreamCountsPerCustomer == 0){
                continue
            }
            (1..totalStreamCountsPerCustomer).forEach(streamCount -> {
                def customerId = "customer-" + customerIdx
                def stream = STREAMS.generate(customerId, artistName)
                streams.add(stream)
                streamsTopic.pipeInput(UUID.randomUUID().toString(), stream)
            })

            // Move onto next customer
            customerIdx += 1
        }
    }
}
