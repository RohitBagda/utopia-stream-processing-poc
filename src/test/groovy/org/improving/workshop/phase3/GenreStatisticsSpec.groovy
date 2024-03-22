package org.improving.workshop.phase3

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.apache.kafka.streams.TopologyTestDriver
import org.improving.workshop.Streams
import org.msse.demo.mockdata.music.artist.Artist
import org.msse.demo.mockdata.music.event.Event
import org.msse.demo.mockdata.music.ticket.Ticket
import spock.lang.Specification

import static org.improving.workshop.phase3.GenreStatistics.*
import static org.improving.workshop.utils.DataFaker.EVENTS

class GenreStatisticsSpec extends Specification{
    TopologyTestDriver driver

    // inputs
    TestInputTopic<String, Event> eventsTopic
    TestInputTopic<String, Artist> artistsTopic
    TestInputTopic<String, Ticket> ticketsTopic

    // outputs
    TestOutputTopic<String, GenreStatistic> outputTopic

    def 'setup'(){
        // instantiate new builder
        StreamsBuilder streamsBuilder = new StreamsBuilder()

        // build the CustomerStreamCount topology (by reference)
        configureTopology(streamsBuilder)

        // build the TopologyTestDriver
        driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties())

        eventsTopic = driver.createInputTopic(Streams.TOPIC_DATA_DEMO_EVENTS, Serdes.String().serializer(), Streams.SERDE_EVENT_JSON.serializer())
        ticketsTopic = driver.createInputTopic(Streams.TOPIC_DATA_DEMO_TICKETS, Serdes.String().serializer(), Streams.SERDE_TICKET_JSON.serializer())
        artistsTopic = driver.createInputTopic(Streams.TOPIC_DATA_DEMO_ARTISTS, Serdes.String().serializer(), Streams.SERDE_ARTIST_JSON.serializer())
        outputTopic = driver.createOutputTopic(OUTPUT_TOPIC, Serdes.String().deserializer(), GENRE_STATISTIC_JSON_SERDE.deserializer())
    }

    def 'cleanup'() {
        // if this close doesn't run (test fails halfway through), subsequent tests may start on old state
        // run the test and let it cleanup, then run the test again.
        driver.close()
    }

    def 'genre statistic'(){
        given: 'a genre and multiple artists and events and tickets'

        // Number of artists = 2
        // The number of events = 2
        // number of tickets = 20 * 2 * 2 = 80
        // price * number of tickets = 50 * 80 = 4000
        def funkArtists = createArtists("Funk", 2)
        funkArtists.forEach{artist ->
            def funkArtistEvents = createEvents(artist.id(), 2)
            funkArtistEvents.forEach{
                event -> createTickets(event.id(), 20, 50)
            }
        }

        when: "Reading the output records"
        def outputRecords = outputTopic.readRecordsToList()

        then: 'the expected number of records were received'
        assert outputRecords.size() == 80

        and: 'Expected results for Funk Artists'
        assert outputRecords.last().value().genre == "Funk"
        assert outputRecords.last().value().totalArtists == 2
        assert outputRecords.last().value().totalTicketsSold == 80
        assert outputRecords.last().value().totalRevenue == 4000

        then: "Given a new Genre with new artists and events and tickets"
        // Number of artists = 4
        // The number of events = 4
        // number of tickets = 10 * 4 * 4 = 160
        // price * number of tickets = 10 * 1600 = 16000
        def hipHopArtists = createArtists("HipHop", 4)
        hipHopArtists.forEach{artist ->
            def hipHopArtistEvents = createEvents(artist.id(), 4)
            hipHopArtistEvents.forEach{
                event -> createTickets(event.id(), 10, 10)
            }
        }

        when: "Reading the output records again"
        outputRecords = outputTopic.readRecordsToList()

        then: 'the expected number of records were received'
        assert outputRecords.size() == 160

        and: "expected results for HipHop Artists"
        assert outputRecords.last().value().genre == "HipHop"
        assert outputRecords.last().value().totalArtists == 4
        assert outputRecords.last().value().totalTicketsSold == 160
        assert outputRecords.last().value().totalRevenue == 1600

        then: "Given an existing genre, create a new funk artist, a new event, and a new ticket ticket"
        def newFunkArtist = createArtists("Funk", 1)
        def newEvent = createEvents(newFunkArtist.first().id(), 1)
        createTickets(newEvent.first().id(), 1, 10)

        when: "Reading the output records again"
        outputRecords = outputTopic.readRecordsToList()

        then: 'the expected number of records were received'
        assert outputRecords.size() == 1

        and: 'Expected results for Funk Artists'
        assert outputRecords.last().value().genre == "Funk"
        assert outputRecords.last().value().totalArtists == 3
        assert outputRecords.last().value().totalTicketsSold == 81
        assert outputRecords.last().value().totalRevenue == 4010

        then: "Given an existing genre, create an existing funk artist, new event, and new ticket"
        def existingFunkArtist = funkArtists.first()
        def anotherNewEvent = createEvents(existingFunkArtist.id(), 1)
        createTickets(anotherNewEvent.first().id(), 1, 50)

        when: "Reading the output records again"
        outputRecords = outputTopic.readRecordsToList()

        then: 'the expected number of records were received'
        assert outputRecords.size() == 1

        and: 'Expected results for Funk Artists'
        assert outputRecords.last().value().genre == "Funk"
        assert outputRecords.last().value().totalArtists == 3
        assert outputRecords.last().value().totalTicketsSold == 82
        assert outputRecords.last().value().totalRevenue == 4060
    }

    def createTickets(String eventId, int numTickets, double price){
        def tickets = new ArrayList<Ticket>();
        for (int i = 0; i < numTickets; i++){
            def ticketId = "ticket" + i + "-" + UUID.randomUUID().toString()
            def ticket = new Ticket(ticketId, UUID.randomUUID().toString(), eventId, price)
            tickets.add(ticket)
            ticketsTopic.pipeInput(ticketId, ticket)
        }
        return tickets
    }

    def createEvents(String artistId, int numEvents) {
        def events = new ArrayList<Event>();
        for (int i = 0; i < numEvents; i++){
            def eventId = "event" + i + "-" + UUID.randomUUID().toString()
            def event = EVENTS.generate(eventId, artistId, UUID.randomUUID().toString(), new Random().nextInt())
            events.add(event)
            eventsTopic.pipeInput(eventId, event)
        }
        return events
    }

    def createArtists(String genre, int numArtists){
        def artists = new ArrayList<Artist>();
        for (int i = 0; i < numArtists; i++){
            def artistId = "artist" + i + "-" + UUID.randomUUID().toString()
            def artistName = "artist-name" + i + "-" + UUID.randomUUID().toString()
            def artist = new Artist(artistId, artistName, genre)
            artists.add(artist)
            artistsTopic.pipeInput(artistId, artist)
        }
        return artists
    }
}