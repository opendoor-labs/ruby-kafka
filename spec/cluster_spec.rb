# frozen_string_literal: true

describe Kafka::Cluster do
  describe "#get_leader" do
    let(:broker) { double(:broker) }
    let(:broker_pool) { double(:broker_pool) }

    let(:cluster) {
      Kafka::Cluster.new(
        seed_brokers: [URI("kafka://test1:9092")],
        broker_pool: broker_pool,
        logger: LOGGER,
      )
    }

    before do
      allow(broker_pool).to receive(:connect) { broker }
      allow(broker).to receive(:disconnect)
    end

    it "raises LeaderNotAvailable if there's no leader for the partition" do
      metadata = Kafka::Protocol::MetadataResponse.new(
        brokers: [
          Kafka::BrokerInfo.new(
            node_id: 42,
            host: "test1",
            port: 9092,
          )
        ],
        controller_id: 42,
        topics: [
          Kafka::Protocol::MetadataResponse::TopicMetadata.new(
            topic_name: "greetings",
            partitions: [
              Kafka::Protocol::MetadataResponse::PartitionMetadata.new(
                partition_id: 42,
                leader: 2,
                partition_error_code: 5, # <-- this is the important bit.
              )
            ]
          )
        ],
      )

      allow(broker).to receive(:fetch_metadata) { metadata }

      expect {
        cluster.get_leader("greetings", 42)
      }.to raise_error Kafka::LeaderNotAvailable
    end

    it "raises InvalidTopic if the topic is invalid" do
      metadata = Kafka::Protocol::MetadataResponse.new(
        brokers: [
          Kafka::BrokerInfo.new(
            node_id: 42,
            host: "test1",
            port: 9092,
          )
        ],
        controller_id: 42,
        topics: [
          Kafka::Protocol::MetadataResponse::TopicMetadata.new(
            topic_name: "greetings",
            topic_error_code: 17, # <-- this is the important bit.
            partitions: []
          )
        ],
      )

      allow(broker).to receive(:fetch_metadata) { metadata }

      expect {
        cluster.get_leader("greetings", 42)
      }.to raise_error Kafka::InvalidTopic
    end

    it "raises ConnectionError if unable to connect to any of the seed brokers" do
      cluster = Kafka::Cluster.new(
        seed_brokers: [URI("kafka://not-there:9092"), URI("kafka://not-here:9092")],
        broker_pool: broker_pool,
        logger: LOGGER,
      )

      allow(broker_pool).to receive(:connect).and_raise(Kafka::ConnectionError)

      expect {
        cluster.get_leader("greetings", 42)
      }.to raise_exception(Kafka::ConnectionError)
    end
  end

  describe "#add_target_topics" do
    let(:broker_pool) { double(:broker_pool) }
    let(:ssl_context) { OpenSSL::SSL::SSLContext.new }
    let(:sasl_authenticator) { double(:sasl_authenticator) }
    let(:connection_builder) {
      Kafka::ConnectionBuilder.new(
        client_id: "test",
        logger: LOGGER,
        instrumenter: Kafka::Instrumenter.new(client_id: "test"),
        connect_timeout: 0.1,
        socket_timeout: 0.1,
        ssl_context: ssl_context,
        sasl_authenticator: sasl_authenticator,
      )
    }
    let(:bad_broker) {
      Kafka::Broker.new(
        connection_builder: connection_builder,
        host: "bad-broker",
        port: 9096,
        logger: LOGGER,
      )
    }
    let(:good_broker) { double(:good_broker, disconnect: nil) }
    let(:seed_brokers) {
      [
        URI("kafka://bad-broker:9096"),
        URI("kafka://good-broker:9096"),
      ]
    }

    let(:cluster) {
      Kafka::Cluster.new(
        seed_brokers: seed_brokers,
        broker_pool: broker_pool,
        logger: LOGGER,
      )
    }

    let(:metadata) {
      Kafka::Protocol::MetadataResponse.new(
        brokers: [
          Kafka::BrokerInfo.new(
            node_id: 42,
            host: "good-broker",
            port: 9096,
          )
        ],
        controller_id: 42,
        topics: [],
      )
    }

    before do
      # Block form (not `.and_return(seed_brokers.dup)`) so each invocation
      # gets a fresh dup — defends against a future Cluster change that
      # calls shuffle more than once per metadata refresh.
      allow(seed_brokers).to receive(:shuffle) { seed_brokers.dup }

      # Load-bearing: the SASL authenticate! stub calls send_request, which
      # forces ConnectionBuilder to actually open the socket. Without this,
      # the SSLSocketWithTimeout stub below would never fire and we wouldn't
      # be exercising the real Connection -> SSL handshake path. Don't remove.
      allow(sasl_authenticator).to receive(:authenticate!) do |connection|
        connection.send_request(Kafka::Protocol::SaslHandshakeRequest.new("PLAIN"))
      end

      allow(good_broker).to receive(:fetch_metadata).and_return(metadata)
    end

    it "falls through to the next seed broker when SSL handshake fails during metadata refresh" do
      expect(Kafka::SSLSocketWithTimeout).to receive(:new).with(
        "bad-broker",
        9096,
        connect_timeout: 0.1,
        timeout: 0.1,
        ssl_context: ssl_context,
      ).and_raise(OpenSSL::SSL::SSLError, "unexpected eof while reading")
      expect(broker_pool).to receive(:connect).with("bad-broker", 9096).ordered.and_return(bad_broker)
      expect(broker_pool).to receive(:connect).with("good-broker", 9096).ordered.and_return(good_broker)

      expect {
        cluster.add_target_topics(["greetings"])
      }.not_to raise_error
    end
  end
end
