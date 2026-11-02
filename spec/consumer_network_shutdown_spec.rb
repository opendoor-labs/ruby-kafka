# frozen_string_literal: true

require "timeout"

describe "consumer network deadlines" do
  let(:control) { Kafka::ConsumerControl.new(shutdown_timeout: 0.15) }
  let(:listener) { TCPServer.new("127.0.0.1", 0) }
  let(:ssl_context) { nil }
  let(:server) { listener }

  let(:connection) do
    Kafka::Connection.new(host: "127.0.0.1", port: listener.addr[1], client_id: "test",
      logger: LOGGER, instrumenter: Kafka::Instrumenter.new, connect_timeout: 1,
      socket_timeout: 0.05, consumer_control: control, ssl_context: ssl_context)
  end
  let(:request) { Kafka::Protocol::JoinGroupRequest.new(group_id: "test", session_timeout: 120, member_id: "") }

  before { server }

  after do
    @worker&.kill
    @worker&.join
    @peers&.each { |peer| peer.close unless peer.closed? }
    @peer&.close unless @peer&.closed?
    connection.close
    server.close
  end

  shared_examples "bounded consumer requests" do
    it "allows a coordination request to outlive the ordinary socket timeout" do
      @worker = Thread.new do
        @peer = server.accept
        size = @peer.read(4).unpack1("N")
        @peer.read(size)
        sleep 0.1
        @peer.close
      end
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)

      expect { connection.send_request(request, timeout: 0.3) }.to raise_error(Kafka::ConnectionError, /EOFError/)
      expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be >= 0.1
    end

    it "cancels a blocked JoinGroup from TERM and shares one deadline across cleanup requests" do
      accepted = Queue.new
      @worker = Thread.new do
        loop do
          peer = server.accept
          (@peers ||= []) << peer
          size = peer.read(4).unpack1("N")
          peer.read(size)
          accepted << true
        end
      end
      cluster = double(:cluster, add_target_topics: nil, disconnect: nil, mark_as_stale!: nil)
      builder = double(:builder, build_connection: connection)
      broker = Kafka::Broker.new(connection_builder: builder, host: "127.0.0.1", port: listener.addr[1], logger: LOGGER)
      allow(cluster).to receive(:get_group_coordinator).and_return(broker)
      group = Kafka::ConsumerGroup.new(cluster: cluster, logger: LOGGER, group_id: "test",
        session_timeout: 120, retention_time: -1, instrumenter: Kafka::Instrumenter.new,
        group_coordination_timeout: 150)
      group.subscribe("addresses")
      offsets = double(:offsets)
      cleanup_attempts = 0
      allow(offsets).to receive(:commit_offsets) do
        cleanup_attempts += 1
        connection.send_request(request, timeout: 150)
      end
      fetcher = double(:fetcher, configure: nil, start: nil, stop: nil)
      consumer = Kafka::Consumer.new(cluster: cluster, logger: LOGGER,
        instrumenter: Kafka::Instrumenter.new, group: group, fetcher: fetcher,
        offset_manager: offsets, session_timeout: 120, heartbeat: double(:heartbeat),
        consumer_control: control)
      old_handler = Signal.trap("TERM") { consumer.stop }
      signaller = Thread.new { accepted.pop; Process.kill("TERM", Process.pid) }
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)

      Timeout.timeout(2) { consumer.each_message { raise "processed before synchronization" } }

      expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be < 1
      expect(cleanup_attempts).to eq(1)
      expect(fetcher).to have_received(:stop).once
      expect(cluster).to have_received(:disconnect).once
    ensure
      Signal.trap("TERM", old_handler) if old_handler
      signaller&.kill
      signaller&.join
    end

    it "enforces one deadline across partial response reads" do
      @worker = Thread.new do
        @peer = server.accept
        size = @peer.read(4).unpack1("N")
        @peer.read(size)
        [20].pack("N").each_byte do |byte|
          @peer.write(byte.chr)
          sleep 0.03
        end
      end

      expect { connection.send_request(request) }.to raise_error(Kafka::ConnectionError, /ETIMEDOUT/)
      expect(connection).not_to be_open
    end  end

  include_examples "bounded consumer requests"

  context "with TLS" do
    let(:ssl_context) { OpenSSL::SSL::SSLContext.new }
    let(:server) do
      key = OpenSSL::PKey::RSA.new(2048)
      certificate = OpenSSL::X509::Certificate.new
      certificate.version = 2
      certificate.serial = 1
      certificate.subject = OpenSSL::X509::Name.parse("/CN=localhost")
      certificate.issuer = certificate.subject
      certificate.public_key = key.public_key
      certificate.not_before = Time.now - 60
      certificate.not_after = Time.now + 3600
      certificate.sign(key, OpenSSL::Digest::SHA256.new)
      context = OpenSSL::SSL::SSLContext.new
      context.cert = certificate
      context.key = key
      OpenSSL::SSL::SSLServer.new(listener, context)
    end

    include_examples "bounded consumer requests"
  end

end
