# frozen_string_literal: true

require 'fake_server'

describe Kafka::Connection do
  let(:logger) { LOGGER }
  let(:host) { "127.0.0.1" }
  let(:server) { TCPServer.new(host, 0) }
  let(:port) { server.addr[1] }
  let(:ssl_context) { nil }

  let(:connection) {
    Kafka::Connection.new(
      host: host,
      port: port,
      client_id: "test",
      logger: logger,
      instrumenter: Kafka::Instrumenter.new(client_id: "test"),
      connect_timeout: 0.1,
      socket_timeout: 0.1,
      ssl_context: ssl_context,
    )
  }

  let!(:broker) { FakeServer.start(server) }

  describe "#send_request" do
    let(:api_key) { 0 }
    let(:request) { double(:request, api_key: api_key) }
    let(:response_decoder) { double(:response_decoder) }

    before do
      allow(request).to receive(:encode) {|encoder| encoder.write_string("hello!") }
      allow(request).to receive(:response_class) { response_decoder }

      allow(response_decoder).to receive(:decode) {|decoder|
        decoder.string
      }
    end

    it "sends requests to a broker and reads back the response" do
      response = connection.send_request(request)

      expect(response).to eq "hello!"
    end

    it "skips responses to previous requests" do
      # By passing nil as the final argument we're telling Connection that we're
      # not expecting a response, so it won't read one. However, the fake broker
      # *is* writing a response, so we'll get that the next time we read a response,
      # causing a mismatch. This simulates the client killing the connection due
      # to e.g. a timeout, then resuming with a new request -- the old response
      # still sits in the connection waiting to be read.
      allow(request).to receive(:response_class) { nil }

      connection.send_request(request)

      allow(request).to receive(:encode) {|encoder| encoder.write_string("goodbye!") }
      allow(request).to receive(:response_class) { response_decoder }
      response = connection.send_request(request)

      expect(response).to eq "goodbye!"
    end

    it "disconnects on network errors" do
      response = connection.send_request(request)

      expect(response).to eq "hello!"

      broker.kill
      # join to avoid race where send_request fires before broker thread ends
      broker.join

      expect {
        connection.send_request(request)
      }.to raise_error(Kafka::ConnectionError)
    end

    it "disconnects on SSL errors during request IO" do
      allow(connection).to receive(:write_request).and_raise(
        OpenSSL::SSL::SSLError,
        "tls alert",
      )

      expect(connection).to receive(:close).and_call_original

      expect {
        connection.send_request(request)
      }.to raise_error(Kafka::ConnectionError, /tls alert/)
    end

    context "when SSL connection setup fails" do
      let(:ssl_context) { OpenSSL::SSL::SSLContext.new }

      def expect_ssl_setup_error_wrapped(error_class, message)
        allow(Kafka::SSLSocketWithTimeout).to receive(:new).and_raise(
          error_class,
          message,
        )

        expect {
          connection.send_request(request)
        }.to raise_error(Kafka::ConnectionError, /#{Regexp.escape(message)}/)
      end

      it "wraps SSL errors in a connection error" do
        expect_ssl_setup_error_wrapped(
          OpenSSL::SSL::SSLError,
          "unexpected eof while reading",
        )
      end

      it "wraps certificate errors in a connection error" do
        expect_ssl_setup_error_wrapped(
          OpenSSL::X509::CertificateError,
          "certificate verify failed",
        )
      end

    end

    it "re-opens the connection after a network error" do
      connection.send_request(request)
      broker.kill
      # join to avoid race where send_request fires before broker thread ends
      broker.join

      # Connection is torn down
      connection.send_request(request) rescue nil

      server.close
      server = TCPServer.new(host, port)
      broker = FakeServer.start(server)

      # Connection is re-established
      response = connection.send_request(request)

      expect(response).to eq "hello!"
    end

    it "emits a notification" do
      events = []

      subscriber = proc {|*args|
        events << ActiveSupport::Notifications::Event.new(*args)
      }

      ActiveSupport::Notifications.subscribed(subscriber, "request.connection.kafka") do
        connection.send_request(request)
      end

      expect(events.count).to eq 1

      event = events.first

      expect(event.payload[:api]).to eq :produce
      expect(event.payload[:request_size]).to eq 22
      expect(event.payload[:response_size]).to eq 12
    end
  end
end
