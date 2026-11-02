# frozen_string_literal: true

module Kafka
  class ConnectionBuilder
    def initialize(client_id:, logger:, instrumenter:, connect_timeout:, socket_timeout:, ssl_context:, sasl_authenticator:)
      @client_id = client_id
      @logger = TaggedLogger.new(logger)
      @instrumenter = instrumenter
      @connect_timeout = connect_timeout
      @socket_timeout = socket_timeout
      @ssl_context = ssl_context
      @sasl_authenticator = sasl_authenticator
    end

    def with_consumer_control(control)
      builder = dup
      builder.instance_variable_set(:@consumer_control, control)
      builder
    end

    def build_connection(host, port)
      connection = Connection.new(
        host: host,
        port: port,
        client_id: @client_id,
        connect_timeout: @connect_timeout,
        socket_timeout: @socket_timeout,
        logger: @logger,
        instrumenter: @instrumenter,
        ssl_context: @ssl_context,
        consumer_control: @consumer_control,
      )

      @sasl_authenticator.authenticate!(connection)

      connection
    end

  end
end
