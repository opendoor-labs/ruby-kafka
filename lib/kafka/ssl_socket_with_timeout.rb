# frozen_string_literal: true

require "socket"
require "kafka/request_deadline"

module Kafka

  # Opens sockets in a non-blocking fashion, ensuring that we're not stalling
  # for long periods of time.
  #
  # It's possible to set timeouts for connecting to the server, for reading data,
  # and for writing data. Whenever a timeout is exceeded, Errno::ETIMEDOUT is
  # raised.
  #
  class SSLSocketWithTimeout
    attr_accessor :request_deadline

    # Opens a socket.
    #
    # @param host [String]
    # @param port [Integer]
    # @param connect_timeout [Integer] the connection timeout, in seconds.
    # @param timeout [Integer] the read and write timeout, in seconds.
    # @param ssl_context [OpenSSL::SSL::SSLContext] which SSLContext the ssl connection should use
    # @raise [Errno::ETIMEDOUT] if the timeout is exceeded.
    def initialize(host, port, connect_timeout: nil, timeout: nil, ssl_context:, request_deadline: nil)
      @tcp_socket = nil
      @ssl_socket = nil

      addr = Socket.getaddrinfo(host, nil)
      sockaddr = Socket.pack_sockaddr_in(port, addr[0][3])

      @timeout = timeout
      @request_deadline = request_deadline
      @connect_deadline = RequestDeadline.new(connect_timeout, parent: request_deadline)

      @tcp_socket = Socket.new(Socket.const_get(addr[0][0]), Socket::SOCK_STREAM, 0)
      @tcp_socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)

      # first initiate the TCP socket
      begin
        # Initiate the socket connection in the background. If it doesn't fail
        # immediately it will raise an IO::WaitWritable (Errno::EINPROGRESS)
        # indicating the connection is in progress.
        @tcp_socket.connect_nonblock(sockaddr)
      rescue IO::WaitWritable
        # select will block until the socket is writable or the timeout
        # is exceeded, whichever comes first.
        unless @connect_deadline.wait(nil, [@tcp_socket])
          # select returns nil when the socket is not ready before timeout
          # seconds have elapsed
          @tcp_socket.close
          raise Errno::ETIMEDOUT
        end

        begin
          # Verify there is now a good connection.
          @tcp_socket.connect_nonblock(sockaddr)
        rescue Errno::EISCONN
          # The socket is connected, we're good!
        end
      end

      # once that's connected, we can start initiating the ssl socket
      @ssl_socket = OpenSSL::SSL::SSLSocket.new(@tcp_socket, ssl_context)
      @ssl_socket.hostname = host

      begin
        # Initiate the socket connection in the background. If it doesn't fail
        # immediately it will raise an IO::WaitWritable (Errno::EINPROGRESS)
        # indicating the connection is in progress.
        # Unlike waiting for a tcp socket to connect, you can't time out ssl socket
        # connections during the connect phase properly, because IO.select only partially works.
        # Instead, you have to retry.
        @ssl_socket.connect_nonblock
      rescue Errno::EAGAIN, Errno::EWOULDBLOCK, IO::WaitReadable
        if @connect_deadline.wait([@ssl_socket], nil)
          retry
        else
          @ssl_socket.close
          close
          raise Errno::ETIMEDOUT
        end
      rescue IO::WaitWritable
        if @connect_deadline.wait(nil, [@ssl_socket])
          retry
        else
          close
          raise Errno::ETIMEDOUT
        end
      end
    rescue StandardError
      begin
        close
      rescue StandardError
        # Preserve the original connection setup failure; close is best-effort cleanup.
      end

      raise
    end

    # Reads bytes from the socket, possible with a timeout.
    #
    # @param num_bytes [Integer] the number of bytes to read.
    # @raise [Errno::ETIMEDOUT] if the timeout is exceeded.
    # @return [String] the data that was read from the socket.
    def read(num_bytes)
      deadline = @request_deadline || RequestDeadline.new(@timeout)
      buffer = String.new(encoding: Encoding::BINARY)

      until buffer.bytesize >= num_bytes
        deadline.check!
        begin
          buffer << @ssl_socket.read_nonblock(num_bytes - buffer.bytesize)
        rescue IO::WaitReadable
          deadline.wait([@ssl_socket], nil)
        rescue IO::WaitWritable
          deadline.wait(nil, [@ssl_socket])
        end
      end

      buffer
    end

    # Writes bytes to the socket, possible with a timeout.
    #
    # @param bytes [String] the data that should be written to the socket.
    # @raise [Errno::ETIMEDOUT] if the timeout is exceeded.
    # @return [Integer] the number of bytes written.
    def write(bytes)
      deadline = @request_deadline || RequestDeadline.new(@timeout)
      offset = 0
      while offset < bytes.bytesize
        deadline.check!
        begin
          offset += @ssl_socket.write_nonblock(bytes.byteslice(offset, bytes.bytesize - offset))
        rescue IO::WaitWritable
          deadline.wait(nil, [@ssl_socket])
        rescue IO::WaitReadable
          deadline.wait([@ssl_socket], nil)
        end
      end
      offset
    end

    def close
      @ssl_socket.close if @ssl_socket && !@ssl_socket.closed?
      @tcp_socket.close if @tcp_socket && !@tcp_socket.closed?
    end

    def closed?
      (@tcp_socket.nil? || @tcp_socket.closed?) || (@ssl_socket.nil? || @ssl_socket.closed?)
    end

    def set_encoding(encoding)
      @tcp_socket.set_encoding(encoding)
    end

  end
end
