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
  class SocketWithTimeout
    attr_accessor :request_deadline

    # Opens a socket.
    #
    # @param host [String]
    # @param port [Integer]
    # @param connect_timeout [Integer] the connection timeout, in seconds.
    # @param timeout [Integer] the read and write timeout, in seconds.
    # @raise [Errno::ETIMEDOUT] if the timeout is exceeded.
    def initialize(host, port, connect_timeout: nil, timeout: nil, request_deadline: nil)
      addr = Socket.getaddrinfo(host, nil)
      sockaddr = Socket.pack_sockaddr_in(port, addr[0][3])

      @timeout = timeout
      @request_deadline = request_deadline

      @socket = Socket.new(Socket.const_get(addr[0][0]), Socket::SOCK_STREAM, 0)
      @socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)

      begin
        # Initiate the socket connection in the background. If it doesn't fail
        # immediately it will raise an IO::WaitWritable (Errno::EINPROGRESS)
        # indicating the connection is in progress.
        @socket.connect_nonblock(sockaddr)
      rescue IO::WaitWritable
        # IO.select will block until the socket is writable or the timeout
        # is exceeded, whichever comes first.
        unless RequestDeadline.new(connect_timeout, parent: request_deadline).wait(nil, [@socket])
          # IO.select returns nil when the socket is not ready before timeout
          # seconds have elapsed
          @socket.close
          raise Errno::ETIMEDOUT
        end

        begin
          # Verify there is now a good connection.
          @socket.connect_nonblock(sockaddr)
        rescue Errno::EISCONN
          # The socket is connected, we're good!
        end
      end
    rescue StandardError
      @socket.close if @socket && !@socket.closed?
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
          buffer << @socket.read_nonblock(num_bytes - buffer.bytesize)
        rescue IO::WaitReadable
          deadline.wait([@socket], nil)
        rescue IO::WaitWritable
          deadline.wait(nil, [@socket])
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
          offset += @socket.write_nonblock(bytes.byteslice(offset, bytes.bytesize - offset))
        rescue IO::WaitWritable
          deadline.wait(nil, [@socket])
        rescue IO::WaitReadable
          deadline.wait([@socket], nil)
        end
      end
      offset
    end

    def close
      @socket.close
    end

    def closed?
      @socket.closed?
    end

    def set_encoding(encoding)
      @socket.set_encoding(encoding)
    end
  end
end
