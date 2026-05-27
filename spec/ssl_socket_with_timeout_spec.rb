# frozen_string_literal: true

describe Kafka::SSLSocketWithTimeout, ".open" do
  let(:ssl_context) { OpenSSL::SSL::SSLContext.new }

  it "times out if the server doesn't accept the connection within the timeout" do
    host = "172.16.0.0" # this address is non-routable!
    port = 4444

    timeout = 0.1
    allowed_time = timeout + 0.1

    start = Time.now

    expect {
      Kafka::SSLSocketWithTimeout.new(host, port, connect_timeout: timeout, timeout: 1, ssl_context: OpenSSL::SSL::SSLContext.new)
    }.to raise_exception(Errno::ETIMEDOUT)

    finish = Time.now

    expect(finish - start).to be < allowed_time
  end

  it "closes sockets when SSL handshake setup fails" do
    tcp_socket = double(:tcp_socket)
    ssl_socket = double(:ssl_socket)

    allow(Socket).to receive(:getaddrinfo).and_return([["AF_INET", nil, nil, "127.0.0.1"]])
    allow(Socket).to receive(:pack_sockaddr_in).and_return("sockaddr")
    allow(Socket).to receive(:new).and_return(tcp_socket)
    allow(tcp_socket).to receive(:setsockopt)
    allow(tcp_socket).to receive(:connect_nonblock).and_return(0)
    allow(tcp_socket).to receive(:closed?).and_return(false)
    expect(tcp_socket).to receive(:close)

    allow(OpenSSL::SSL::SSLSocket).to receive(:new).with(tcp_socket, ssl_context).and_return(ssl_socket)
    allow(ssl_socket).to receive(:hostname=)
    allow(ssl_socket).to receive(:connect_nonblock).and_raise(
      OpenSSL::SSL::SSLError,
      "unexpected eof while reading",
    )
    allow(ssl_socket).to receive(:closed?).and_return(false)
    expect(ssl_socket).to receive(:close)

    expect {
      Kafka::SSLSocketWithTimeout.new(
        "broker.example.com",
        9096,
        connect_timeout: 0.1,
        timeout: 1,
        ssl_context: ssl_context,
      )
    }.to raise_error(OpenSSL::SSL::SSLError, /unexpected eof while reading/)
  end
end
