# frozen_string_literal: true

module Kafka
  class ConsumerControl
    attr_reader :generation, :shutdown_deadline

    def initialize(shutdown_timeout:)
      raise ArgumentError, "shutdown_timeout must be positive" unless shutdown_timeout > 0

      @shutdown_timeout = shutdown_timeout
      @generation = 0
    end

    def start
      @shutdown_deadline = nil
      @generation += 1
    end

    def expired?
      @shutdown_deadline && Process.clock_gettime(Process::CLOCK_MONOTONIC) >= @shutdown_deadline
    end

    def stop
      return if @shutdown_deadline

      @shutdown_deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + @shutdown_timeout
      @generation += 1
    end
  end
end
