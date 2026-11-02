# frozen_string_literal: true

module Kafka
  class RequestDeadline
    def initialize(timeout, consumer_control: nil, parent: nil)
      @parent = parent
      @deadline = timeout && now + timeout
      @consumer_control = consumer_control
      @generation = consumer_control&.generation
    end

    def check!
      if @consumer_control && @generation != @consumer_control.generation
        raise IOError, "consumer request cancelled"
      end

      limit = [@deadline, @consumer_control&.shutdown_deadline].compact.min
      remaining = [limit && limit - now, @parent&.check!].compact.min
      raise Errno::ETIMEDOUT if remaining && remaining <= 0

      remaining
    end

    def wait(readers, writers)
      loop do
        remaining = check!
        interval = [remaining || 0.1, 0.1].min
        ready = IO.select(readers, writers, nil, interval)
        check!
        return ready if ready
      end
    end

    private

    def now
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
  end
end
