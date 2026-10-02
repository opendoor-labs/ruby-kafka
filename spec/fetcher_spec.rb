# frozen_string_literal: true

describe Kafka::Fetcher do
  let(:group) { double(:group, :group_id => '123') }
  let(:instrumenter) { double(:instrumenter) }
  let(:logger) { Logger.new(StringIO.new) }
  let(:cluster) { double(:cluster) }
  let(:fetcher) do
    described_class.new(group: group,
                        instrumenter: instrumenter,
                        logger: logger,
                        cluster: cluster,
                        max_queue_size: 1)
  end

  before do
    # don't actually start the loop
    thread = double(:thread)
    allow(thread).to receive(:abort_on_exception=)
    allow(Thread).to receive(:new).and_return(thread)
  end

  describe '#handle_seek' do
    it "should instrument seeks" do
      allow(instrumenter).to receive(:instrument)
      fetcher.send(:handle_seek, 'my-topic', 1, 2)
      expect(instrumenter).to have_received(:instrument).with(
        'seek.consumer',
        group_id: '123',
        topic: 'my-topic',
        partition: 1,
        offset: 2
      )
    end
  end

  describe '#reset' do
    let(:batch) do
      Kafka::FetchedBatch.new(topic: 'my-topic', partition: 1, last_offset: 2,
                              highwater_mark_offset: 3, messages: [double(:message, offset: 2)])
    end

    before do
      allow(instrumenter).to receive(:instrument)
    end

    it 'discards a response when reset was requested during its fetch' do
      allow(fetcher).to receive(:fetch_batches) do
        fetcher.reset
        [batch]
      end

      fetcher.send(:step)

      expect(fetcher.poll).to eq [:batches, []]

      fetcher.send(:handle_reset, 1)
      allow(fetcher).to receive(:fetch_batches).and_return([batch])
      fetcher.send(:step)

      expect(fetcher.poll).to eq [:batches, [batch]]
    end

    it 'discards responses until all requested resets have been processed' do
      fetcher.reset
      fetcher.reset
      allow(fetcher).to receive(:fetch_batches).and_return([batch])

      fetcher.send(:handle_reset, 1)
      fetcher.send(:step)
      expect(fetcher.poll).to eq [:batches, []]

      fetcher.send(:handle_reset, 2)
      fetcher.send(:step)
      expect(fetcher.poll).to eq [:batches, [batch]]
    end
  end
end
