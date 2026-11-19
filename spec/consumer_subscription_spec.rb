# frozen_string_literal: true

describe Kafka::Consumer, "subscription assignments" do
  let(:logger) { Logger.new(StringIO.new) }
  let(:instrumenter) { Kafka::Instrumenter.new(client_id: "test", group_id: "test") }
  let(:coordinator) { double(:coordinator) }
  let(:cluster) { double(:cluster, add_target_topics: nil, disconnect: nil) }
  let(:heartbeat) { double(:heartbeat, trigger: nil) }
  let(:fetcher) do
    double(:fetcher, configure: nil, subscribe: nil, start: nil, stop: nil,
      reset: nil, seek: nil, data?: true)
  end
  let(:group) do
    Kafka::ConsumerGroup.new(cluster: cluster, logger: logger, group_id: "test",
      session_timeout: 30, retention_time: 3600, instrumenter: instrumenter)
  end
  let(:offset_manager) do
    Kafka::OffsetManager.new(cluster: cluster, group: group, fetcher: fetcher,
      logger: logger, commit_interval: 10, commit_threshold: 0, offset_retention_time: 3600)
  end
  let(:consumer) do
    described_class.new(cluster: cluster, logger: logger, instrumenter: instrumenter,
      group: group, fetcher: fetcher, offset_manager: offset_manager,
      session_timeout: 30, heartbeat: heartbeat)
  end
  let(:assignment) do
    Kafka::RoundRobinAssignmentStrategy.new(cluster: cluster).assign(
      members: ["old-member", "new-member"], topics: ["retained", "removed"]
    ).fetch("new-member")
  end
  let(:committed_offsets) do
    topics = { "retained" => 42, "removed" => 7 }.each_with_object({}) do |(topic, offset), result|
      result[topic] = {
        1 => Kafka::Protocol::OffsetFetchResponse::PartitionOffsetInfo.new(
          offset: offset, metadata: nil, error_code: 0
        )
      }
    end
    Kafka::Protocol::OffsetFetchResponse.new(topics: topics)
  end
  let(:messages) do
    ["retained", "removed"].map do |topic|
      double(:message, topic: topic, partition: 1, offset: 42, create_time: Time.now,
        key: nil, value: "message", headers: {}, is_control_record: false)
    end
  end
  let(:batches) do
    messages.map do |message|
      Kafka::FetchedBatch.new(topic: message.topic, partition: message.partition,
        last_offset: message.offset, highwater_mark_offset: 43, messages: [message])
    end
  end

  before do
    allow(cluster).to receive(:get_group_coordinator).with(group_id: "test").and_return(coordinator)
    allow(cluster).to receive(:partitions_for).and_return(
      [double(:partition, partition_id: 0), double(:partition, partition_id: 1)]
    )
    allow(coordinator).to receive(:join_group).and_return(
      Kafka::Protocol::JoinGroupResponse.new(error_code: 0, generation_id: 1,
        group_protocol: "roundrobin", leader_id: "old-member", member_id: "new-member",
        members: { "old-member" => "", "new-member" => "" })
    )
    allow(coordinator).to receive(:sync_group).and_return(
      Kafka::Protocol::SyncGroupResponse.new(error_code: 0, member_assignment: assignment)
    )
    allow(coordinator).to receive(:fetch_offsets).and_return(committed_offsets)
    allow(coordinator).to receive(:commit_offsets).and_return(double(:response, topics: {}))
    allow(coordinator).to receive(:leave_group)
    consumer.subscribe("retained", max_bytes_per_partition: 2048)
  end

  it "fetches the subscribed assignment from a mixed group assignment" do
    expect(fetcher).to receive(:poll).once.and_return([:batches, batches])
    received = []

    consumer.each_message do |message|
      received << message.topic
      consumer.stop
    end

    expect(received).to eq(["retained"])
    expect(group.assigned_partitions).to eq("retained" => [1], "removed" => [1])
    expect(group.subscribed_partitions).to eq("retained" => [1])
    expect(fetcher).to have_received(:seek).with("retained", 1, 42).once
    expect(fetcher).not_to have_received(:seek).with("removed", 1, anything)
    expect(fetcher).to have_received(:subscribe).with("retained", max_bytes_per_partition: 2048)
  end

  it "keeps every locally subscribed assigned topic consumable" do
    consumer.subscribe("removed", max_bytes_per_partition: 4096)
    expect(fetcher).to receive(:poll).once.and_return([:batches, batches])
    received = []

    consumer.each_message do |message|
      received << message.topic
      consumer.stop if received.size == 2
    end

    expect(received).to eq(["retained", "removed"])
    expect(fetcher).to have_received(:seek).with("retained", 1, 42).once
    expect(fetcher).to have_received(:seek).with("removed", 1, 7).once
    expect(fetcher).to have_received(:subscribe).with("removed", max_bytes_per_partition: 4096)
  end

  it "keeps a paused subscription idle until it resumes" do
    consumer.pause("retained", 1)
    consumer.send(:join_group)

    expect(fetcher).not_to have_received(:seek)

    consumer.resume("removed", 1)
    expect(fetcher).not_to have_received(:seek)

    consumer.resume("retained", 1)
    expect(fetcher).to have_received(:seek).with("retained", 1, 42).once
  end
end
