# frozen_string_literal: true

require "timeout"

describe "consumer rebalance recovery" do
  let(:logger) { LOGGER }
  let(:instrumenter) { Kafka::Instrumenter.new(client_id: "test", group_id: "test") }
  let(:cluster) { double(:cluster, add_target_topics: nil, mark_as_stale!: nil, disconnect: nil) }
  let(:coordinator) { double(:coordinator, leave_group: nil) }
  let(:assignment) { Kafka::Protocol::MemberAssignment.new(topics: { "addresses" => [0] }) }
  let(:group) do
    Kafka::ConsumerGroup.new(cluster: cluster, logger: logger, group_id: "test",
      session_timeout: 120, retention_time: -1, instrumenter: instrumenter,
      group_coordination_timeout: 150)
  end
  let(:joined) do
    Kafka::Protocol::JoinGroupResponse.new(error_code: 0, generation_id: 1,
      group_protocol: "standard", leader_id: "other", member_id: "member", members: {})
  end
  let(:synced) { Kafka::Protocol::SyncGroupResponse.new(error_code: 0, member_assignment: assignment) }

  before do
    allow(cluster).to receive(:get_group_coordinator).and_return(coordinator)
    allow(coordinator).to receive(:join_group).and_return(joined)
    allow(coordinator).to receive(:sync_group).and_return(synced)
    group.subscribe("addresses")
  end

  it "returns coordinator failures to the caller without retrying inside discovery" do
    pool = double(:pool, connect: coordinator)
    metadata = Kafka::Protocol::MetadataResponse.new(
      brokers: [Kafka::BrokerInfo.new(node_id: 1, host: "broker", port: 9092)],
      controller_id: 1, topics: [])
    actual_cluster = Kafka::Cluster.new(seed_brokers: [URI("kafka://broker:9092")],
      broker_pool: pool, logger: logger)
    allow(coordinator).to receive(:fetch_metadata).and_return(metadata)
    allow(coordinator).to receive(:disconnect)
    allow(coordinator).to receive(:find_coordinator).and_raise(Kafka::CoordinatorNotAvailable)

    expect { actual_cluster.get_group_coordinator(group_id: "test") }.to raise_error(Kafka::CoordinatorNotAvailable)
    expect(coordinator).to have_received(:find_coordinator).once
  end

  it "does not retain membership when synchronization fails and can recover" do
    group.join
    expect(group).to be_member
    allow(coordinator).to receive(:sync_group).and_raise(Kafka::RebalanceInProgress)

    expect { group.join }.to raise_error(Kafka::RebalanceInProgress)
    expect(group).not_to be_member

    allow(coordinator).to receive(:sync_group).and_return(synced)
    group.join
    expect(group).to be_member
    expect(group.assigned_partitions).to eq("addresses" => [0])
    expect(coordinator).to have_received(:join_group).with(hash_including(timeout: 150)).exactly(3).times
    expect(coordinator).to have_received(:sync_group).with(hash_including(timeout: 150)).exactly(3).times
  end

  it "resets an unknown member and lets the next outer attempt rejoin" do
    group.join
    allow(coordinator).to receive(:join_group).and_raise(Kafka::UnknownMemberId)
    expect { group.join }.to raise_error(Kafka::UnknownMemberId)
    expect(group).not_to be_member

    allow(coordinator).to receive(:join_group).and_return(joined)
    group.join
    expect(coordinator).to have_received(:join_group).with(hash_including(member_id: "")).twice
    expect(group).to be_member
  end

  it "keeps foreground retry progress visible through an outage longer than startup" do
    elapsed = 0
    attempts = 0
    phases = []
    memberships = []
    fetcher = double(:fetcher, configure: nil, start: nil, stop: nil, reset: nil, seek: nil, data?: false)
    offsets = double(:offsets, commit_offsets: nil, commit_offsets_if_necessary: nil, clear_offsets: nil, clear_offsets_excluding: nil, next_offset_for: 0)
    consumer = Kafka::Consumer.new(cluster: cluster, logger: logger, instrumenter: instrumenter,
      group: group, fetcher: fetcher, offset_manager: offsets, session_timeout: 120,
      heartbeat: double(:heartbeat, trigger: nil))
    allow(consumer).to receive(:sleep) { |seconds| elapsed += seconds }
    allow(coordinator).to receive(:sync_group) do
      attempts += 1
      raise Kafka::RebalanceInProgress if attempts <= 7
      synced
    end
    allow(coordinator).to receive(:join_group) { elapsed += 60; joined }
    allow(fetcher).to receive(:reset) { consumer.stop }
    subscriber = proc do |*args|
      phases << args.last[:phase]
      memberships << group.member?
    end

    ActiveSupport::Notifications.subscribed(subscriber, "progress.consumer.kafka") do
      Timeout.timeout(2) { consumer.each_message { raise "processed before synchronization" } }
    end

    expect(elapsed).to be > 360
    expect(attempts).to eq(8)
    expect(phases.count(:sync_group)).to eq(8)
    expect(memberships).to all(be(false))
    expect(fetcher).not_to have_received(:data?)
    expect(fetcher).to have_received(:reset).once
  end
end
