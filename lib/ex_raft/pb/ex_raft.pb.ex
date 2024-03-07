defmodule ExRaft.Pb.MessageType do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.12.0"

  field :no_op, 0
  field :heartbeat, 1
  field :heartbeat_resp, 2
  field :append_entries, 3
  field :append_entries_resp, 4
  field :request_vote, 5
  field :request_vote_resp, 6
  field :propose, 7
  field :request_pre_vote, 8
  field :request_pre_vote_resp, 9
end

defmodule ExRaft.Pb.Message do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.12.0"

  field :type, 1, proto3_optional: true, type: ExRaft.Pb.MessageType, enum: true
  field :to, 2, proto3_optional: true, type: :uint64
  field :from, 3, proto3_optional: true, type: :uint64
  field :cluster_id, 4, proto3_optional: true, type: :uint64, json_name: "clusterId"
  field :term, 5, proto3_optional: true, type: :uint64
  field :log_term, 6, proto3_optional: true, type: :uint64, json_name: "logTerm"
  field :log_index, 7, proto3_optional: true, type: :uint64, json_name: "logIndex"
  field :commit, 8, proto3_optional: true, type: :uint64
  field :reject, 9, proto3_optional: true, type: :bool
  field :entries, 10, repeated: true, type: ExRaft.Pb.Entry
end

defmodule ExRaft.Pb.Entry do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.12.0"

  field :term, 1, proto3_optional: true, type: :uint64
  field :index, 2, proto3_optional: true, type: :uint64
  field :cmd, 3, proto3_optional: true, type: :bytes
end