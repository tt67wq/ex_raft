defmodule ExRaft.Pb.MessageType do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.12.0"

  field :heartbeat, 0
  field :heartbeat_resp, 1
  field :append_entries, 2
  field :append_entries_resp, 3
  field :request_vote, 4
  field :request_vote_resp, 5
  field :propose, 6
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
