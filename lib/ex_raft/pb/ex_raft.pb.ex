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
  field :config_change, 10
end

defmodule ExRaft.Pb.EntryType do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.12.0"

  field :etype_no_op, 0
  field :etype_normal, 1
  field :etype_config_change, 2
end

defmodule ExRaft.Pb.ConfigChangeType do
  @moduledoc false

  use Protobuf, enum: true, syntax: :proto3, protoc_gen_elixir_version: "0.12.0"

  field :cctype_add_node, 0
  field :cctype_remove_node, 1
end

defmodule ExRaft.Pb.Message do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.12.0"

  field :type, 1, proto3_optional: true, type: ExRaft.Pb.MessageType, enum: true
  field :to, 2, proto3_optional: true, type: :uint64
  field :from, 3, proto3_optional: true, type: :uint64
  field :term, 4, proto3_optional: true, type: :uint64
  field :log_term, 5, proto3_optional: true, type: :uint64, json_name: "logTerm"
  field :log_index, 6, proto3_optional: true, type: :uint64, json_name: "logIndex"
  field :commit, 7, proto3_optional: true, type: :uint64
  field :reject, 8, proto3_optional: true, type: :bool
  field :entries, 9, repeated: true, type: ExRaft.Pb.Entry
end

defmodule ExRaft.Pb.Entry do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.12.0"

  field :term, 1, proto3_optional: true, type: :uint64
  field :type, 2, proto3_optional: true, type: ExRaft.Pb.EntryType, enum: true
  field :index, 3, proto3_optional: true, type: :uint64
  field :cmd, 4, proto3_optional: true, type: :bytes
end

defmodule ExRaft.Pb.ConfigChange do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.12.0"

  field :config_change_id, 1, proto3_optional: true, type: :uint64, json_name: "configChangeId"
  field :type, 2, proto3_optional: true, type: ExRaft.Pb.ConfigChangeType, enum: true
  field :replica_id, 3, proto3_optional: true, type: :uint64, json_name: "replicaId"
  field :addr, 4, proto3_optional: true, type: :string
end