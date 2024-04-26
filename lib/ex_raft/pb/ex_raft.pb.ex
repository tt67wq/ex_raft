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
  field :read_index, 11
  field :read_index_resp, 12
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

  field :type, 1, type: ExRaft.Pb.MessageType, enum: true
  field :to, 2, type: :uint64
  field :from, 3, type: :uint64
  field :term, 4, type: :uint64
  field :log_term, 5, type: :uint64, json_name: "logTerm"
  field :log_index, 6, type: :uint64, json_name: "logIndex"
  field :commit, 7, type: :uint64
  field :reject, 8, type: :bool
  field :entries, 9, repeated: true, type: ExRaft.Pb.Entry
  field :hint, 10, type: :uint64
  field :ref, 11, type: :bytes
end

defmodule ExRaft.Pb.Entry do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.12.0"

  field :term, 1, type: :uint64
  field :type, 2, type: ExRaft.Pb.EntryType, enum: true
  field :index, 3, type: :uint64
  field :cmd, 4, type: :bytes
end

defmodule ExRaft.Pb.ConfigChange do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.12.0"

  field :config_change_id, 1, type: :uint64, json_name: "configChangeId"
  field :type, 2, type: ExRaft.Pb.ConfigChangeType, enum: true
  field :replica_id, 3, type: :uint64, json_name: "replicaId"
  field :addr, 4, type: :string
end

defmodule ExRaft.Pb.SnapshotMetadata.AddressesEntry do
  @moduledoc false

  use Protobuf, map: true, syntax: :proto3, protoc_gen_elixir_version: "0.12.0"

  field :key, 1, type: :uint64
  field :value, 2, type: :string
end

defmodule ExRaft.Pb.SnapshotMetadata do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.12.0"

  field :filepath, 1, type: :string
  field :replica_id, 2, type: :uint64, json_name: "replicaId"
  field :term, 3, type: :uint64
  field :index, 4, type: :uint64
  field :addresses, 5, repeated: true, type: ExRaft.Pb.SnapshotMetadata.AddressesEntry, map: true
end
