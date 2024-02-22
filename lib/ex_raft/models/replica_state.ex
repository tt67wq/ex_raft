defmodule ExRaft.Models.ReplicaState do
  @moduledoc """
  Replica State
  """
  alias ExRaft.Models

  @type index_t :: non_neg_integer() | -1
  @type replica_id_t :: non_neg_integer() | -1

  @type t :: %__MODULE__{
          self: Models.Replica.t(),
          peers: list(Models.Replica.t()),
          term: non_neg_integer(),
          election_reset_ts: non_neg_integer(),
          election_timeout: non_neg_integer(),
          election_check_delta: non_neg_integer(),
          heartbeat_delta: non_neg_integer(),
          voted_for: replica_id_t(),
          leader_id: replica_id_t(),
          last_log_index: index_t(),
          commit_index: index_t(),
          last_applied: index_t(),
          next_index: %{replica_id_t() => non_neg_integer()},
          match_index: %{replica_id_t() => index_t()},
          rpc_impl: ExRaft.Rpc.t(),
          log_store_impl: ExRaft.LogStore.t(),
          statemachine_impl: ExRaft.Statemachine.t()
        }

  defstruct self: nil,
            peers: [],
            term: 0,
            election_reset_ts: 0,
            election_timeout: 0,
            election_check_delta: 0,
            heartbeat_delta: 0,
            voted_for: -1,
            leader_id: -1,
            last_log_index: -1,
            commit_index: -1,
            last_applied: -1,
            next_index: %{},
            match_index: %{},
            rpc_impl: nil,
            log_store_impl: nil,
            statemachine_impl: nil
end
