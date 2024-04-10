defmodule ExRaft.Models.ReplicaState do
  @moduledoc """
  Replica State
  """
  alias ExRaft.Models
  alias ExRaft.Typespecs

  @type t :: %__MODULE__{
          self: Typespecs.replica_id_t(),
          remotes: %{Typespecs.replica_id_t() => Models.Replica.t()},
          members_count: non_neg_integer(),
          term: non_neg_integer(),
          tick_delta: pos_integer(),
          local_tick: non_neg_integer(),
          election_tick: non_neg_integer(),
          heartbeat_tick: non_neg_integer(),
          apply_tick: non_neg_integer(),
          election_timeout: non_neg_integer(),
          randomized_election_timeout: non_neg_integer(),
          heartbeat_timeout: non_neg_integer(),
          apply_timeout: non_neg_integer(),
          voted_for: Typespecs.replica_id_t(),
          leader_id: Typespecs.replica_id_t(),
          last_index: Typespecs.index_t(),
          commit_index: Typespecs.index_t(),
          last_applied: Typespecs.index_t(),
          votes: %{non_neg_integer() => bool()},
          pending_config_change?: boolean(),
          read_index_q: [Typespecs.ref()],
          read_index_waiter: %{Typespecs.ref() => Models.ReadStatus.t()},
          remote_impl: ExRaft.Remote.t(),
          log_store_impl: ExRaft.LogStore.t(),
          statemachine_impl: ExRaft.Statemachine.t()
        }

  defstruct self: 0,
            remotes: %{},
            members_count: 0,
            term: 0,
            tick_delta: 100,
            local_tick: 0,
            election_tick: 0,
            heartbeat_tick: 0,
            apply_tick: 0,
            election_timeout: 10,
            randomized_election_timeout: 0,
            heartbeat_timeout: 2,
            apply_timeout: 2,
            voted_for: 0,
            leader_id: 0,
            last_index: 0,
            commit_index: 0,
            last_applied: 0,
            votes: %{},
            pending_config_change?: false,
            # --------------- read index --------------
            read_index_q: [],
            read_index_waiter: %{},
            read_index_ready: [],
            # --------------- read index end --------------
            remote_impl: nil,
            log_store_impl: nil,
            statemachine_impl: nil
end
