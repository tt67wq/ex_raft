defmodule ExRaft.MessageHandlers.Leader do
  @moduledoc false

  alias ExRaft.Core.Common
  alias ExRaft.LogStore
  alias ExRaft.Models
  alias ExRaft.Models.ReplicaState
  alias ExRaft.Pb
  alias ExRaft.Typespecs

  require Logger

  def handle(%Pb.Message{type: :request_vote} = msg, state) do
    %Pb.Message{from: from_id} = msg
    %ReplicaState{self: id, term: current_term} = state

    Common.send_msg(state, %Pb.Message{
      type: :request_vote_resp,
      to: from_id,
      from: id,
      term: current_term,
      reject: true
    })

    :keep_state_and_data
  end

  def handle(%Pb.Message{type: :heartbeat_resp} = msg, state) do
    %ReplicaState{remotes: remotes} = state
    %Pb.Message{from: from_id} = msg
    peer = remotes |> Map.fetch!(from_id) |> Models.Replica.set_active()

    {read_index_updated?, ref, state} =
      state
      |> Common.update_remote(peer)
      |> Common.send_replicate_msg(peer)
      |> Common.may_read_index_confirm(msg)

    if read_index_updated? and Common.read_index_check_quorum_pass?(state, ref) do
      {to_pops, state} = Common.pop_all_ready_read_index_status(state, ref)

      to_pops
      |> Task.async_stream(fn status -> Common.response_read_index_req(status, state) end)
      |> Stream.run()

      {:keep_state, state}
    else
      {:keep_state, state}
    end
  end

  def handle(%Pb.Message{type: :append_entries_resp, reject: false} = msg, state) do
    %Pb.Message{from: from_id, log_index: log_index} = msg
    %ReplicaState{remotes: remotes} = state
    %{^from_id => peer} = remotes

    {peer, updated?} =
      peer
      |> Models.Replica.set_active()
      |> Models.Replica.try_update(log_index)

    state = Common.update_remote(state, peer)

    if updated? do
      {:keep_state, state, [{:next_event, :internal, :commit}]}
    else
      {:keep_state, state}
    end
  end

  def handle(%Pb.Message{type: :append_entries_resp, reject: true} = msg, state) do
    Logger.warning("reject append entries, #{inspect(msg)}")
    %Pb.Message{from: from_id, hint: low} = msg
    %ReplicaState{remotes: remotes} = state
    %Models.Replica{match: match} = peer = Map.fetch!(remotes, from_id)
    {peer, back?} = Models.Replica.make_rollback(peer, min(low, match))

    if back? do
      {:keep_state, Common.update_remote(state, peer)}
    else
      :keep_state_and_data
    end
  end

  def handle(%Pb.Message{type: :propose} = msg, state) do
    %Pb.Message{entries: entries} = msg
    %ReplicaState{term: term, last_index: last_index, pending_config_change?: pending_config_change?} = state
    {entries, has_config_change?} = prepare_entries(entries, last_index, term, pending_config_change?)
    append_local_entries(%ReplicaState{state | pending_config_change?: has_config_change?}, entries)
  end

  def handle(%Pb.Message{type: :config_change} = msg, state) do
    %Pb.Message{entries: [entry]} = msg
    %ReplicaState{term: term, last_index: last_index} = state
    entry = %Pb.Entry{entry | term: term, index: last_index + 1, type: :etype_config_change}
    append_local_entries(state, [entry])
  end

  def handle(%Pb.Message{type: :request_pre_vote} = msg, state) do
    Common.handle_request_pre_vote(msg, state)
  end

  def handle(%Pb.Message{type: :read_index}, %ReplicaState{leader_id: 0}) do
    Logger.warning("no leader, ignore read_index request")
    :keep_state_and_data
  end

  def handle(%Pb.Message{type: :read_index} = msg, state) do
    %Pb.Message{from: from, ref: ref_bin} = msg

    ref = :erlang.binary_to_term(ref_bin)

    state
    |> Common.has_commited_entry_at_current_term?()
    |> if do
      state =
        state
        |> Common.add_read_index_req(ref, from)
        |> Common.broadcast_heartbeat_with_read_index(ref)

      {:keep_state, state}
    else
      Logger.warning("no commited entry at current term, ignore, #{inspect(msg)}")
      :keep_state_and_data
    end
  end

  # fallback
  def handle(msg, _state) do
    Logger.warning("Unknown message, ignore, #{inspect(msg)}")
    :keep_state_and_data
  end

  # ---------------- private ----------------
  @spec prepare_entries(
          entries :: list(Typespecs.entry_t()),
          last_index :: Typespecs.index_t(),
          term :: Typespecs.term_t(),
          pending_config_change? :: boolean()
        ) ::
          {entries :: list(Typespecs.entry_t()), has_config_change? :: boolean()}

  # p33-35 of the raft thesis describes a simple membership change protocol which
  # requires only one node can be added or removed at a time. its safety is
  # guarded by the fact that when there is only one node to be added or removed
  # at a time, the old and new quorum are guaranteed to overlap.
  # the protocol described in the raft thesis requires the membership change
  # entry to be executed as soon as it is appended. this also introduces an extra
  # troublesome step to roll back to an old membership configuration when
  # necessary.
  # similar to etcd raft, we treat such membership change entry as regular
  # entries that are only executed after being committed (by the old quorum).
  # to do that, however, we need to further restrict the leader to only has at
  # most one pending not applied membership change entry in its log. this is to
  # avoid the situation that two pending membership change entries are committed
  # in one go with the same quorum while they actually require different quorums.
  # consider the following situation -
  # for a 3 nodes cluster with existing members X, Y and Z, let's say we first
  # propose a membership change to add a new node A, before A gets committed and
  # applied, say we propose another membership change to add a new node B. When
  # B gets committed, A will be committed as well, both will be using the 3 node
  # membership quorum meaning both entries concerning A and B will become
  # committed when any two of the X, Y, Z cluster have them replicated. this thus
  # violates the safety requirement as B will require 3 out of the 4 nodes (X,
  # Y, Z, A) to have it replicated before it can be committed.
  # we use the following pendingConfigChange flag to help tracking whether there
  # is already a pending membership change entry in the log waiting to be
  # executed.
  defp prepare_entries(entries, last_index, term, pending_config_change?) do
    {entries, has_config_change?} =
      entries
      |> Enum.with_index(last_index + 1)
      |> Enum.reduce(
        {[], pending_config_change?},
        fn
          {%Pb.Entry{type: :etype_config_change} = entry, index}, {entries, false} ->
            {
              [%Pb.Entry{entry | term: term, index: index} | entries],
              true
            }

          {%Pb.Entry{type: :etype_config_change} = entry, index}, {entries, true} ->
            Logger.warning("drop config change entry, has pending config change, #{inspect(entry)}")

            {
              [%Pb.Entry{entry | term: term, index: index, type: :etype_no_op} | entries],
              true
            }

          {entry, index}, {entries, has_config_change?} ->
            {
              [%Pb.Entry{entry | term: term, index: index, type: :etype_normal} | entries],
              has_config_change?
            }
        end
      )

    {Enum.reverse(entries), has_config_change?}
  end

  defp append_local_entries(%ReplicaState{last_index: last_index, log_store_impl: log_store_impl} = state, entries) do
    {:ok, cnt} = LogStore.append_log_entries(log_store_impl, entries)

    {peer, updated?} =
      state
      |> Common.local_peer()
      |> Models.Replica.try_update(last_index + cnt)

    state = Common.update_remote(state, peer)

    if updated? do
      {:keep_state, %ReplicaState{state | last_index: last_index + cnt}, [{:next_event, :internal, :broadcast_replica}]}
    else
      {:keep_state, %ReplicaState{state | last_index: last_index + cnt}}
    end
  end
end
