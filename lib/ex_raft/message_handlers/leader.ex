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

    state =
      state
      |> Common.update_remote(peer)
      |> Common.send_replicate_msg(peer)

    {:keep_state, state}
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
    %Pb.Message{from: from_id} = msg
    %ReplicaState{remotes: remotes} = state
    %Models.Replica{match: match} = peer = Map.fetch!(remotes, from_id)
    {peer, back?} = Models.Replica.make_rollback(peer, match)

    if back? do
      {:keep_state, Common.update_remote(state, peer)}
    else
      :keep_state_and_data
    end
  end

  def handle(%Pb.Message{type: :propose} = msg, state) do
    %Pb.Message{entries: entries} = msg
    %ReplicaState{term: term, last_index: last_index} = state
    entries = prepare_entries(entries, last_index, term)
    append_local_entries(state, entries)
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

  # fallback
  def handle(msg, _state) do
    Logger.warning("Unknown message, ignore, #{inspect(msg)}")
    :keep_state_and_data
  end

  # ---------------- private ----------------
  @spec prepare_entries(list(Typespecs.entry_t()), non_neg_integer(), non_neg_integer()) :: list(Typespecs.entry_t())
  defp prepare_entries(entries, last_index, term) do
    entries
    |> Enum.with_index(last_index + 1)
    |> Enum.map(fn {entry, index} ->
      %Pb.Entry{entry | term: term, index: index, type: :etype_normal}
    end)
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
