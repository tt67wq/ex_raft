defmodule ExRaft.MessageHandlers.Leader do
  @moduledoc false

  alias ExRaft.Config
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
      |> send_replicate_msg(peer)

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

  # ------------- private ----------------

  @spec make_replicate_msg(Models.Replica.t(), ReplicaState.t()) :: Typespecs.message_t() | nil
  defp make_replicate_msg(peer, state) do
    %Models.Replica{id: to_id, next: next} = peer
    %ReplicaState{self: id, log_store_impl: log_store_impl, term: term} = state
    {:ok, log_term} = LogStore.get_log_term(log_store_impl, next - 1)
    {:ok, entries} = LogStore.get_limit(log_store_impl, next - 1, Config.max_msg_batch_size())

    %Pb.Message{
      to: to_id,
      from: id,
      type: :append_entries,
      term: term,
      log_index: next - 1,
      log_term: log_term,
      entries: entries
    }
  end

  @spec send_replicate_msg(ReplicaState.t(), Models.Replica.t()) :: ReplicaState.t()
  defp send_replicate_msg(%ReplicaState{last_index: last_index} = state, %Models.Replica{match: match} = peer)
       when match < last_index do
    %Pb.Message{entries: entries} = msg = make_replicate_msg(peer, state)
    Common.send_msg(state, msg)

    entries
    |> List.last()
    |> case do
      # empty rpc
      nil ->
        state

      %Pb.Entry{index: index} ->
        {peer, true} = Models.Replica.make_progress(peer, index)
        Common.update_remote(state, peer)
    end
  end

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
