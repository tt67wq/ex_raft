defmodule ExRaft.MessageHandlers.Follower do
  @moduledoc false

  alias ExRaft.Core.Common
  alias ExRaft.LogStore
  alias ExRaft.Models
  alias ExRaft.Models.ReplicaState
  alias ExRaft.Pb
  alias ExRaft.Typespecs

  require Logger

  # heartbeat
  def handle(%Pb.Message{type: :heartbeat} = msg, state) do
    %Pb.Message{from: from_id, commit: commit} = msg
    %ReplicaState{self: id, term: term} = state

    Common.send_msg(state, %Pb.Message{
      type: :heartbeat_resp,
      to: from_id,
      from: id,
      term: term
    })

    state =
      state
      |> Common.commit_to(commit)
      |> Common.became_follower(term, from_id)

    {:keep_state, state}
  end

  def handle(%Pb.Message{type: :request_vote} = msg, state) do
    Common.handle_request_vote(msg, state)
  end

  def handle(%Pb.Message{type: :append_entries} = msg, state) do
    %Pb.Message{from: from_id, type: :append_entries} = msg
    %Models.ReplicaState{term: current_term} = state

    state =
      state
      |> do_append_entries(msg)
      |> Common.became_follower(current_term, from_id)

    {:keep_state, state}
  end

  def handle(%Pb.Message{type: :request_pre_vote} = msg, state) do
    Common.handle_request_pre_vote(msg, state)
  end

  def handle(msg, _state) do
    Logger.warning("Unknown message, ignore, #{inspect(msg)}")
    :keep_state_and_data
  end

  # --------------- private ----------------
  @spec do_append_entries(state :: ReplicaState.t(), req :: Typespecs.message_t()) :: state :: ReplicaState.t()
  defp do_append_entries(%ReplicaState{commit_index: commit_index} = state, %Pb.Message{log_index: log_index} = msg)
       when log_index < commit_index do
    %ReplicaState{self: id, term: term} = state
    %Pb.Message{from: from_id} = msg

    Common.send_msg(state, %Pb.Message{
      from: id,
      to: from_id,
      type: :append_entries_resp,
      term: term,
      log_index: commit_index,
      reject: true
    })

    state
  end

  defp do_append_entries(%ReplicaState{last_index: last_index} = state, %Pb.Message{log_index: log_index} = msg)
       when log_index <= last_index do
    %ReplicaState{term: term, log_store_impl: log_store_impl, self: id} = state
    %Pb.Message{log_term: log_term, entries: entries, commit: leader_commit, from: from_id} = msg

    log_index
    |> match_term?(log_term, log_store_impl)
    |> if do
      to_append_entries =
        entries
        |> get_conflict_index(log_store_impl)
        |> case do
          0 -> []
          conflict_index -> Enum.slice(entries, (conflict_index - log_index - 1)..-1//1)
        end

      {:ok, cnt} = LogStore.append_log_entries(log_store_impl, to_append_entries)

      Common.send_msg(state, %Pb.Message{
        from: id,
        to: from_id,
        type: :append_entries_resp,
        term: term,
        log_index: log_index + cnt,
        reject: false
      })

      {peer, _} =
        state
        |> Common.local_peer()
        |> Models.Replica.try_update(log_index + cnt)

      %ReplicaState{state | last_index: log_index + cnt}
      |> Common.commit_to(min(leader_commit, log_index + cnt))
      |> Common.update_remote(peer)
    else
      Common.send_msg(state, %Pb.Message{
        from: id,
        to: from_id,
        type: :append_entries_resp,
        term: term,
        log_index: log_index,
        reject: true
      })

      state
    end
  end

  defp do_append_entries(state, _req), do: state

  defp match_term?(0, 0, _log_store_impl), do: true

  defp match_term?(log_index, log_term, log_store_impl) do
    log_store_impl
    |> LogStore.get_log_entry(log_index)
    |> case do
      {:ok, nil} -> false
      {:ok, %Pb.Entry{term: tm}} -> tm == log_term
      {:error, e} -> raise e
    end
  end

  defp get_conflict_index(entries, log_store_impl) do
    entries
    |> Enum.find(fn %Pb.Entry{index: index, term: term} ->
      not match_term?(index, term, log_store_impl)
    end)
    |> case do
      %Pb.Entry{index: index} -> index
      nil -> 0
    end
  end
end
