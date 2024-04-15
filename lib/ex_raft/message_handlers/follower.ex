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
    %Pb.Message{from: from_id, commit: commit, ref: ref} = msg
    %ReplicaState{self: id, term: term} = state

    Common.send_msg(state, %Pb.Message{
      type: :heartbeat_resp,
      to: from_id,
      from: id,
      term: term,
      ref: ref
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
  @spec do_append_entries(state :: ReplicaState.t(), req :: Typespecs.message_t()) :: ReplicaState.t()
  defp do_append_entries(%ReplicaState{commit_index: commit_index} = state, %Pb.Message{log_index: log_index} = msg)
       when log_index < commit_index do
    Logger.warning("log index #{log_index} < commit index #{commit_index}")
    %ReplicaState{self: id, term: term} = state
    %Pb.Message{from: from_id} = msg

    Common.send_msg(state, %Pb.Message{
      from: id,
      to: from_id,
      type: :append_entries_resp,
      term: term,
      log_index: commit_index,
      reject: false
    })

    state
  end

  defp do_append_entries(%ReplicaState{last_index: last_index} = state, %Pb.Message{log_index: log_index} = msg)
       when log_index <= last_index do
    %ReplicaState{term: term, log_store_impl: log_store_impl, self: id} = state
    %Pb.Message{log_term: log_term, entries: entries, from: from_id} = msg

    log_index
    |> match_term?(log_term, log_store_impl)
    |> if do
      to_append_entries = get_to_append_entries(entries, log_store_impl)
      append_local_entries(state, msg, to_append_entries)
    else
      Logger.warning(
        "term mismatch: log index #{log_index}, log term: #{log_term}",
        ReplicaState.metadata(state)
      )

      Common.send_msg(state, %Pb.Message{
        from: id,
        to: from_id,
        type: :append_entries_resp,
        term: term,
        log_index: log_index,
        hint: last_index,
        reject: true
      })

      state
    end
  end

  defp do_append_entries(state, msg) do
    %ReplicaState{term: term, self: id, last_index: last_index} = state
    %Pb.Message{from: from_id, log_index: log_index} = msg
    Logger.warning("log_index #{log_index} > last index #{last_index}")

    Common.send_msg(state, %Pb.Message{
      from: id,
      to: from_id,
      type: :append_entries_resp,
      term: term,
      log_index: log_index,
      hint: last_index,
      reject: true
    })

    state
  end

  @spec append_local_entries(ReplicaState.t(), Typespecs.message_t(), list(Pb.Entry.t())) :: ReplicaState.t()
  defp append_local_entries(state, _msg, []), do: state

  defp append_local_entries(state, msg, to_append_entries) do
    %ReplicaState{log_store_impl: log_store_impl, self: id, term: term, last_index: last_index} = state
    %Pb.Message{from: from_id, commit: leader_commit} = msg
    Enum.each(to_append_entries, fn x -> Logger.debug("append entry: #{inspect(x)}", ReplicaState.metadata(state)) end)
    {:ok, cnt} = LogStore.append_log_entries(log_store_impl, to_append_entries)

    Common.send_msg(state, %Pb.Message{
      from: id,
      to: from_id,
      type: :append_entries_resp,
      term: term,
      log_index: last_index + cnt,
      reject: cnt == 0
    })

    {peer, _} =
      state
      |> Common.local_peer()
      |> Models.Replica.try_update(last_index + cnt)

    %ReplicaState{state | last_index: last_index + cnt}
    |> Common.commit_to(min(leader_commit, last_index + cnt))
    |> Common.update_remote(peer)
  end

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

  defp get_to_append_entries([], _log_store_impl), do: []

  defp get_to_append_entries(entries, log_store_impl) do
    [he | _] = entries
    %Pb.Entry{index: index} = he
    {:ok, local_entries} = LogStore.get_limit(log_store_impl, index - 1, Enum.count(entries))

    match_entries(entries, local_entries)
  end

  defp match_entries([], _), do: []
  defp match_entries(entries, []), do: entries

  defp match_entries([e1 | h1], [e2 | h2]) do
    %Pb.Entry{index: i1, term: t1} = e1
    %Pb.Entry{index: i2, term: t2} = e2

    if i1 == i2 and t1 == t2 do
      match_entries(h1, h2)
    else
      raise ExRaft.Exception, message: "entry mismatch", details: %{"entry1" => e1, "entry2" => e2}
    end
  end
end
