defmodule ExRaft.Roles.Leader do
  @moduledoc """
  Leader Role Module

  Handle :gen_statm callbacks for leader role
  """
  alias ExRaft.Config
  alias ExRaft.LogStore
  alias ExRaft.Models
  alias ExRaft.Models.ReplicaState
  alias ExRaft.Pb
  alias ExRaft.Roles.Common
  alias ExRaft.Typespecs

  require Logger

  def leader(:enter, _old_state, _state) do
    :keep_state_and_data
  end

  def leader(
        {:timeout, :tick},
        _,
        %ReplicaState{heartbeat_tick: heartbeat_tick, heartbeat_timeout: heartbeat_timeout} = state
      )
      when heartbeat_tick + 1 > heartbeat_timeout do
    %ReplicaState{
      self: id,
      term: term,
      remotes: remotes,
      commit_index: commit_index
    } = state

    ms =
      remotes
      |> Enum.reject(fn {to_id, _} -> to_id == id end)
      |> Enum.map(fn {to_id, %Models.Replica{match: match}} ->
        %Pb.Message{
          type: :heartbeat,
          to: to_id,
          from: id,
          term: term,
          commit: min(match, commit_index)
        }
      end)

    Common.send_msgs(state, ms)

    state =
      state
      |> Common.reset(term, true)
      |> Common.tick(true)

    {:keep_state, state, Common.tick_action(state)}
  end

  def leader({:timeout, :tick}, _, %ReplicaState{apply_tick: apply_tick, apply_timeout: apply_timeout} = state)
      when apply_tick + 1 > apply_timeout do
    state =
      state
      |> Common.apply_to_statemachine()
      |> Common.tick(true)

    {:keep_state, state, Common.tick_action(state)}
  end

  def leader({:timeout, :tick}, _, %ReplicaState{} = state) do
    {:keep_state, Common.tick(state, true), Common.tick_action(state)}
  end

  # -------------------- pipein msg handle --------------------
  # on term mismatch
  def leader(:cast, {:pipein, %Pb.Message{term: term} = msg}, %ReplicaState{term: current_term} = state)
      when term != current_term do
    Common.handle_term_mismatch(false, msg, state)
  end

  def leader(:cast, {:pipein, %Pb.Message{type: :request_vote} = msg}, state) do
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

  def leader(:cast, {:pipein, %Pb.Message{type: :heartbeat_resp} = msg}, state) do
    %ReplicaState{remotes: remotes, last_log_index: last_index} = state
    %Pb.Message{from: from_id} = msg
    %Models.Replica{match: match} = peer = Map.fetch!(remotes, from_id)

    if last_index > match do
      # send log replica msg
      peer
      |> make_replicate_msg(state)
      |> case do
        nil ->
          :keep_state_and_data

        %Pb.Message{entries: entries} = msg ->
          Common.send_msg(state, msg)

          %Pb.Entry{index: index} =
            List.last(entries)

          {peer, _} = Models.Replica.make_progress(peer, index)
          {:keep_state, Common.update_remote(state, peer)}
      end
    else
      :keep_state_and_data
    end
  end

  def leader(:cast, {:pipein, %Pb.Message{type: :append_entries_resp, reject: false} = msg}, state) do
    %Pb.Message{from: from_id, log_index: log_index} = msg
    %ReplicaState{remotes: remotes} = state
    %{^from_id => peer} = remotes
    {peer, updated?} = Models.Replica.try_update(peer, log_index)
    state = %ReplicaState{state | remotes: Map.put(remotes, from_id, peer)}

    if updated? do
      {:keep_state, state, [{:next_event, :internal, :commit}]}
    else
      {:keep_state, state}
    end
  end

  def leader(:cast, {:pipein, %Pb.Message{type: :append_entries_resp, reject: true}}, _) do
    # TODO: handle this
    :keep_state_and_data
  end

  def leader(:cast, {:pipein, %Pb.Message{type: :propose} = msg}, state) do
    %Pb.Message{entries: entries} = msg
    %ReplicaState{term: term, last_log_index: last_log_index, log_store_impl: log_store_impl} = state

    entries =
      entries
      |> Enum.with_index(last_log_index + 1)
      |> Enum.map(fn {entry, index} ->
        %Pb.Entry{entry | term: term, index: index}
      end)

    {:ok, cnt} = LogStore.append_log_entries(log_store_impl, entries)

    {peer, _} =
      state
      |> Common.local_peer()
      |> Models.Replica.try_update(last_log_index + cnt)

    state = Common.update_remote(state, peer)
    # [{:next_event, :internal, :broadcast_replica}]
    {:keep_state, %ReplicaState{state | last_log_index: last_log_index + cnt}}
  end

  # other pipein, ignore
  def leader(:cast, {:pipein, msg}, _state) do
    Logger.warning("Unknown message, ignore, #{inspect(msg)}")
    :keep_state_and_data
  end

  # ---------------- propose ----------------

  def leader(:cast, {:propose, entries}, state) do
    %ReplicaState{self: id, term: term} = state

    msg = %Pb.Message{
      type: :propose,
      term: term,
      from: id,
      entries: entries
    }

    {:keep_state_and_data, Common.cast_action(msg)}
  end

  # ------------------ internal event handler ------------------

  def leader(:internal, :commit, state) do
    # get middle index
    %ReplicaState{
      remotes: remotes,
      commit_index: commit_index,
      log_store_impl: log_store_impl,
      term: current_term
    } = state

    {_, %Models.Replica{match: match}} =
      remotes
      |> Enum.sort()
      |> Enum.at(Common.quorum(state) - 1)

    {:ok, term} = LogStore.get_log_term(log_store_impl, match)

    if match > commit_index and term == current_term do
      {:keep_state, Common.commit_to(state, match)}
    else
      :keep_state_and_data
    end
  end

  # ------------------ call event handler ------------------
  def leader({:call, from}, :show, state) do
    :gen_statem.reply(from, {:ok, %{role: :leader, state: state}})
    :keep_state_and_data
  end

  # --------------------- fallback -----------------

  def leader(event, data, state) do
    ExRaft.Debug.stacktrace(%{
      event: event,
      data: data,
      state: state
    })

    :keep_state_and_data
  end

  # ----------------- make replicate msgs ---------------
  @spec make_replicate_msg(Models.Replica.t(), ReplicaState.t()) :: Typespecs.message_t() | nil
  defp make_replicate_msg(peer, state) do
    %Models.Replica{id: to_id, next: next} = peer
    %ReplicaState{self: id, log_store_impl: log_store_impl, term: term} = state
    {:ok, log_term} = LogStore.get_log_term(log_store_impl, next - 1)
    {:ok, entries} = LogStore.get_limit(log_store_impl, next - 1, Config.max_msg_batch_size())

    case entries do
      [] ->
        nil

      _ ->
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
  end
end
