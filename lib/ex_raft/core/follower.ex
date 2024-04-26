defmodule ExRaft.Core.Follower do
  @moduledoc """
  Follower Role Module

  Handle :gen_statm callbacks for follower role
  """
  import ExRaft.Guards

  alias ExRaft.Core.Common
  alias ExRaft.LogStore
  alias ExRaft.MessageHandlers
  alias ExRaft.Models.ReplicaState
  alias ExRaft.Pb
  alias ExRaft.Statemachine
  alias ExRaft.Utils

  require Logger

  def follower(:enter, _old_state, %ReplicaState{self: id}) do
    Logger.info("Replica #{id} become follower")
    :keep_state_and_data
  end

  def follower({:timeout, :tick}, _, state) do
    if Common.self_removed?(state) do
      Logger.warning("This replica has been removed")
      :keep_state_and_data
    else
      tick(state)
    end
  end

  # -------------------- pipein msg handle --------------------
  # term mismatch
  def follower(:cast, {:pipein, %Pb.Message{term: term} = msg}, %ReplicaState{term: current_term} = state)
      when term != current_term do
    Common.handle_term_mismatch(:follower, msg, state)
  end

  # msg from non-exists peer
  def follower(:cast, {:pipein, %Pb.Message{from: from} = msg}, state) when not_empty(from) do
    %ReplicaState{remotes: remotes} = state

    remotes
    |> Map.has_key?(from)
    |> if do
      MessageHandlers.Follower.handle(msg, state)
    else
      Logger.warning("Receive msg from non-exists peer: #{inspect(msg)}")
      :keep_state_and_data
    end
  end

  def follower(:cast, {:pipein, msg}, state) do
    MessageHandlers.Follower.handle(msg, state)
  end

  # ---------------- propose ----------------
  def follower(:cast, {:propose, entries}, state) do
    %ReplicaState{self: id, term: term, leader_id: leader_id} = state

    msg = %Pb.Message{
      type: :propose,
      term: term,
      from: id,
      to: leader_id,
      entries: entries
    }

    Common.send_msg(state, msg)

    :keep_state_and_data
  end

  # ---------------- config change ----------------
  def follower(:cast, {:config_change, entries}, state) do
    %ReplicaState{self: id, term: term, leader_id: leader_id} = state

    msg = %Pb.Message{
      type: :config_change,
      term: term,
      from: id,
      to: leader_id,
      entries: entries
    }

    Common.send_msg(state, msg)

    :keep_state_and_data
  end

  # ------------------ call event handler ------------------
  def follower({:call, from}, :show, state) do
    :gen_statem.reply(from, {:ok, %{role: :follower, state: state}})
    :keep_state_and_data
  end

  def follower({:call, from}, :read_index, %ReplicaState{leader_id: 0}) do
    :gen_statem.reply(from, {:error, :no_leader})
    :keep_state_and_data
  end

  def follower({:call, from}, :read_index, state) do
    %ReplicaState{req_register: rr, self: id, leader_id: leader_id, term: term} = state
    ref = make_ref()
    :ok = Utils.ReqRegister.register_req(rr, ref, from)

    msg = %Pb.Message{
      type: :read_index,
      from: id,
      to: leader_id,
      ref: :erlang.term_to_binary(ref),
      term: term
    }

    Common.send_msg(state, msg)

    :keep_state_and_data
  end

  # -------------------- internal --------------------
  def follower(:internal, :save_snapshot, state) do
    %ReplicaState{statemachine_impl: statemachine_impl, log_store_impl: log_store_impl} = state

    %Pb.SnapshotMetadata{filepath: fp, index: index} = sm = Common.create_snapshot_metadata(state)

    sm_bin = Pb.SnapshotMetadata.encode(sm)
    sm_bin_prefix = Utils.Uvaint.encode(byte_size(sm_bin))

    {:ok, _} =
      File.open(fp, [:write, :binary], fn file ->
        IO.write(file, sm_bin_prefix <> sm_bin)
        Statemachine.save_snapshot(statemachine_impl, file)
      end)

    :ok = LogStore.truncate_before(log_store_impl, index)

    :keep_state_and_data
  end

  # ------------------ fallback -----------------

  def follower(event, data, state) do
    Logger.debug(%{
      role: :follower,
      event: event,
      data: data,
      state: state
    })

    :keep_state_and_data
  end

  # ----------------- tick ---------------
  defp tick(%ReplicaState{election_tick: election_tick, randomized_election_timeout: election_timeout} = state)
       when election_tick + 1 >= election_timeout do
    {:next_state, :prevote, Common.became_prevote(state), Common.tick_action(state)}
  end

  defp tick(%ReplicaState{apply_tick: apply_tick, apply_timeout: apply_timeout} = state)
       when apply_tick + 1 > apply_timeout do
    state = Common.apply_to_statemachine(state)
    {:keep_state, Common.tick(state, false), Common.tick_action(state)}
  end

  defp tick(state) do
    {:keep_state, Common.tick(state, false), Common.tick_action(state)}
  end
end
