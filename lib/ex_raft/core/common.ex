defmodule ExRaft.Core.Common do
  @moduledoc """
  Common behavior for all roles
  """
  import ExRaft.Guards

  alias ExRaft.Config
  alias ExRaft.LogStore
  alias ExRaft.Models
  alias ExRaft.Models.ReplicaState
  alias ExRaft.Pb
  alias ExRaft.Remote
  alias ExRaft.Statemachine
  alias ExRaft.Typespecs
  alias ExRaft.Utils

  require Logger

  # term equal situation
  def handle_request_pre_vote(req, state) do
    %Pb.Message{from: from_id} = req
    %ReplicaState{term: current_term, self: id} = state

    send_msg(state, %Pb.Message{
      type: :request_pre_vote_resp,
      to: from_id,
      from: id,
      term: current_term,
      reject: true
    })

    :keep_state_and_data
  end

  # term equal situation
  def handle_request_vote(req, state) do
    %Pb.Message{from: from_id} = req
    %ReplicaState{log_store_impl: log_store_impl, self: id, term: current_term} = state
    {:ok, last_log} = LogStore.get_last_log_entry(log_store_impl)

    if can_vote?(req, state) and log_updated?(req, last_log) do
      send_msg(state, %Pb.Message{
        type: :request_vote_resp,
        to: from_id,
        from: id,
        term: current_term,
        reject: false
      })

      state =
        state
        |> vote_for(from_id)
        |> reset(current_term)

      {:keep_state, state}
    else
      send_msg(state, %Pb.Message{
        type: :request_vote_resp,
        to: from_id,
        from: id,
        term: current_term,
        reject: true
      })

      :keep_state_and_data
    end
  end

  def can_vote?(%Pb.Message{from: cid}, %ReplicaState{voted_for: voted_for}) do
    voted_for in [cid, 0]
  end

  def can_vote?(_, _), do: false

  def log_updated?(_req, nil), do: true

  def log_updated?(%Pb.Message{log_term: req_last_log_term}, %Pb.Entry{term: last_log_term})
      when req_last_log_term > last_log_term,
      do: true

  def log_updated?(%Pb.Message{log_index: req_last_index, log_term: req_last_log_term}, %Pb.Entry{
        term: last_log_term,
        index: last_index
      })
      when req_last_log_term == last_log_term and req_last_index >= last_index,
      do: true

  def log_updated?(_, _), do: false

  def cast_pipein(msg), do: [{:next_event, :cast, {:pipein, msg}}]

  # --------------------- term_mismatch ------------------------

  @spec handle_term_mismatch(
          role :: Typespecs.role_t(),
          msg :: Pb.Message.t(),
          state :: ReplicaState.t()
        ) :: any()
  def handle_term_mismatch(_, %Pb.Message{type: req_type, term: term}, %ReplicaState{
        term: current_term,
        leader_id: leader_id,
        election_tick: election_tick,
        election_timeout: election_timeout
      })
      when request_vote_req?(req_type) and term > current_term and leader_id != 0 and election_tick < election_timeout do
    # we got a RequestVote with higher term, but we recently had heartbeat msg
    # from leader within the minimum election timeout and that leader is known
    # to have quorum. we thus drop such RequestVote to minimize interruption by
    # network partitioned nodes with higher term.
    # this idea is from the last paragraph of the section 6 of the raft paper
    :keep_state_and_data
  end

  def handle_term_mismatch(
        _,
        %Pb.Message{type: :request_pre_vote, term: term} = msg,
        %ReplicaState{term: current_term} = state
      )
      when term > current_term do
    %Pb.Message{from: from_id} = msg
    %ReplicaState{log_store_impl: log_store_impl, self: id} = state
    {:ok, last_log} = LogStore.get_last_log_entry(log_store_impl)

    if log_updated?(msg, last_log) do
      send_msg(state, %Pb.Message{
        type: :request_pre_vote_resp,
        to: from_id,
        from: id,
        term: term,
        reject: false
      })
    else
      Logger.warning("Reject request_pre_vote from #{from_id}, log not updated")

      send_msg(state, %Pb.Message{
        type: :request_pre_vote_resp,
        to: from_id,
        from: id,
        term: current_term,
        reject: true
      })
    end

    :keep_state_and_data
  end

  def handle_term_mismatch(
        :prevote,
        %Pb.Message{type: :request_pre_vote_resp, reject: false, term: term} = msg,
        %ReplicaState{term: current_term} = state
      )
      when term > current_term do
    %Pb.Message{from: from_id} = msg
    %ReplicaState{votes: votes} = state
    votes = Map.put_new(votes, from_id, false)
    state = %ReplicaState{state | votes: votes}

    if vote_quorum_pass?(state) do
      {:next_state, :candidate, became_candidate(state)}
    else
      {:keep_state, state}
    end
  end

  def handle_term_mismatch(_, %Pb.Message{type: :request_pre_vote_resp, reject: false, term: term}, %ReplicaState{
        term: current_term
      })
      when term > current_term do
    # ignore rejected request_pre_vote_resp
    :keep_state_and_data
  end

  def handle_term_mismatch(_, %Pb.Message{type: :request_pre_vote_resp, reject: true, term: term}, %ReplicaState{
        term: current_term
      })
      when term > current_term do
    # ignore rejected request_pre_vote_resp
    :keep_state_and_data
  end

  def handle_term_mismatch(
        role,
        %Pb.Message{type: :requst_vote, term: term} = msg,
        %ReplicaState{term: current_term} = state
      )
      when term > current_term do
    # not to reset the electionTick value to avoid the risk of having the
    # local node not being to campaign at all. if the local node generates
    # the tick much slower than other nodes (e.g. bad config, hardware
    # clock issue, bad scheduling, overloaded etc), it may lose the chance
    # to ever start a campaign unless we keep its electionTick value here.
    if role == :follower do
      {:keep_state, became_follower_keep_election(state, term, 0), cast_pipein(msg)}
    else
      {:next_state, :follower, became_follower_keep_election(state, term, 0), cast_pipein(msg)}
    end
  end

  def handle_term_mismatch(role, %Pb.Message{term: term} = msg, %ReplicaState{term: current_term} = state)
      when term > current_term do
    %Pb.Message{from: from_id, type: msg_type} = msg
    leader_id = (leader_message?(msg) && from_id) || 0
    Logger.info("Receive higher term #{term} from #{from_id}, set leader to #{leader_id}, msg_type is #{msg_type}")

    if role == :follower do
      {:keep_state, became_follower(state, term, leader_id), cast_pipein(msg)}
    else
      {:next_state, :follower, became_follower(state, term, leader_id), cast_pipein(msg)}
    end
  end

  def handle_term_mismatch(_, %Pb.Message{term: term}, %ReplicaState{term: current_term}) when term < current_term,
    do: :keep_state_and_data

  defp leader_message?(%Pb.Message{type: type}) do
    type in [:append_entries, :heartbeat]
  end

  # --------------------- term_mismatch end ------------------------

  defp gen_election_timeout(timeout), do: Enum.random(timeout..(2 * timeout))

  def reset(state, term) do
    state
    |> set_term(term)
    |> reset_election_tick()
  end

  defp set_term(%ReplicaState{term: current_term} = state, term) when term != current_term do
    %ReplicaState{state | term: term, voted_for: 0}
  end

  defp set_term(state, _term), do: state

  defp reset_election_tick(state) do
    %ReplicaState{election_timeout: election_timeout} = state
    %ReplicaState{state | election_tick: 0, randomized_election_timeout: gen_election_timeout(election_timeout)}
  end

  def reset_heartbeat(state), do: %ReplicaState{state | heartbeat_tick: 0}

  @spec tick(state :: ReplicaState.t(), leader? :: bool()) :: ReplicaState.t()
  def tick(state, false) do
    %ReplicaState{local_tick: local_tick, election_tick: election_tick, apply_tick: apply_tick} = state

    %ReplicaState{
      state
      | local_tick: local_tick + 1,
        apply_tick: apply_tick + 1,
        election_tick: election_tick + 1
    }
  end

  def tick(state, true) do
    %ReplicaState{
      local_tick: local_tick,
      apply_tick: apply_tick,
      election_tick: election_tick,
      heartbeat_tick: heartbeat_tick
    } = state

    %ReplicaState{
      state
      | local_tick: local_tick + 1,
        apply_tick: apply_tick + 1,
        election_tick: election_tick + 1,
        heartbeat_tick: heartbeat_tick + 1
    }
  end

  def tick_action(%ReplicaState{tick_delta: tick_delta}), do: [{{:timeout, :tick}, tick_delta, nil}]

  def campaign(%ReplicaState{term: term, self: id} = state) do
    state
    |> reset(term + 1)
    |> vote_for(id)
    |> reset_votes()
    |> tick(false)
  end

  def prevote_campaign(%ReplicaState{term: term} = state) do
    state
    |> reset(term)
    |> reset_votes()
    |> tick(false)
  end

  defp reset_votes(state) do
    %ReplicaState{self: id} = state

    Map.put(state, :votes, %{id => false})
  end

  defp set_leader_id(%ReplicaState{leader_id: leader_id} = state, id) when leader_id != id do
    Logger.info("Leader changed from #{leader_id} to #{id}")
    %ReplicaState{state | leader_id: id}
  end

  defp set_leader_id(state, _id), do: state

  def became_leader(%ReplicaState{self: id, term: term} = state) do
    state
    |> reset(term)
    |> reset_heartbeat()
    |> set_leader_id(id)
    |> set_all_remotes_inactive()
    |> set_pending_config_change()
    |> add_no_op_entry()
  end

  def became_prevote(%ReplicaState{term: term} = state) do
    state
    |> reset(term)
    |> set_leader_id(0)
    |> reset_votes()
  end

  def became_candidate(%ReplicaState{term: term} = state) do
    state
    |> reset(term)
    |> set_leader_id(0)
  end

  def became_follower(%ReplicaState{} = state, term, leader_id) do
    state
    |> reset(term)
    |> set_leader_id(leader_id)
  end

  def became_follower_keep_election(%ReplicaState{} = state, term, leader_id) do
    state
    |> set_term(term)
    |> set_leader_id(leader_id)
  end

  @spec vote_for(state :: ReplicaState.t(), vote_for_id :: Typespecs.replica_id_t()) :: ReplicaState.t()
  def vote_for(state, vote_for_id) do
    %ReplicaState{state | voted_for: vote_for_id}
  end

  defp set_pending_config_change(%ReplicaState{commit_index: commit_index} = state) do
    state
    |> get_pending_config_change_count(commit_index, 0)
    |> Kernel.>(0)
    |> if do
      %ReplicaState{state | pending_config_change?: true}
    else
      state
    end
  end

  defp unset_pending_config_change(state) do
    %ReplicaState{state | pending_config_change?: false}
  end

  @spec get_pending_config_change_count(ReplicaState.t(), Typespecs.index_t(), non_neg_integer()) :: non_neg_integer()
  defp get_pending_config_change_count(%ReplicaState{log_store_impl: log_store_impl} = state, since, count) do
    {:ok, entries} = LogStore.get_limit(log_store_impl, since, Config.max_msg_batch_size())

    if Enum.empty?(entries) do
      count
    else
      cc_count = Enum.count(entries, fn %Pb.Entry{type: x} -> x == :etype_config_change end)
      %Pb.Entry{index: index} = List.last(entries)
      get_pending_config_change_count(state, index, count + cc_count)
    end
  end

  def vote_quorum_pass?(%ReplicaState{votes: votes, members_count: members_count}) do
    votes
    |> Enum.count(fn {_, rejected?} -> not rejected? end)
    |> Kernel.*(2)
    |> Kernel.>(members_count)
  end

  @spec send_msgs(ReplicaState.t(), [Pb.Message.t()]) :: :ok | {:error, ExRaft.Exception.t()}
  def send_msgs(%ReplicaState{remote_impl: remote_impl}, msgs), do: Remote.pipeout(remote_impl, msgs)

  def send_msg(%ReplicaState{} = state, %Pb.Message{} = msg) do
    send_msgs(state, [msg])
  end

  defp apply_index(%ReplicaState{last_applied: last_applied} = state, index) when last_applied == index do
    %ReplicaState{state | apply_tick: 0}
  end

  defp apply_index(%ReplicaState{last_applied: last_applied} = state, index) when last_applied < index do
    %ReplicaState{state | last_applied: index, apply_tick: 0}
  end

  @spec apply_to_statemachine(ReplicaState.t()) :: ReplicaState.t()
  def apply_to_statemachine(%ReplicaState{commit_index: commit_index, last_applied: last_applied} = state)
      when commit_index == last_applied,
      do: %ReplicaState{state | apply_tick: 0}

  def apply_to_statemachine(%ReplicaState{commit_index: commit_index, last_applied: last_applied} = state)
      when commit_index > last_applied do
    %ReplicaState{log_store_impl: log_store_impl, statemachine_impl: statemachine_impl} = state

    max_limit = Config.max_msg_batch_size()
    limit = (commit_index - last_applied > max_limit && max_limit) || commit_index - last_applied

    log_store_impl
    |> LogStore.get_limit(last_applied, limit)
    |> case do
      {:ok, []} ->
        %ReplicaState{state | apply_tick: 0}

      {:ok, logs} ->
        log_group = Enum.group_by(logs, & &1.type)

        :ok = Statemachine.update(statemachine_impl, Map.get(log_group, :etype_normal, []))
        %Pb.Entry{index: index} = List.last(logs)

        state
        |> apply_config_change(Map.get(log_group, :etype_config_change, []))
        |> unset_pending_config_change()
        |> apply_index(index)
    end
  end

  def apply_to_statemachine(_, state), do: state

  @spec local_peer(ReplicaState.t()) :: Models.Replica.t()
  def local_peer(state) do
    %ReplicaState{remotes: remotes, self: id} = state
    %{^id => peer} = remotes
    peer
  end

  @spec connect_all_remotes(ReplicaState.t()) :: ReplicaState.t()
  def connect_all_remotes(%ReplicaState{remotes: remotes, remote_impl: remote_impl, self: self_id} = state) do
    Enum.each(remotes, fn
      {id, peer} when id != self_id -> Remote.connect(remote_impl, peer)
      _ -> nil
    end)

    state
  end

  @spec commit_to(ReplicaState.t(), Typespecs.index_t()) :: ReplicaState.t()
  def commit_to(%ReplicaState{commit_index: commit_index, last_index: last_index} = state, index)
      when index > commit_index and index <= last_index do
    %ReplicaState{state | commit_index: index}
  end

  def commit_to(state, _), do: state

  @spec update_remote(ReplicaState.t(), Models.Replica.t()) :: ReplicaState.t()
  def update_remote(%ReplicaState{remotes: remotes} = state, peer) do
    %Models.Replica{id: id} = peer
    remotes = Map.replace(remotes, id, peer)
    %ReplicaState{state | remotes: remotes}
  end

  @spec add_remote(ReplicaState.t(), Models.Replica.t()) :: ReplicaState.t()
  def add_remote(%ReplicaState{remotes: remotes} = state, peer) do
    %Models.Replica{id: id} = peer
    remotes = Map.put_new(remotes, id, peer)
    %ReplicaState{state | remotes: remotes, members_count: Enum.count(remotes)}
  end

  def quorum(%ReplicaState{members_count: members_count}) do
    members_count
    |> div(2)
    |> Kernel.+(1)
  end

  def set_all_remotes_inactive(%ReplicaState{remotes: remotes} = state) do
    remotes = Map.new(remotes, fn {id, peer} -> {id, Models.Replica.set_inactive(peer)} end)
    %ReplicaState{state | remotes: remotes}
  end

  @spec self_removed?(ReplicaState.t()) :: boolean()
  def self_removed?(%ReplicaState{remotes: remotes, self: id}) do
    remotes
    |> Enum.any?(fn {x, _} -> x == id end)
    |> Kernel.not()
  end

  defp apply_config_change(state, entries) do
    config_change_cmds = Enum.map(entries, fn %Pb.Entry{cmd: cmd} -> Pb.ConfigChange.decode(cmd) end)
    Enum.reduce(config_change_cmds, state, &apply_one_config_change_entry/2)
  end

  @spec apply_one_config_change_entry(Pb.ConfigChange.t(), ReplicaState.t()) :: ReplicaState.t()
  defp apply_one_config_change_entry(%Pb.ConfigChange{type: :cctype_add_node} = cmd, state) do
    Logger.info("apply_config_change: #{inspect(cmd)}")
    %Pb.ConfigChange{replica_id: id, addr: addr} = cmd
    %ReplicaState{self: self_id, remote_impl: remote_impl, remotes: remotes, last_index: last_index} = state

    if Map.has_key?(remotes, id) do
      state
    else
      remote = %Models.Replica{
        id: id,
        host: addr,
        next: last_index + 1
      }

      if self_id != id do
        Remote.connect(remote_impl, remote)
      end

      add_remote(state, remote)
    end
  end

  defp apply_one_config_change_entry(%Pb.ConfigChange{type: :cctype_remove_node} = cmd, state) do
    Logger.info("apply_config_change: #{inspect(cmd)}")
    %Pb.ConfigChange{replica_id: id} = cmd
    %ReplicaState{remote_impl: remote_impl, remotes: remotes, term: term} = state

    {to_drop, remotes} = Map.pop(remotes, id)

    unless is_nil(to_drop) do
      Remote.disconnect(remote_impl, to_drop)
    end

    became_follower(%ReplicaState{state | remotes: remotes, members_count: Enum.count(remotes)}, term, 0)
  end

  defp add_no_op_entry(%ReplicaState{log_store_impl: log_store_impl, term: term, last_index: last_index} = state) do
    {:ok, 1} =
      LogStore.append_log_entries(log_store_impl, [%Pb.Entry{term: term, index: last_index + 1, type: :etype_no_op}])

    {peer, _} =
      state
      |> local_peer()
      |> Models.Replica.try_update(last_index + 1)

    update_remote(%ReplicaState{state | last_index: last_index + 1}, peer)
  end

  # ----------------- replicate msgs ---------------
  @spec make_replicate_msg(Models.Replica.t(), ReplicaState.t()) :: Pb.Message.t() | nil
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
  def send_replicate_msg(%ReplicaState{last_index: last_index} = state, %Models.Replica{match: match} = peer)
      when match < last_index do
    %Pb.Message{entries: entries} = msg = make_replicate_msg(peer, state)
    send_msg(state, msg)

    case entries do
      [] ->
        state

      _ ->
        %Pb.Entry{index: index} = List.last(entries)
        peer = Models.Replica.make_progress!(peer, index)
        update_remote(state, peer)
    end
  end

  def send_replicate_msg(%ReplicaState{last_index: last_index} = state, %Models.Replica{match: match})
      when match == last_index,
      do: state

  def send_replicate_msg(_state, _peer),
    do: raise(ExRaft.Exception, message: "remote's match is not less than last_index")

  @spec has_commited_entry_at_current_term?(ReplicaState.t()) :: boolean()
  def has_commited_entry_at_current_term?(%ReplicaState{
        commit_index: commit_index,
        term: term,
        log_store_impl: log_store_impl
      }) do
    {:ok, commit_term} = LogStore.get_log_term(log_store_impl, commit_index)
    commit_term == term
  end

  # -------------- read index ----------
  @spec add_read_index_req(ReplicaState.t(), Typespecs.ref(), Typespecs.replica_id_t()) :: ReplicaState.t()
  def add_read_index_req(
        %ReplicaState{read_index_q: read_index_reqs, commit_index: commit_index, read_index_status: status_store} = state,
        ref,
        from_id
      ) do
    status_store
    |> Map.has_key?(ref)
    |> if do
      state
    else
      status = %Models.ReadStatus{
        index: commit_index,
        from: from_id,
        ref: ref
      }

      # ExRaft.Debug.debug("add_read_index_req: #{inspect(ref)}")

      %ReplicaState{state | read_index_q: [ref | read_index_reqs], read_index_status: Map.put(status_store, ref, status)}
    end
  end

  @spec peek_read_index_req(ReplicaState.t()) :: Typespecs.ref() | nil
  defp peek_read_index_req(%ReplicaState{read_index_q: []}), do: nil

  defp peek_read_index_req(%ReplicaState{read_index_q: [req | _]}), do: req

  @spec may_read_index_confirm(ReplicaState.t(), Pb.Message.t()) :: {boolean(), Typespecs.ref(), ReplicaState.t()}
  def may_read_index_confirm(state, %Pb.Message{ref: ""}), do: {false, nil, state}

  def may_read_index_confirm(state, msg) do
    %ReplicaState{read_index_status: status_store} = state
    %Pb.Message{from: from, ref: ref_bin} = msg

    ref = :erlang.binary_to_term(ref_bin)

    # update status_store
    {updated?, status_store} =
      status_store
      |> Map.get(ref)
      |> case do
        nil ->
          {false, status_store}

        status ->
          %Models.ReadStatus{confirmed: confirmed} = status
          confirmed2 = MapSet.put(confirmed, from)

          # Logger.debug("confirm read index, #{inspect(ref)}")

          {
            MapSet.equal?(confirmed, confirmed2),
            Map.put(status_store, ref, %Models.ReadStatus{status | confirmed: confirmed2})
          }
      end

    {updated?, ref, %ReplicaState{state | read_index_status: status_store}}
  end

  @spec read_index_check_quorum_pass?(ReplicaState.t(), Typespecs.ref()) :: boolean()
  def read_index_check_quorum_pass?(state, ref) do
    %ReplicaState{read_index_status: status_store} = state
    %Models.ReadStatus{confirmed: confirmed} = Map.fetch!(status_store, ref)

    MapSet.size(confirmed) >= quorum(state)
  end

  @spec pop_all_ready_read_index_status(ReplicaState.t(), Typespecs.ref()) ::
          {[Models.ReadStatus.t()], ReplicaState.t()}
  def pop_all_ready_read_index_status(state, ref) do
    %ReplicaState{read_index_q: q, read_index_status: status_store} = state
    {to_pop, status_store, q} = iter_read_index_queue(Enum.reverse(q), status_store, ref, [])
    {to_pop, %ReplicaState{state | read_index_q: q, read_index_status: status_store}}
  end

  defp iter_read_index_queue([h | t], status_store, ref, acc) when h == ref do
    {status, status_store} = Map.pop!(status_store, h)
    %Models.ReadStatus{index: index} = status
    acc = Enum.map(acc, fn x -> %Models.ReadStatus{x | index: index} end)
    {[status | acc], status_store, Enum.reverse(t)}
  end

  defp iter_read_index_queue([h | t], status_store, ref, acc) do
    {status, status_store} = Map.pop!(status_store, h)
    iter_read_index_queue(t, status_store, ref, [status | acc])
  end

  # reponse local req
  @spec response_read_index_req(Models.ReadStatus.t(), ReplicaState.t()) :: :ok | {:error, ExRaft.Exception.t()}
  def response_read_index_req(%Models.ReadStatus{from: from} = status, %ReplicaState{self: id} = state) when from == id do
    %Models.ReadStatus{ref: ref, index: index} = status
    %ReplicaState{req_register: rr, commit_index: commit_index, last_applied: last_applied} = state

    rr
    |> Utils.ReqRegister.pop_req(ref)
    |> case do
      nil ->
        # already timeout
        Logger.debug("read index req already timeout, #{inspect(ref)}")
        :ok

      from_p ->
        cond do
          index <= last_applied ->
            :gen_statem.reply(from_p, {:ok, :applied})

          index <= commit_index ->
            :gen_statem.reply(from_p, {:ok, :committed})

          true ->
            :gen_statem.reply(from_p, {:ok, :uncommitted})
        end
    end
  end

  # response remote req
  def response_read_index_req(%Models.ReadStatus{} = status, state) do
    %Models.ReadStatus{from: from, ref: ref, index: index} = status
    %ReplicaState{self: self, term: term} = state

    msg = %Pb.Message{
      to: from,
      from: self,
      term: term,
      type: :read_index_resp,
      ref: :erlang.term_to_binary(ref),
      hint: index
    }

    send_msg(state, msg)
  end

  # ------------- broadcast heartbeat -----------
  @spec broadcast_heartbeat(ReplicaState.t()) :: ReplicaState.t()
  def broadcast_heartbeat(state), do: broadcast_heartbeat_with_read_index(state, peek_read_index_req(state))

  @spec broadcast_heartbeat_with_read_index(ReplicaState.t(), Typespecs.ref() | nil) :: ReplicaState.t()
  def broadcast_heartbeat_with_read_index(state, nil) do
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

    send_msgs(state, ms)

    state
    |> reset_heartbeat()
    |> tick(true)
  end

  def broadcast_heartbeat_with_read_index(state, ref) do
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
          commit: min(match, commit_index),
          ref: :erlang.term_to_binary(ref)
        }
      end)

    send_msgs(state, ms)

    state
    |> reset_heartbeat()
    |> tick(true)
  end

  # ------------------------- snapshot --------------------

  @spec create_snapshot_metadata(ReplicaState.t()) :: {Pb.SnapshotMetadata.t(), Statemachine.safepoint()}
  defp create_snapshot_metadata(state) do
    %ReplicaState{self: id, data_path: data_path, statemachine_impl: statemachine_impl, remotes: remotes} = state

    addresses =
      Enum.map(remotes, fn {replica_id, %Models.Replica{host: host}} ->
        %Pb.SnapshotMetadata.AddressesEntry{key: replica_id, value: host}
      end)

    {:ok, {index, term, _} = safepoint} = Statemachine.prepare_snapshot(statemachine_impl)

    {
      struct(Pb.SnapshotMetadata,
        filepath: Path.join([data_path, "snapshot", "#{id}", "snapshot-#{index}.dat"]),
        replica_id: id,
        index: index,
        term: term,
        addresses: addresses
      ),
      safepoint
    }
  end

  @spec save_snapshot(ReplicaState.t()) :: :ok | {:error, ExRaft.Exception.t()}
  def save_snapshot(state) do
    %ReplicaState{statemachine_impl: statemachine_impl, log_store_impl: log_store_impl, task_supervisor: task_supervisor} =
      state

    {%Pb.SnapshotMetadata{filepath: fp, index: index} = sm, safe_point} = create_snapshot_metadata(state)

    sm_bin = Pb.SnapshotMetadata.encode(sm)
    sm_bin_prefix = Utils.Uvaint.encode(byte_size(sm_bin))

    # save snapshot asynchronously
    Task.Supervisor.start_child(task_supervisor, fn ->
      {:ok, _} =
        File.open(fp, [:write, :binary], fn file ->
          IO.write(file, sm_bin_prefix <> sm_bin)
          Statemachine.save_snapshot(statemachine_impl, safe_point, file)
        end)
    end)

    LogStore.truncate_before(log_store_impl, index)
  end
end
