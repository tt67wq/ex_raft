defmodule ExRaft.Replica do
  @moduledoc """
  Replica
  """

  @behaviour :gen_statem

  alias ExRaft.Exception
  alias ExRaft.Models
  alias ExRaft.Rpc

  require Logger

  # alias ExRaft.Models

  defmodule State do
    @moduledoc """
    Replica GenServer State
    """
    alias ExRaft.Models

    @type t :: %__MODULE__{
            self: Models.Replica.t(),
            peers: list(Models.Replica.t()),
            term: non_neg_integer(),
            election_reset_ts: non_neg_integer(),
            election_timeout: non_neg_integer(),
            election_check_delta: non_neg_integer(),
            heartbeat_delta: non_neg_integer(),
            vote_for: Models.Replica.t() | nil,
            rpc_impl: ExRaft.Rpc.t()
          }

    defstruct self: nil,
              peers: [],
              term: 0,
              election_reset_ts: 0,
              election_timeout: 0,
              election_check_delta: 0,
              heartbeat_delta: 0,
              vote_for: nil,
              rpc_impl: nil
  end

  @replica_opts_schema [
    id: [
      type: :non_neg_integer,
      required: true,
      doc: "Replica ID"
    ],
    peers: [
      type: {:list, :any},
      default: [],
      doc: "Replica peers, list of `{id :: non_neg_integer(), host :: String.t(), port :: non_neg_integer()}`"
    ],
    term: [
      type: :non_neg_integer,
      default: 0,
      doc: "Replica term"
    ],
    rpc_impl: [
      type: :any,
      default: ExRaft.Rpc.Default.new(),
      doc: "RPC implementation of `ExRaft.Rpc`"
    ],
    election_timeout: [
      type: :non_neg_integer,
      default: 150,
      doc: "Election timeout in milliseconds, default 150ms~300ms"
    ],
    election_check_delta: [
      type: :non_neg_integer,
      default: 15,
      doc: "Election check delta in milliseconds, default 15ms"
    ],
    heartbeat_delta: [
      type: :non_neg_integer,
      default: 50,
      doc: "Heartbeat delta in milliseconds, default 50ms"
    ]
  ]

  @type state_t :: :follower | :candidate | :leader
  @type replica_opts_t :: [unquote(NimbleOptions.option_typespec(@replica_opts_schema))]

  @impl true
  def callback_mode do
    [:state_functions, :state_enter]
  end

  @spec start_link(replica_opts_t()) :: {:ok, pid()} | {:error, term()}
  def start_link(opts) do
    opts = NimbleOptions.validate!(opts, @replica_opts_schema)
    :gen_statem.start_link(__MODULE__, opts, [])
  end

  defp gen_election_timeout(timeout), do: Enum.random(timeout..(2 * timeout))

  @impl true
  def init(opts) do
    :rand.seed(:exsss, {100, 101, 102})

    peers =
      Enum.map(opts[:peers], fn {id, host, port} -> Models.Replica.new(id, host, port) end)

    # connect to peers
    Enum.each(peers, fn node -> Rpc.connect(opts[:rpc_impl], node) end)

    {:ok, :follower,
     %State{
       self: find_peer(opts[:id], peers),
       peers: Enum.reject(peers, fn %Models.Replica{id: id} -> id == opts[:id] end),
       term: opts[:term],
       election_reset_ts: System.system_time(:millisecond),
       election_timeout: gen_election_timeout(opts[:election_timeout]),
       election_check_delta: opts[:election_check_delta],
       heartbeat_delta: opts[:heartbeat_delta],
       rpc_impl: opts[:rpc_impl]
     }, [{{:timeout, :election}, 300, nil}]}
  end

  @impl true
  def terminate(reason, current_state, %State{term: term}) do
    Logger.warning("terminate: reason: #{inspect(reason)}, current_state: #{inspect(current_state)}, term: #{term}")
  end

  # . ------------ follower ------------ .

  def follower(:enter, _old_state, _state) do
    {:keep_state_and_data, [{{:timeout, :election}, 300, nil}]}
  end

  def follower(
        {:timeout, :election},
        _,
        %State{
          election_reset_ts: election_reset_ts,
          election_timeout: election_timeout,
          election_check_delta: election_check_delta
        } = state
      ) do
    if System.system_time(:millisecond) - election_reset_ts > election_timeout do
      {:next_state, :candidate, state}
    else
      {:keep_state_and_data, [{{:timeout, :election}, election_check_delta, nil}]}
    end
  end

  def follower(
        {:call, from},
        {:rpc_call, %Models.RequestVote.Req{term: term, candidate_id: candidate_id}},
        %State{peers: peers} = state
      ) do
    Logger.debug("follower: handle request vote: term: #{term}, candidate_id: #{candidate_id}")
    requst_peer = find_peer(candidate_id, peers)

    if is_nil(requst_peer) do
      {:keep_state_and_data, [{:reply, from, {:error, Exception.new("peer not found", candidate_id)}}]}
    else
      handle_request_vote(:follower, from, requst_peer, term, state)
    end
  end

  def follower(
        {:call, from},
        {:rpc_call, %Models.AppendEntries.Req{term: term, leader_id: leader_id}},
        %State{peers: peers} = state
      ) do
    Logger.debug("follower: handle append entries: term: #{term}, leader_id: #{leader_id}")
    request_peer = find_peer(leader_id, peers)

    if is_nil(request_peer) do
      {:keep_state_and_data, [{:reply, from, {:error, Exception.new("peer not found", leader_id)}}]}
    else
      handle_append_entries(:follower, from, request_peer, term, state)
    end
  end

  def follower({:call, from}, :show, state) do
    {:keep_state_and_data, [{:reply, from, {:ok, %{state: state, role: :follower}}}]}
  end

  def follower(event, data, state) do
    ExRaft.Debug.stacktrace(%{
      event: event,
      data: data,
      state: state
    })

    :keep_state_and_data
  end

  # . ------------ candidate ------------ .

  def candidate(:enter, _old_state, _state) do
    {:keep_state_and_data, [{{:timeout, :election}, 300, nil}]}
  end

  def candidate(
        {:timeout, :election},
        _,
        %State{
          election_reset_ts: election_reset_ts,
          election_timeout: election_timeout,
          election_check_delta: election_check_delta
        } = state
      ) do
    if System.system_time(:millisecond) - election_reset_ts > election_timeout do
      state
      |> handle_election()
      |> case do
        {:leader, term} ->
          {:next_state, :leader, %State{state | term: term, vote_for: nil}}

        {:follower, term} ->
          {:next_state, :follower, %State{state | term: term, vote_for: nil}}

        {:candidate, term} ->
          {:keep_state, %State{state | term: term, vote_for: nil}, [{{:timeout, :election}, election_check_delta, nil}]}
      end
    else
      {:keep_state_and_data, [{{:timeout, :election}, election_check_delta, nil}]}
    end
  end

  def candidate({:call, from}, {:rpc_call, %Models.RequestVote.Req{term: term, candidate_id: candidate_id}}, state) do
    requst_peer = find_peer(candidate_id, state.peers)

    if is_nil(requst_peer) do
      {:keep_state_and_data, [{:reply, from, {:error, Exception.new("peer not found", candidate_id)}}]}
    else
      handle_request_vote(:candidate, from, requst_peer, term, state)
    end
  end

  def candidate(
        {:call, from},
        {:rpc_call, %Models.AppendEntries.Req{term: term, leader_id: leader_id}},
        %State{peers: peers} = state
      ) do
    Logger.debug("candidate: handle append entries: term: #{term}, leader_id: #{leader_id}")
    request_peer = find_peer(leader_id, peers)

    if is_nil(request_peer) do
      {:keep_state_and_data, [{:reply, from, {:error, Exception.new("peer not found", leader_id)}}]}
    else
      handle_append_entries(:candidate, from, request_peer, term, state)
    end
  end

  def candidate({:call, from}, :show, state) do
    {:keep_state_and_data, [{:reply, from, {:ok, %{state: state, role: :candidate}}}]}
  end

  def candidate(event, data, state) do
    ExRaft.Debug.stacktrace(%{
      event: event,
      data: data,
      state: state
    })

    :keep_state_and_data
  end

  # . ------------ leader ------------ .

  def leader(:enter, _old_state, _state) do
    {:keep_state_and_data, [{{:timeout, :heartbeat}, 50, nil}]}
  end

  def leader(
        {:timeout, :heartbeat},
        _,
        %State{
          self: %Models.Replica{id: id},
          peers: peers,
          term: current_term,
          rpc_impl: rpc_impl,
          heartbeat_delta: heartbeat_delta
        } = state
      ) do
    Logger.debug("leader: send heartbeat: term: #{current_term}")

    peers
    |> Enum.map(fn peer ->
      Task.async(fn ->
        {peer, Rpc.just_call(rpc_impl, peer, %Models.AppendEntries.Req{term: current_term, leader_id: id})}
      end)
    end)
    |> Task.await_many(200)
    |> Enum.reduce_while(0, fn
      %Models.AppendEntries.Reply{
        term: term
      },
      _acc
      when term > current_term ->
        {:halt, term}

      {_, _}, acc ->
        {:cont, acc}
    end)
    |> case do
      0 ->
        {:keep_state_and_data, [{{:timeout, :heartbeat}, heartbeat_delta, nil}]}

      term ->
        {:next_state, :follower, %State{state | term: term, vote_for: nil}}
    end
  end

  def leader(
        {:call, from},
        {:rpc_call, %Models.RequestVote.Req{term: term, candidate_id: candidate_id}},
        %State{peers: peers} = state
      ) do
    requst_peer = find_peer(candidate_id, peers)

    if is_nil(requst_peer) do
      {:keep_state_and_data, [{:reply, from, {:error, Exception.new("peer not found", candidate_id)}}]}
    else
      handle_request_vote(:leader, from, requst_peer, term, state)
    end
  end

  def leader(
        {:call, from},
        {:rpc_call, %Models.AppendEntries.Req{term: term, leader_id: leader_id}},
        %State{peers: peers} = state
      ) do
    request_peer = find_peer(leader_id, peers)

    if is_nil(request_peer) do
      {:keep_state_and_data, [{:reply, from, {:error, Exception.new("peer not found", leader_id)}}]}
    else
      handle_append_entries(:leader, from, request_peer, term, state)
    end
  end

  def leader({:call, from}, :show, state) do
    {:keep_state_and_data, [{:reply, from, {:ok, %{state: state, role: :leader}}}]}
  end

  def leader(event, data, state) do
    ExRaft.Debug.stacktrace(%{
      event: event,
      data: data,
      state: state
    })

    :keep_state_and_data
  end

  # . ------------ handle_request_vote ------------ .

  @spec handle_request_vote(state_t(), pid(), Models.Replica.t(), non_neg_integer(), State.t()) ::
          {:keep_state, State.t(), list({:reply, pid(), {:ok, Models.RequestVote.Reply.t()}})}
  defp handle_request_vote(:follower, from, requst_peer, term, %State{term: current_term} = state)
       when term > current_term do
    {:keep_state, %State{state | term: term, vote_for: requst_peer, election_reset_ts: System.system_time(:millisecond)},
     [{:reply, from, {:ok, %Models.RequestVote.Reply{term: term, vote_granted: true}}}]}
  end

  defp handle_request_vote(:follower, from, request_peer, term, %State{term: current_term, vote_for: nil} = state)
       when term == current_term do
    {:keep_state, %State{state | vote_for: request_peer},
     [{:reply, from, {:ok, %Models.RequestVote.Reply{term: current_term, vote_granted: true}}}]}
  end

  defp handle_request_vote(:follower, from, %Models.Replica{id: candidate_id}, term, %State{
         term: current_term,
         vote_for: %Models.Replica{id: origin_leader_id}
       })
       when term == current_term and origin_leader_id == candidate_id do
    {:keep_state_and_data, [{:reply, from, {:ok, %Models.RequestVote.Reply{term: current_term, vote_granted: true}}}]}
  end

  defp handle_request_vote(:follower, from, _request_peer, _term, %State{term: current_term}) do
    {:keep_state_and_data, [{:reply, from, {:ok, %Models.RequestVote.Reply{term: current_term, vote_granted: false}}}]}
  end

  defp handle_request_vote(:candidate, from, request_peer, term, %State{term: current_term} = state)
       when term > current_term do
    {:next_state, :follower,
     %State{state | term: term, vote_for: request_peer, election_reset_ts: System.system_time(:millisecond)},
     [{:reply, from, {:ok, %Models.RequestVote.Reply{term: term, vote_granted: true}}}]}
  end

  defp handle_request_vote(:candidate, from, request_peer, term, %State{term: current_term, vote_for: nil} = state)
       when current_term == term do
    {:next_state, :follower, %State{state | vote_for: request_peer, election_reset_ts: System.system_time(:millisecond)},
     [{:reply, from, {:ok, %Models.RequestVote.Reply{term: current_term, vote_granted: true}}}]}
  end

  defp handle_request_vote(:candidate, from, %Models.Replica{id: candidate_id}, term, %State{
         term: current_term,
         vote_for: %Models.Replica{id: origin_leader_id}
       })
       when current_term == term and candidate_id == origin_leader_id do
    {:keep_state_and_data, [{:reply, from, {:ok, %Models.RequestVote.Reply{term: current_term, vote_granted: true}}}]}
  end

  defp handle_request_vote(:candidate, from, _request_peer, _term, %State{term: current_term}) do
    {:keep_state_and_data, [{:reply, from, {:ok, %Models.RequestVote.Reply{term: current_term, vote_granted: false}}}]}
  end

  defp handle_request_vote(:leader, from, _request_peer, term, %State{term: current_term}) when term > current_term do
    {:next_state, :follower, %State{term: term, vote_for: nil},
     [{:reply, from, {:ok, %Models.RequestVote.Reply{term: term, vote_granted: true}}}]}
  end

  defp handle_request_vote(:leader, from, _request_peer, _term, %State{term: current_term}) do
    {:keep_state_and_data, [{:reply, from, {:ok, %Models.RequestVote.Reply{term: current_term, vote_granted: false}}}]}
  end

  # . ------------ handle_append_entries ------------ .

  @spec handle_append_entries(state_t(), pid(), Models.Replica.t(), non_neg_integer(), State.t()) ::
          {:keep_state, State.t(), list({:reply, pid(), {:ok, Models.AppendEntries.Reply.t()}})}
  defp handle_append_entries(:follower, from, request_peer, term, %State{term: current_term} = state)
       when term > current_term do
    {:keep_state, %State{state | vote_for: request_peer, term: term, election_reset_ts: System.system_time(:millisecond)},
     [{:reply, from, {:ok, %Models.AppendEntries.Reply{term: term, success: true}}}]}
  end

  defp handle_append_entries(
         :follower,
         from,
         %Models.Replica{id: leader_id},
         term,
         %State{term: current_term, vote_for: %Models.Replica{id: origin_leader_id}} = state
       )
       when term == current_term and leader_id == origin_leader_id do
    {:keep_state, %State{state | election_reset_ts: System.system_time(:millisecond)},
     [{:reply, from, {:ok, %Models.AppendEntries.Reply{term: current_term, success: true}}}]}
  end

  defp handle_append_entries(:follower, from, request_peer, term, %State{term: current_term, vote_for: nil} = state)
       when term == current_term do
    {:keep_state, %State{state | vote_for: request_peer, election_reset_ts: System.system_time(:millisecond)},
     [{:reply, from, {:ok, %Models.AppendEntries.Reply{term: current_term, success: true}}}]}
  end

  defp handle_append_entries(:follower, from, _request_peer, _term, %State{term: current_term} = state) do
    {:keep_state, state, [{:reply, from, {:ok, %Models.AppendEntries.Reply{term: current_term, success: false}}}]}
  end

  defp handle_append_entries(:candidate, from, request_peer, term, %State{term: current_term} = state)
       when term >= current_term do
    {:next_state, :follower,
     %State{state | term: term, election_reset_ts: System.system_time(:millisecond), vote_for: request_peer},
     [{:reply, from, {:ok, %Models.AppendEntries.Reply{term: term, success: true}}}]}
  end

  defp handle_append_entries(:candidate, from, _, term, %State{term: current_term}) when term < current_term do
    {:keep_state_and_data, [{:reply, from, {:ok, %Models.AppendEntries.Reply{term: current_term, success: false}}}]}
  end

  defp handle_append_entries(:leader, from, _request_peer, term, %State{term: current_term}) when term > current_term do
    {:next_state, :follower, %State{term: term, vote_for: nil},
     [{:reply, from, {:ok, %Models.AppendEntries.Reply{term: term, success: true}}}]}
  end

  defp handle_append_entries(:leader, from, _request_peer, _term, %State{term: current_term}) do
    {:keep_state_and_data, [{:reply, from, {:ok, %Models.AppendEntries.Reply{term: current_term, success: false}}}]}
  end

  # . ------------ handle_election ------------ .

  @spec handle_election(State.t()) :: {state_t(), non_neg_integer()}
  defp handle_election(%State{term: term, peers: []}) do
    # no other peers, become leader
    {:leader, term + 1}
  end

  defp handle_election(%State{term: term, peers: peers, self: %Models.Replica{id: id}, rpc_impl: rpc_impl}) do
    term = term + 1
    # start election
    peers
    |> Enum.map(fn peer ->
      Task.async(fn ->
        # ExRaft.Debug.stacktrace(%{
        #   peer: peer,
        #   term: term,
        #   id: id
        # })

        Rpc.just_call(rpc_impl, peer, %Models.RequestVote.Req{term: term, candidate_id: id})
      end)
    end)
    |> Task.await_many(2000)
    |> Enum.reduce_while(
      {1, :candidate, term},
      fn
        %Models.RequestVote.Reply{
          term: reply_term
        },
        {votes, _, _}
        when reply_term > term ->
          # a higher term is found, become follower
          {:halt, {votes, :follower, reply_term}}

        %Models.RequestVote.Reply{vote_granted: true, term: ^term}, {votes, :candidate, _} ->
          if 2 * (votes + 1) > Enum.count(peers) + 1 do
            # over half of the peers voted for me, become leader
            {:halt, {votes + 1, :leader, term}}
          else
            {:cont, {votes + 1, :candidate, term}}
          end

        _resp, acc ->
          # vote not granted or error, continue
          {:cont, acc}
      end
    )
    # |> ExRaft.Debug.debug()
    |> case do
      # election success
      {_, :leader, _} ->
        {:leader, term}

      # higher term found, become follower
      {_, :follower, higher_term} ->
        {:follower, higher_term}

      # election failed, restart election
      {_, :candidate, _} ->
        {:candidate, term}
    end
  end

  @spec find_peer(non_neg_integer(), list(Models.Replica.t())) :: Models.Replica.t() | nil
  defp find_peer(id, peers), do: Enum.find(peers, fn %Models.Replica{id: x} -> x == id end)
end
