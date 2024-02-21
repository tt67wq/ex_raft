defmodule ExRaft.Replica do
  @moduledoc """
  Replica
  """

  @behaviour :gen_statem

  alias ExRaft.Exception
  alias ExRaft.LogStore
  alias ExRaft.Models
  alias ExRaft.Roles
  alias ExRaft.Rpc
  alias ExRaft.Statemachine

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
            voted_for: integer(),
            leader_id: integer(),
            last_log_index: integer(),
            commit_index: integer(),
            last_applied: integer(),
            rpc_impl: ExRaft.Rpc.t(),
            log_store_impl: ExRaft.LogStore.t(),
            statemachine_impl: ExRaft.Statemachine.t()
          }

    defstruct self: nil,
              peers: [],
              term: 0,
              election_reset_ts: 0,
              election_timeout: 0,
              election_check_delta: 0,
              heartbeat_delta: 0,
              voted_for: -1,
              leader_id: -1,
              last_log_index: -1,
              commit_index: -1,
              last_applied: -1,
              rpc_impl: nil,
              log_store_impl: nil,
              statemachine_impl: nil
  end

  @type state_t :: :follower | :candidate | :leader
  @type term_t :: non_neg_integer()

  @impl true
  def callback_mode do
    [:state_functions, :state_enter]
  end

  @spec start_link(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_link(opts) do
    :gen_statem.start_link(__MODULE__, opts, [])
  end

  defp gen_election_timeout(timeout), do: Enum.random(timeout..(2 * timeout))

  @impl true
  def init(opts) do
    :rand.seed(:exsss, {100, 101, 102})

    peers =
      Enum.map(opts[:peers], fn {id, host, port} -> Models.Replica.new(id, host, port) end)

    # start rpc client
    {:ok, _} = Rpc.start_link(opts[:rpc_impl])

    # connect to peers
    Enum.each(peers, fn node -> Rpc.connect(opts[:rpc_impl], node) end)

    local = find_peer(opts[:id], peers)
    is_nil(local) && raise(Exception.new("local peer not found", opts[:id]))

    # start log store
    {:ok, _} = LogStore.start_link(opts[:log_store_impl])

    # start statemachine
    {:ok, _} = Statemachine.start_link(opts[:statemachine_impl])

    {:ok, :follower,
     %State{
       self: local,
       peers: Enum.reject(peers, fn %Models.Replica{id: id} -> id == opts[:id] end),
       term: opts[:term],
       election_reset_ts: System.system_time(:millisecond),
       election_timeout: gen_election_timeout(opts[:election_timeout]),
       election_check_delta: opts[:election_check_delta],
       heartbeat_delta: opts[:heartbeat_delta],
       rpc_impl: opts[:rpc_impl],
       log_store_impl: opts[:log_store_impl]
     }, [{{:timeout, :election}, 300, nil}]}
  end

  @impl true
  def terminate(reason, current_state, %State{term: term}) do
    Logger.warning("terminate: reason: #{inspect(reason)}, current_state: #{inspect(current_state)}, term: #{term}")
  end

  defdelegate follower(event, data, state), to: Roles.Follower

  defdelegate candidate(event, data, state), to: Roles.Candidate

  defdelegate leader(event, data, state), to: Roles.Leader

  @spec find_peer(non_neg_integer(), list(Models.Replica.t())) :: Models.Replica.t() | nil
  defp find_peer(id, peers), do: Enum.find(peers, fn %Models.Replica{id: x} -> x == id end)
end
