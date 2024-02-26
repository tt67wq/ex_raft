defmodule ExRaft.Replica do
  @moduledoc """
  Replica
  """

  @behaviour :gen_statem

  alias ExRaft.Exception
  alias ExRaft.LogStore
  alias ExRaft.Models
  alias ExRaft.Pipeline
  alias ExRaft.Roles
  alias ExRaft.Statemachine

  require Logger

  # alias ExRaft.Models

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

    # start pipeline client
    {:ok, _} = Pipeline.start_link(opts[:pipeline_impl])

    # connect to peers
    Enum.each(peers, fn node -> Pipeline.connect(opts[:pipeline_impl], node) end)

    local = find_peer(opts[:id], peers)
    is_nil(local) && raise(Exception.new("local peer not found", opts[:id]))

    # start log store
    {:ok, _} = LogStore.start_link(opts[:log_store_impl])

    # start statemachine
    {:ok, _} = Statemachine.start_link(opts[:statemachine_impl])

    {:ok, :follower,
     %Models.ReplicaState{
       self: local,
       peers: Enum.reject(peers, fn %Models.Replica{id: id} -> id == opts[:id] end),
       term: opts[:term],
       election_reset_ts: System.system_time(:millisecond),
       election_timeout: gen_election_timeout(opts[:election_timeout]),
       election_check_delta: opts[:election_check_delta],
       heartbeat_delta: opts[:heartbeat_delta],
       pipeline_impl: opts[:pipeline_impl],
       log_store_impl: opts[:log_store_impl]
     }, [{{:timeout, :election}, 300, nil}]}
  end

  @impl true
  def terminate(reason, current_state, %Models.ReplicaState{term: term}) do
    Logger.warning("terminate: reason: #{inspect(reason)}, current_state: #{inspect(current_state)}, term: #{term}")
  end

  defdelegate follower(event, data, state), to: Roles.Follower

  defdelegate candidate(event, data, state), to: Roles.Candidate

  defdelegate leader(event, data, state), to: Roles.Leader

  @spec find_peer(non_neg_integer(), list(Models.Replica.t())) :: Models.Replica.t() | nil
  defp find_peer(id, peers), do: Enum.find(peers, fn %Models.Replica{id: x} -> x == id end)
end
