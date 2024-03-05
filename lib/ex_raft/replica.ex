defmodule ExRaft.Replica do
  @moduledoc """
  Replica
  """

  @behaviour :gen_statem

  alias ExRaft.LogStore
  alias ExRaft.Models
  alias ExRaft.Models.ReplicaState
  alias ExRaft.Pipeline
  alias ExRaft.Roles
  alias ExRaft.Roles.Common
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

  @impl true
  def init(opts) do
    :rand.seed(:exsss, {100, 101, 102})

    remotes =
      Map.new(opts[:peers], fn {id, host, port} ->
        {id,
         %Models.Replica{
           id: id,
           host: host,
           port: port
         }}
      end)

    # start pipeline client
    {:ok, _} = Pipeline.start_link(opts[:pipeline_impl])

    self_id = opts[:id]
    # connect to remotes
    Enum.each(remotes, fn
      {id, peer} when id != self_id -> Pipeline.connect(opts[:pipeline_impl], peer)
      _ -> :do_nothing
    end)

    # start log store
    {:ok, _} = LogStore.start_link(opts[:log_store_impl])

    # start statemachine
    {:ok, _} = Statemachine.start_link(opts[:statemachine_impl])

    state =
      Common.became_follower(
        %ReplicaState{
          self: self_id,
          remotes: remotes,
          members_count: Enum.count(remotes),
          tick_delta: opts[:tick_delta],
          election_timeout: opts[:election_timeout],
          heartbeat_timeout: opts[:heartbeat_timeout],
          pipeline_impl: opts[:pipeline_impl],
          log_store_impl: opts[:log_store_impl],
          statemachine_impl: opts[:statemachine_impl]
        },
        0,
        0
      )

    {:ok, :follower, state, Common.tick_action(state)}
  end

  @impl true
  def terminate(reason, current_state, %Models.ReplicaState{
        term: term,
        pipeline_impl: pipeline_impl,
        log_store_impl: log_store_impl,
        statemachine_impl: statemachine_impl
      }) do
    Logger.warning("terminate: reason: #{inspect(reason)}, current_state: #{inspect(current_state)}, term: #{term}")
    Pipeline.stop(pipeline_impl)
    LogStore.stop(log_store_impl)
    Statemachine.stop(statemachine_impl)
  end

  defdelegate follower(event, data, state), to: Roles.Follower

  defdelegate candidate(event, data, state), to: Roles.Candidate

  defdelegate leader(event, data, state), to: Roles.Leader
end
