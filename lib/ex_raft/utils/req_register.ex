defmodule ExRaft.Utils.ReqRegister do
  @moduledoc """
  A simple TTL (time-to-live) req register .
  """

  use GenServer

  def start_link(opts \\ []) do
    opts =
      opts
      |> Keyword.put_new(:name, __MODULE__)
      |> Keyword.put_new(:tick_rt, 100)

    # tick each 100 milliseconds default

    GenServer.start_link(__MODULE__, opts, name: opts[:name])
  end

  def stop(name_or_pid), do: GenServer.stop(name_or_pid)

  @doc """
  Register a request with a timeout.
  """
  @spec register_req(GenServer.name(), reference(), GenServer.from(), non_neg_integer()) :: :ok
  def register_req(name_or_pid, ref, from, ttl \\ 10) do
    GenServer.cast(name_or_pid, {:register, ref, from, ttl})
  end

  @spec pop_req(GenServer.name(), reference()) :: GenServer.from() | nil
  def pop_req(name_or_pid, ref) do
    GenServer.call(name_or_pid, {:pop, ref})
  end

  # ----------------- server ------------------

  @impl GenServer
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    tick_rt = Keyword.fetch!(opts, :tick_rt)

    table = :ets.new(name, [:ordered_set, :public, :named_table, read_concurrency: true])

    {:ok, %{table: table, tick_rt: tick_rt, tick: 0}, {:continue, :run}}
  end

  @impl GenServer
  def terminate(_reason, state) do
    %{table: table} = state
    :ets.delete(table)
  end

  @impl GenServer
  def handle_continue(:run, state) do
    %{tick_rt: tick_rt} = state

    Process.send_after(self(), :tick, tick_rt)
    {:noreply, state}
  end

  @impl GenServer
  def handle_info(:tick, state) do
    %{table: table, tick_rt: tick_rt, tick: tick} = state

    # delete object which $3 < tick
    :ets.select_delete(table, :ets.fun2ms(fn {_, _, x} -> x < tick end))

    # schedule next tick
    Process.send_after(self(), :tick, tick_rt)
    {:noreply, %{state | tick: tick + 1}}
  end

  @impl GenServer
  def handle_cast({:register, ref, from, ttl}, state) do
    %{table: table, tick: tick} = state

    # Add the key-value pair
    :ets.insert(table, {ref, from, tick + ttl})

    {:noreply, state}
  end

  @impl GenServer
  def handle_call({:pop, ref}, _from, state) do
    %{table: table} = state

    case :ets.lookup(table, ref) do
      [{ref, from, _}] ->
        :ets.delete(table, ref)
        {:reply, from, state}

      _ ->
        {:reply, nil, state}
    end
  end
end
