defmodule ExRaft.Pipeline.Erlang do
  @moduledoc """
  ExRaft.Pipeline implementation using Erlang
  """
  @behaviour ExRaft.Pipeline

  use GenServer

  alias ExRaft.Models
  alias ExRaft.Serialize

  @type t :: %__MODULE__{
          name: atom(),
          self_id: non_neg_integer(),
          pipe_delta: non_neg_integer()
        }

  defstruct name: __MODULE__, self_id: 0, pipe_delta: 100

  @spec new(Keyword.t()) :: t()
  def new(opts \\ []) do
    opts =
      opts
      |> Keyword.put_new(:name, __MODULE__)
      |> Keyword.put_new(:self_id, 0)
      |> Keyword.put_new(:pipe_delta, 100)

    struct(__MODULE__, opts)
  end

  @impl ExRaft.Pipeline
  def start_link(pipeline: %__MODULE__{name: name} = m) do
    GenServer.start_link(__MODULE__, m, name: name)
  end

  @impl ExRaft.Pipeline
  def stop(%__MODULE__{name: name}) do
    GenServer.stop(name)
  end

  @impl ExRaft.Pipeline
  def connect(%__MODULE__{name: name}, peer) do
    GenServer.call(name, {:connect, peer})
  end

  @impl ExRaft.Pipeline
  def pipeout(%__MODULE__{name: name}, to_id, materials) do
    GenServer.cast(name, {:pipeout, to_id, materials})
  end

  @impl ExRaft.Pipeline
  def batch_pipeout(%__MODULE__{name: name}, ms) do
    GenServer.cast(name, {:batch_pipeout, ms})
  end

  # ---------- Server ----------

  @impl GenServer
  def init(%__MODULE__{pipe_delta: pipe_delta, self_id: self_id}) do
    schedule_work(pipe_delta)

    {:ok,
     %{
       self_id: self_id,
       pipe_delta: pipe_delta,
       peers: %{},
       pipelines: %{}
     }}
  end

  @impl GenServer
  def terminate(_reason, _state) do
    :ok
  end

  @impl GenServer
  def handle_call({:connect, %Models.Replica{id: id} = peer}, _from, %{peers: peers, pipelines: pipelines} = state) do
    peer
    |> do_connect()
    |> case do
      :ok ->
        {:reply, :ok, %{state | peers: Map.put_new(peers, id, peer)}}

      {:error, exception} ->
        {:reply, {:error, exception}, state}
    end
  end

  @impl GenServer
  def handle_cast({:pipeout, to_id, materials}, %{peers: peers, pipelines: pipelines} = state) do
    peers
    |> Map.has_key?(to_id)
    |> if do
      pipelines = Map.update(pipelines, to_id, materials, &append_materials(materials, &1))
      {:noreply, %{state | pipelines: pipelines}}
    else
      {:noreply, state}
    end
  end

  def handle_cast({:batch_pipeout, ms}, %{peers: peers, pipelines: pipelines} = state) do
    pipelines =
      Enum.reduce(ms, pipelines, fn {to_id, materials}, acc ->
        peers
        |> Map.has_key?(to_id)
        |> if do
          Map.update(acc, to_id, materials, &append_materials(materials, &1))
        else
          acc
        end
      end)

    {:noreply, %{state | pipelines: pipelines}}
  end

  @impl GenServer
  def handle_info(:work, %{self_id: self_id, pipe_delta: pipe_delta, peers: peers, pipelines: pipelines} = state) do
    pipelines
    |> Task.async_stream(fn {to_id, materials} ->
      do_pipeout(self_id, Map.fetch!(peers, to_id), materials)
    end)
    |> Stream.run()

    schedule_work(pipe_delta)
    {:noreply, %{state | pipelines: %{}}}
  end

  defp schedule_work(delta) do
    Process.send_after(self(), :work, delta)
  end

  defp do_connect(%Models.Replica{erl_node: erl_node} = node) do
    erl_node
    |> Node.connect()
    |> if do
      :ok
    else
      {:error, ExRaft.Exception.new("connect_failed", node)}
    end
  end

  defp do_pipeout(self_id, %Models.Replica{id: to_id, erl_node: node} = replica, materials) do
    node
    |> Node.ping()
    |> case do
      :pong ->
        pkg =
          %Models.Package{
            from_id: self_id,
            to_id: to_id,
            materials: materials
          }

        try do
          replica
          |> Models.Replica.server()
          |> GenServer.cast({:pipeout, Serialize.encode(pkg)})
        catch
          :exit, _ -> {:error, ExRaft.Exception.new("rpc_timeout", replica)}
          other_exception -> raise other_exception
        end

      :pang ->
        {:error, ExRaft.Exception.new("node not connected", node)}
    end
  end

  defp append_materials(materials, olds), do: Enum.reduce(materials, olds, fn material, acc -> [material | acc] end)
end
