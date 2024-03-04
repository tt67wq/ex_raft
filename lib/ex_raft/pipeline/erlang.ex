defmodule ExRaft.Pipeline.Erlang do
  @moduledoc """
  ExRaft.Pipeline implementation using Erlang
  """
  @behaviour ExRaft.Pipeline

  use GenServer

  alias ExRaft.Models
  alias ExRaft.Serialize
  alias ExRaft.Utils.Buffer

  require Logger

  @type t :: %__MODULE__{
          name: atom(),
          pipe_delta: non_neg_integer()
        }

  defstruct name: __MODULE__, pipe_delta: 10

  @spec new(Keyword.t()) :: t()
  def new(opts \\ []) do
    opts =
      opts
      |> Keyword.put_new(:name, __MODULE__)
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
    GenServer.cast(name, {:connect, peer})
  end

  @impl ExRaft.Pipeline
  def disconnect(%__MODULE__{name: name}, peer) do
    GenServer.cast(name, {:disconnect, peer})
  end

  @impl ExRaft.Pipeline
  def pipeout(%__MODULE__{name: name}, messages) do
    GenServer.cast(name, {:pipeout, messages})
  end

  # ---------- Server ----------

  @impl GenServer
  def init(%__MODULE__{pipe_delta: pipe_delta}) do
    schedule_work(pipe_delta)

    {:ok,
     %{
       pipe_delta: pipe_delta,
       peers: %{},
       pipelines: %{}
     }}
  end

  @impl GenServer
  def terminate(_reason, %{peers: peers, pipelines: pipelines}) do
    Enum.each(peers, fn {_, peer} -> Models.Replica.disconnect(peer) end)
    Enum.each(pipelines, fn {_, pipe} -> Buffer.stop(pipe) end)
  end

  @impl GenServer
  def handle_cast({:connect, %Models.Replica{id: id} = peer}, %{peers: peers} = state) do
    peers
    |> Map.get(id)
    |> case do
      nil ->
        do_connect(peer, state)

      _ ->
        {:noreply, state}
    end
  end

  def handle_cast({:disconnect, %Models.Replica{id: id} = peer}, %{peers: peers, pipelines: pipelines} = state) do
    Models.Replica.disconnect(peer)
    {pipe, pipelines} = Map.pop(pipelines, id)
    Buffer.stop(pipe)
    {:noreply, %{state | peers: Map.delete(peers, id), pipelines: pipelines}}
  end

  @impl GenServer
  def handle_cast({:pipeout, msgs}, %{pipelines: pipelines} = state) do
    msgs
    |> Enum.group_by(& &1.to)
    |> Enum.each(fn {to_id, msgs} ->
      %{^to_id => pipe} = pipelines
      Buffer.put(pipe, msgs)
    end)

    {:noreply, state}
  end

  @impl GenServer
  def handle_info(:work, %{pipe_delta: pipe_delta, peers: peers, pipelines: pipelines} = state) do
    pipelines
    |> Task.async_stream(fn {to_id, pipe} ->
      do_pipeout(Map.fetch!(peers, to_id), Buffer.take(pipe))
    end)
    |> Stream.run()

    schedule_work(pipe_delta)
    {:noreply, state}
  end

  defp schedule_work(delta) do
    Process.send_after(self(), :work, delta)
  end

  defp do_pipeout(%Models.Replica{} = replica, messages) do
    replica
    |> Models.Replica.ping()
    |> case do
      :ok ->
        try do
          replica
          |> Models.Replica.server()
          |> GenServer.cast({:pipeout, Serialize.batch_encode(messages)})
        catch
          :exit, _ -> {:error, ExRaft.Exception.new("pipeline timeout", replica)}
          other_exception -> raise other_exception
        end

      err ->
        err
    end
  end

  defp do_connect(%Models.Replica{id: id} = peer, %{peers: peers, pipelines: pipelines} = state) do
    peer
    |> Models.Replica.connect()
    |> case do
      :ok ->
        pipelines
        |> Map.get(id)
        |> is_nil()
        |> unless do
          raise ExRaft.Exception, message: "pipeline already exists", details: id
        end

        {:noreply,
         %{
           state
           | peers: Map.put_new(peers, id, peer),
             pipelines:
               Map.put_new_lazy(pipelines, id, fn ->
                 {:ok, pipe} = Buffer.start_link(name: :"pipeline_#{id}", size: 2048)
                 pipe
               end)
         }}

      {:error, exception} ->
        Logger.error("connect to replica failed: #{ExRaft.Exception.message(exception)}")
        {:noreply, state}
    end
  end
end
