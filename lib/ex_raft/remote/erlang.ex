defmodule ExRaft.Remote.Erlang do
  @moduledoc """
  ExRaft.Remote implementation using Erlang
  """
  @behaviour ExRaft.Remote

  use GenServer

  alias ExRaft.Models
  alias ExRaft.Pb
  alias ExRaft.Serialize

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

  @impl ExRaft.Remote
  def start_link(remote: %__MODULE__{name: name} = m) do
    GenServer.start_link(__MODULE__, m, name: name)
  end

  @impl ExRaft.Remote
  def stop(%__MODULE__{name: name}) do
    GenServer.stop(name)
  end

  @impl ExRaft.Remote
  def connect(%__MODULE__{name: name}, peer) do
    GenServer.cast(name, {:connect, peer})
  end

  @impl ExRaft.Remote
  def disconnect(%__MODULE__{name: name}, peer) do
    GenServer.cast(name, {:disconnect, peer})
  end

  @impl ExRaft.Remote
  def pipeout(%__MODULE__{name: name}, messages) do
    GenServer.cast(name, {:pipeout, messages})
  end

  # ---------- Server ----------

  @impl GenServer
  def init(%__MODULE__{pipe_delta: pipe_delta}) do
    Process.send_after(self(), :send_msg, pipe_delta)

    {:ok,
     %{
       pipe_delta: pipe_delta,
       peers: %{}
     }}
  end

  @impl GenServer
  def terminate(_reason, %{peers: peers}) do
    Enum.each(peers, fn {_, peer} -> Models.Replica.disconnect(peer) end)
  end

  @impl GenServer
  def handle_cast({:connect, %Models.Replica{} = peer}, %{} = state) do
    do_connect(peer, state)
  end

  def handle_cast({:disconnect, %Models.Replica{id: id} = peer}, %{peers: peers} = state) do
    Models.Replica.disconnect(peer)
    {:noreply, %{state | peers: Map.delete(peers, id)}}
  end

  @impl GenServer
  def handle_cast({:pipeout, [%Pb.Message{to: to_id} = msg]}, %{peers: peers} = state) do
    case peers do
      %{^to_id => peer} ->
        Models.Replica.put_msgs(peer, [msg])

      _ ->
        Logger.warning("peer #{to_id} not connected, ignore")
    end

    {:noreply, state}
  end

  def handle_cast({:pipeout, msgs}, %{peers: peers} = state) do
    msgs
    |> Enum.group_by(& &1.to)
    |> Enum.each(fn {to_id, msgs} ->
      case peers do
        %{^to_id => peer} ->
          Models.Replica.put_msgs(peer, msgs)

        _ ->
          Logger.warning("peer #{to_id} not connected, ignore")
      end
    end)

    {:noreply, state}
  end

  @impl GenServer
  def handle_info(:send_msg, %{pipe_delta: pipe_delta, peers: peers} = state) do
    peers
    |> Task.async_stream(fn {_, peer} ->
      do_pipeout(peer, Models.Replica.get_msgs(peer))
    end)
    |> Stream.run()

    Process.send_after(self(), :send_msg, pipe_delta)
    {:noreply, state}
  end

  def handle_info({:retry_connect, peer}, state) do
    do_connect(peer, state)
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
          :exit, _ -> {:error, ExRaft.Exception.new("pipeout timeout", replica)}
          other_exception -> raise other_exception
        end

      err ->
        err
    end
  end

  defp do_connect(%Models.Replica{id: id} = peer, %{peers: peers} = state) do
    Logger.debug("do_connect: peer: #{inspect(peer)}, peers: #{inspect(peers)}")

    peer
    |> Models.Replica.connect()
    |> case do
      :ok ->
        nil

      {:error, exception} ->
        Logger.error("connect to replica failed: #{ExRaft.Exception.message(exception)}")
        Process.send_after(self(), {:retry_connect, peer}, 2000)
    end

    {:noreply, %{state | peers: Map.put_new_lazy(peers, id, fn -> Models.Replica.start_buffer(peer) end)}}
  end
end
