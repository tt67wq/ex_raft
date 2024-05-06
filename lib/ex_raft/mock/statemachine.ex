defmodule ExRaft.Mock.Statemachine do
  @moduledoc """
  Mock statemachine for testing.
  """

  @behaviour ExRaft.Statemachine

  use GenServer

  alias ExRaft.Pb

  require Logger

  @type t :: %__MODULE__{
          name: atom()
        }

  defstruct name: __MODULE__

  @spec new(Keyword.t()) :: t()
  def new(opts \\ []) do
    opts = Keyword.put_new(opts, :name, __MODULE__)
    struct(__MODULE__, opts)
  end

  @impl ExRaft.Statemachine
  def start_link(statemachine: %__MODULE__{name: name} = m) do
    GenServer.start_link(__MODULE__, m, name: name)
  end

  @impl ExRaft.Statemachine
  def stop(%__MODULE__{name: name}) do
    GenServer.stop(name)
  end

  @impl ExRaft.Statemachine
  def update(%__MODULE__{name: name}, commands), do: GenServer.cast(name, {:update, commands})

  @impl ExRaft.Statemachine
  def read(%__MODULE__{name: name}, req) do
    GenServer.call(name, {:read, req})
  end

  @impl ExRaft.Statemachine
  def prepare_snapshot(%__MODULE__{name: name}) do
    GenServer.call(name, :prepare_snapshot)
  end

  @impl ExRaft.Statemachine
  def save_snapshot(%__MODULE__{}, _safe_point, io) do
    IO.write(io, "save snapshot")
  end

  @impl ExRaft.Statemachine
  def load_snapshot(%__MODULE__{}, io_device) do
    IO.read(io_device)
  end

  # --------------- server ---------------

  @impl GenServer
  def init(%__MODULE__{}) do
    {:ok, %{index: 0, term: 0}}
  end

  @impl GenServer
  def terminate(_reason, _state) do
    :ok
  end

  @impl GenServer
  def handle_call({:read, req}, _from, state) do
    Logger.info("read: #{inspect(req)}")
    {:reply, req, state}
  end

  def handle_call(:prepare_snapshot, _from, state) do
    %{index: index, term: term} = state
    {:reply, {index, term, nil}, state}
  end

  @impl GenServer
  def handle_cast({:update, cmds}, _state) do
    Enum.each(cmds, fn %Pb.Entry{} = e -> Logger.info("apply: #{inspect(e)}") end)
    %Pb.Entry{index: index, term: term} = List.last(cmds)
    {:noreply, %{index: index, term: term}}
  end
end
