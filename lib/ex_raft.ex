defmodule ExRaft do
  @moduledoc """
  Documentation for `ExRaft`.
  """

  defmacro __using__(_opts) do
    quote do
      use GenServer

      # client
      def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

      def leader, do: GenServer.call(__MODULE__, :leader)

      # server
      def init(opts) do
        {:ok, pid} = ExRaft.Server.start_link(opts)
        {:ok, %{raft_pid: pid}}
      end

      def handle_call(:leader, _from, %{raft_pid: pid} = state) do
        {:reply, ExRaft.Server.leader(pid), state}
      end
    end
  end
end
