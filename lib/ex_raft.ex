defmodule ExRaft do
  @moduledoc """
  Documentation for `ExRaft`.
  """

  defmacro __using__(opts) do
    app = Keyword.fetch!(opts, :app)

    quote do
      use GenServer

      def leader do
        GenServer.call(__MODULE__, :leader)
      end

      def show_cluster_info do
        GenServer.call(__MODULE__, :show_cluster_info)
      end

      def start_link(opts) do
        opts = Keyword.put_new_lazy(opts, :name, fn -> __MODULE__ end)
        GenServer.start_link(__MODULE__, [], opts)
      end

      def init(_) do
        {:ok, pid} = unquote(app) |> Application.get_env(:ex_raft) |> ExRaft.Server.start_link()
        {:ok, %{pid: pid}}
      end

      def handle_call(:leader, _from, %{pid: pid} = state) do
        {:reply, ExRaft.Server.leader(pid), state}
      end

      def handle_call(:show_cluster_info, _from, %{pid: pid} = state) do
        {:reply, ExRaft.Server.show_cluster_info(pid), state}
      end
    end
  end
end
