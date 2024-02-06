defmodule ExRaft do
  @moduledoc """
  Documentation for `ExRaft`.
  """

  alias ExRaft.Models

  @callback apply_log_entries(log_entries :: [Models.LogEntry.t()]) :: :ok

  defmacro __using__(_) do
    quote do
      @behaviour ExRaft

      def apply_log_entries(_log_entries), do: raise("Not implemented")

      defoverridable apply_log_entries: 1

      defdelegate start_link(opt), to: ExRaft.Server, as: :start_link
      defdelegate leader(pid), to: ExRaft.Server, as: :leader
      defdelegate show_cluster_info(pid), to: ExRaft.Server, as: :show_cluster_info

      def child_spec(opts) do
        %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [opts]},
          type: :worker,
          restart: :permanent,
          shutdown: 500
        }
      end
    end
  end
end
