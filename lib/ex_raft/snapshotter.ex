defmodule ExRaft.Snapshotter do
  @moduledoc """
  A behaviour for implementing custom snapshotters.
  """
  @type t :: struct()
  @type on_start ::
          {:ok, pid()}
          | :ignore
          | {:error, {:already_started, pid()} | term()}

  @callback start_link(snapshotter: t()) :: on_start()
  @callback stop(t()) :: :ok
end
