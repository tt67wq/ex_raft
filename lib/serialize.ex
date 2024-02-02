defmodule ExRaft.Serialize do
  @moduledoc """
  Serialize behaviors
  """

  @callback encode(any()) :: {:ok, binary()} | {:error, ExRaft.Exception.t()}
  @callback encode!(any()) :: binary()
  @callback decode(binary()) :: {:ok, any()} | {:error, ExRaft.Exception.t()}
  @callback decode!(binary()) :: any()
end
