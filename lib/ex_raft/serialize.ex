defmodule ExRaft.Serialize do
  @moduledoc """
  Serialize behaviors
  """

  @type t :: struct()

  @callback encode(t :: t()) :: binary()
  @callback decode(binary()) :: t()

  @spec encode(t()) :: binary()
  def encode(%module{} = m), do: apply(module, :encode, [m])

  @spec decode(binary(), module()) :: t()
  def decode(binary, module), do: apply(module, :decode, [binary])
end
