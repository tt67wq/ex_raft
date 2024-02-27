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

  @spec batch_encode([t()]) :: binary()
  def batch_encode(ms) do
    Enum.map_join(ms, fn m ->
      b = encode(m)
      <<byte_size(b)::size(64), b::binary>>
    end)
  end

  @spec batch_decode(binary(), module()) :: [t()]
  def batch_decode(b, module) do
    decode_many(b, module, [])
  end

  defp decode_many(<<>>, _, acc), do: Enum.reverse(acc)

  defp decode_many(<<msg_size::size(64), rest::binary>>, module, acc) do
    <<msg::bytes-size(msg_size), rest::binary>> = rest
    decode_many(rest, module, [decode(msg, module) | acc])
  end
end
