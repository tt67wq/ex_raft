defmodule ExRaft.Models.LogEntry do
  @moduledoc """
  LogEntry Model
  """

  @behaviour ExRaft.Serialize

  @type t :: %__MODULE__{
          term: non_neg_integer(),
          command: binary()
        }

  defstruct term: 0, command: <<>>

  @impl ExRaft.Serialize
  def encode(%__MODULE__{term: term, command: command}) do
    <<term::size(64), command::binary>>
  end

  @impl ExRaft.Serialize
  def decode(<<term::size(64), command::binary>>) do
    %__MODULE__{term: term, command: command}
  end

  @spec encode_many([t()]) :: binary()
  def encode_many(entries) do
    Enum.map_join(entries, fn entry ->
      entry_bin = encode(entry)
      <<byte_size(entry_bin)::size(32), entry_bin::binary>>
    end)
  end

  @spec decode_many(binary()) :: [t()]
  def decode_many(data), do: decode_many(data, [])
  defp decode_many(<<>>, acc), do: Enum.reverse(acc)

  defp decode_many(<<entry_size::size(32), rest::binary>>, acc) do
    <<entry_bin::binary-size(entry_size), rest::binary>> = rest
    decode_many(rest, [decode(entry_bin) | acc])
  end
end
