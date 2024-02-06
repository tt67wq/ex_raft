defmodule ExRaft.Models.LogEntry do
  @moduledoc """
  LogEntry Model
  """

  alias ExRaft.Utils.Uvaint

  @type t :: %__MODULE__{
          index: non_neg_integer(),
          term: non_neg_integer(),
          command: binary()
        }

  defstruct index: 0, term: 0, command: <<>>

  @spec encode(t()) :: binary()
  def encode(%__MODULE__{index: index, term: term, command: command}) do
    command_size = byte_size(command)
    command_with_size = Uvaint.encode(command_size) <> command
    <<index::size(64), term::size(64), command_with_size::binary>>
  end

  @spec decode(binary()) :: t()
  def decode(<<index::size(64), term::size(64), command_with_size::binary>>) do
    {command_size, _bytes_read, command} = Uvaint.decode(command_with_size)
    byte_size(command) == command_size || raise ArgumentError, "Invalid command size"
    %__MODULE__{index: index, term: term, command: command}
  end
end
