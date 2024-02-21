defmodule ExRaft.Models.LogEntry do
  @moduledoc """
  LogEntry Model
  """

  alias ExRaft.Exception
  alias ExRaft.Utils.Uvaint

  @type t :: %__MODULE__{
          term: non_neg_integer(),
          command: binary()
        }

  defstruct term: 0, command: <<>>

  @spec encode(t()) :: binary()
  def encode(%__MODULE__{term: term, command: command}) do
    command_size = byte_size(command)
    command_with_size = Uvaint.encode(command_size) <> command
    <<term::size(64), command_with_size::binary>>
  end

  @spec decode(binary()) :: t()
  def decode(<<term::size(64), command_with_size::binary>>) do
    {command_size, _bytes_read, command} = Uvaint.decode(command_with_size)
    byte_size(command) == command_size || raise Exception, message: "Invalid command size"
    %__MODULE__{term: term, command: command}
  end
end
