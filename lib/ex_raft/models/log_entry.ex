defmodule ExRaft.Models.LogEntry do
  @moduledoc """
  LogEntry Model
  """

  @type t :: %__MODULE__{
          term: non_neg_integer(),
          index: non_neg_integer(),
          command: term()
        }

  defstruct term: 0, index: 0, command: nil
end
