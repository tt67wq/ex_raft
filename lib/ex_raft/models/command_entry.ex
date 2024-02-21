defmodule ExRaft.Models.CommandEntry do
  @moduledoc """
  CommandEntry Model
  """

  @type t :: %__MODULE__{
          index: non_neg_integer(),
          command: binary()
        }
  defstruct index: 0, command: <<>>
end
