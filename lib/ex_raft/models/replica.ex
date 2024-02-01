defmodule ExRaft.Models.Replica do
  @moduledoc """
  Replica
  """

  @type t :: %__MODULE__{
          id: non_neg_integer(),
          name: atom()
        }

  defstruct id: 0, name: nil

  @spec new(non_neg_integer(), atom()) :: t()
  def new(id, name) do
    %__MODULE__{
      id: id,
      name: name
    }
  end
end
