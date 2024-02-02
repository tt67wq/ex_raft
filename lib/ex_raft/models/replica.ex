defmodule ExRaft.Models.Replica do
  @moduledoc """
  Replica
  id => unique id of the replica
  name => name of the replica, eg: node1@localhost
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
