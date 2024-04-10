defmodule ExRaft.Models.ReadStatus do
  @moduledoc """
  A struct that represents the status of a read index.
  """

  alias ExRaft.Typespecs

  @type t :: %__MODULE__{
          index: Typespecs.index_t(),
          from: Typespecs.replica_id_t(),
          ref: Typespecs.ref(),
          confirmed: MapSet.t(Typespecs.replica_id_t())
        }
  defstruct index: 0, from: 0, ref: "", confirmed: MapSet.new()
end
