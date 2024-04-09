defmodule ExRaft.Models.ReadStatus do
  @moduledoc """
  A struct that represents the status of a read index.
  """

  alias ExRaft.Typespecs

  @type t :: %__MODULE__{
          index: Typespecs.index_t(),
          from: Typespecs.replica_id_t(),
          ctx: Typespecs.read_index_context(),
          confirmed: MapSet.t(Typespecs.replica_id_t())
        }
  defstruct index: 0, from: 0, ctx: {0, 0}, confirmed: MapSet.new()
end
