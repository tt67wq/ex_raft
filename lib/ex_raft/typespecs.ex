defmodule ExRaft.Typespecs do
  @moduledoc """
  Typespecs
  """

  @type index_t :: non_neg_integer()
  @type term_t :: non_neg_integer()
  @type replica_id_t :: non_neg_integer()
  @type entry_t :: %ExRaft.Pb.Entry{}
  @type message_t :: %ExRaft.Pb.Message{}
end
