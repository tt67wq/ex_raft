defmodule ExRaft.Models.AppendEntries do
  @moduledoc """
  AppendEntries Model
  """
  defmodule Req do
    @moduledoc false

    @type t :: %__MODULE__{
            term: non_neg_integer(),
            leader_id: non_neg_integer(),
            prev_log_index: non_neg_integer(),
            prev_log_term: non_neg_integer(),
            entries: [ExRaft.Models.LogEntry.t()],
            leader_commit: non_neg_integer()
          }

    defstruct term: 0, leader_id: 0, prev_log_index: 0, prev_log_term: 0, entries: [], leader_commit: 0
  end

  defmodule Reply do
    @moduledoc false

    @type t :: %__MODULE__{
            term: non_neg_integer(),
            success: boolean()
          }
    defstruct term: 0, success: false
  end
end
