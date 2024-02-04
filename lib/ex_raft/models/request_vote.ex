defmodule ExRaft.Models.RequestVote do
  @moduledoc """
  RequestVote Model
  """

  defmodule Req do
    @moduledoc false

    @type t :: %__MODULE__{
            term: non_neg_integer(),
            candidate_id: non_neg_integer(),
            last_log_index: non_neg_integer(),
            last_log_term: non_neg_integer()
          }
    defstruct term: 0, candidate_id: 0, last_log_index: 0, last_log_term: 0
  end

  defmodule Reply do
    @moduledoc false

    @type t :: %__MODULE__{
            term: non_neg_integer(),
            vote_granted: boolean()
          }
    defstruct term: 0, vote_granted: false
  end
end
