defmodule ExRaft.Models.RequestVote do
  @moduledoc """
  RequestVote Model
  """

  defmodule Req do
    @moduledoc false

    @behaviour ExRaft.Serialize

    @type t :: %__MODULE__{
            term: non_neg_integer(),
            candidate_id: non_neg_integer(),
            last_log_index: non_neg_integer(),
            last_log_term: non_neg_integer()
          }
    defstruct term: 0, candidate_id: 0, last_log_index: 0, last_log_term: 0

    @impl ExRaft.Serialize
    def encode(%__MODULE__{
          term: term,
          candidate_id: candidate_id,
          last_log_index: last_log_index,
          last_log_term: last_log_term
        }) do
      <<term::size(64), candidate_id::size(32), last_log_index::size(64), last_log_term::size(64)>>
    end

    @impl ExRaft.Serialize
    def decode(<<term::size(64), candidate_id::size(32), last_log_index::size(64), last_log_term::size(64)>>) do
      %__MODULE__{
        term: term,
        candidate_id: candidate_id,
        last_log_index: last_log_index,
        last_log_term: last_log_term
      }
    end
  end

  defmodule Reply do
    @moduledoc false

    @behaviour ExRaft.Serialize

    @type t :: %__MODULE__{
            term: non_neg_integer(),
            vote_granted: boolean()
          }
    defstruct term: 0, vote_granted: false

    @impl ExRaft.Serialize
    def encode(%__MODULE__{term: term, vote_granted: vote_granted}) do
      vote_granted_int = (vote_granted && 1) || 0
      <<term::size(64), vote_granted_int::size(8)>>
    end

    @impl ExRaft.Serialize
    def decode(<<term::size(64), vote_granted_int::size(8)>>) do
      %__MODULE__{term: term, vote_granted: vote_granted_int == 1}
    end
  end
end
