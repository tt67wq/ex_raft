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

    def encode(%__MODULE__{} = req) do
      {:ok, :erlang.term_to_binary(req)}
    end

    def encode!(%__MODULE__{} = req) do
      :erlang.term_to_binary(req)
    end

    def decode(bin) do
      {:ok, :erlang.binary_to_term(bin)}
    end

    def decode!(bin) do
      :erlang.binary_to_term(bin)
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

    def encode(%__MODULE__{} = m) do
      {:ok, :erlang.term_to_binary(m)}
    end

    def encode!(%__MODULE__{} = m) do
      :erlang.term_to_binary(m)
    end

    def decode(bin) do
      {:ok, :erlang.binary_to_term(bin)}
    end

    def decode!(bin) do
      :erlang.binary_to_term(bin)
    end
  end
end
