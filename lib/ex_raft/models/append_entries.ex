defmodule ExRaft.Models.AppendEntries do
  @moduledoc """
  AppendEntries Model
  """
  defmodule Req do
    @moduledoc false

    @behaviour ExRaft.Serialize

    alias ExRaft.Models

    @type t :: %__MODULE__{
            term: non_neg_integer(),
            leader_id: non_neg_integer(),
            prev_log_index: non_neg_integer(),
            prev_log_term: non_neg_integer(),
            entries: [ExRaft.Models.LogEntry.t()],
            leader_commit: non_neg_integer()
          }

    defstruct term: 0, leader_id: 0, prev_log_index: 0, prev_log_term: 0, entries: [], leader_commit: 0

    @impl ExRaft.Serialize
    def encode(%__MODULE__{
          term: term,
          leader_id: leader_id,
          prev_log_index: prev_log_index,
          prev_log_term: prev_log_term,
          entries: entries,
          leader_commit: leader_commit
        }) do
      entries_body = Models.LogEntry.encode_many(entries)

      <<
        term::size(64),
        leader_id::size(32),
        prev_log_index::size(64),
        prev_log_term::size(64),
        leader_commit::size(64),
        entries_body::binary
      >>
    end

    @impl ExRaft.Serialize
    def decode(
          <<term::size(64), leader_id::size(32), prev_log_index::size(64), prev_log_term::size(64),
            leader_commit::size(64), entries_body::binary>>
        ) do
      entries = Models.LogEntry.decode_many(entries_body)

      %__MODULE__{
        term: term,
        leader_id: leader_id,
        prev_log_index: prev_log_index,
        prev_log_term: prev_log_term,
        entries: entries,
        leader_commit: leader_commit
      }
    end
  end

  defmodule Reply do
    @moduledoc false

    @behaviour ExRaft.Serialize

    alias ExRaft.Typespecs

    @type t :: %__MODULE__{
            success: boolean(),
            term: non_neg_integer(),
            log_index: Typespecs.index_t()
          }
    defstruct success: false, term: 0, log_index: -1

    @impl ExRaft.Serialize
    def encode(%__MODULE__{success: success, term: term, log_index: log_index}) do
      success_int = (success && 1) || 0
      <<success_int::size(8), term::size(64), log_index::size(64)>>
    end

    @impl ExRaft.Serialize
    def decode(<<success_int::size(8), term::size(64), log_index::size(64)>>) do
      %__MODULE__{success: success_int == 1, term: term, log_index: log_index}
    end
  end
end
