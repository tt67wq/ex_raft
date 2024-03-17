defmodule ExRaft.Guards do
  @moduledoc """
  Some useful guards for raft
  """

  defguard request_vote_req?(msg_type) when msg_type == :request_vote or msg_type == :request_pre_vote

  defguard not_empty(m) when m != 0 and not is_nil(m)
end
