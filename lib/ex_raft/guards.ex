defmodule ExRaft.Guards do
  @moduledoc """
  Some useful guards for raft
  """
  defguard is_request_vote_req(msg_type) when msg_type == :request_vote or msg_type == :request_pre_vote
end
