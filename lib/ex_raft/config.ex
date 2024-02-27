defmodule ExRaft.Config do
  @moduledoc """
  Config for raft
  """

  @max_msg_batch_size Application.compile_env(:ex_raft, :max_msg_batch_size, 1024)

  def max_msg_batch_size, do: @max_msg_batch_size
end
