defmodule ExRaft.Models.Replica do
  @moduledoc """
  Replica
  id => unique id of the replica
  host => host of the replica
  port => port of the replica
  """

  @type t :: %__MODULE__{
          id: non_neg_integer(),
          host: String.t(),
          port: non_neg_integer(),
          erl_node: node()
        }

  defstruct id: 0, host: "", port: 0, erl_node: nil

  @spec new(non_neg_integer(), String.t(), non_neg_integer()) :: t()
  def new(id, host, port) do
    %__MODULE__{
      id: id,
      host: host,
      port: port,
      erl_node: :"raft_#{id}@#{host}"
    }
  end

  def server(%__MODULE__{erl_node: node}), do: {ExRaft.Server, node}
end
