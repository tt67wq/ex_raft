defmodule ExRaft.Models.Replica do
  @moduledoc """
  Replica
  id => unique id of the replica
  host => host of the replica
  port => port of the replica
  """
  alias ExRaft.Typespecs
  alias ExRaft.Utils.Buffer

  @type t :: %__MODULE__{
          id: non_neg_integer(),
          host: String.t(),
          port: non_neg_integer(),
          match: Typespecs.index_t(),
          next: Typespecs.index_t(),
          active?: boolean(),
          buffer: Buffer.t()
        }

  defstruct id: 0, host: "", port: 0, match: 0, next: 1, active?: true, buffer: nil

  defp erl_node(%__MODULE__{id: id, host: host}), do: :"raft_#{id}@#{host}"

  def server(%__MODULE__{} = replica), do: {ExRaft.Server, erl_node(replica)}

  @spec connect(t()) :: :ok | {:error, ExRaft.Exception.t()}
  def connect(%__MODULE__{} = replica) do
    replica
    |> erl_node()
    |> Node.connect()
    |> case do
      true -> :ok
      false -> {:error, ExRaft.Exception.new("node connect failed", replica)}
      :ignored -> {:error, ExRaft.Exception.new("local node not alive")}
    end
  end

  @spec ping(t()) :: :ok | {:error, ExRaft.Exception.t()}
  def ping(%__MODULE__{} = replica) do
    replica
    |> erl_node()
    |> Node.ping()
    |> case do
      :pong -> :ok
      _ -> {:error, ExRaft.Exception.new("node not connected", replica)}
    end
  end

  def disconnect(%__MODULE__{buffer: buff} = replica) do
    replica
    |> erl_node()
    |> Node.disconnect()

    buff
    |> is_nil()
    |> unless do
      Buffer.stop(buff)
    end
  end

  def start_buffer(%__MODULE__{buffer: nil, id: id} = replica) do
    {:ok, buff} = Buffer.start_link(name: :"buff_#{id}", size: 2048)
    %__MODULE__{replica | buffer: buff}
  end

  def start_buffer(%__MODULE__{} = replica) do
    replica
  end

  def put_msgs(%__MODULE__{buffer: nil}, _msgs), do: raise(ExRaft.Exception, message: "buffer is nil")

  def put_msgs(%__MODULE__{buffer: buff}, msgs) do
    Buffer.put(buff, msgs)
  end

  def get_msgs(%__MODULE__{buffer: nil}), do: []
  def get_msgs(%__MODULE__{buffer: buff}), do: Buffer.take(buff)

  @spec try_update(t(), Typespecs.index_t()) :: {t(), boolean()}
  def try_update(%__MODULE__{match: match, next: next} = peer, index) do
    new_next = max(next, index + 1)
    new_match = max(match, index)
    {%__MODULE__{peer | next: new_next, match: new_match}, match < index}
  end

  @spec make_progress(t(), Typespecs.index_t()) :: {t(), boolean()}
  def make_progress(%__MODULE__{next: next} = peer, index) do
    new_next = max(next, index + 1)
    {%__MODULE__{peer | next: new_next}, next < index + 1}
  end

  @spec make_rollback(t(), Typespecs.index_t()) :: {t(), boolean()}
  def make_rollback(%__MODULE__{next: next} = peer, index) do
    new_next = min(next, index + 1)
    {%__MODULE__{peer | next: new_next}, next > index + 1}
  end

  def set_active(peer) do
    %__MODULE__{peer | active?: true}
  end

  def set_inactive(peer) do
    %__MODULE__{peer | active?: false}
  end
end
