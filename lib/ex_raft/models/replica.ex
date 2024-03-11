defmodule ExRaft.Models.Replica do
  @moduledoc """
  Replica
  id => unique id of the replica
  host => host of the replica
  port => port of the replica
  """
  alias ExRaft.Remote.Client
  alias ExRaft.Typespecs

  require Logger

  @type t :: %__MODULE__{
          id: non_neg_integer(),
          host: String.t(),
          port: non_neg_integer(),
          match: Typespecs.index_t(),
          next: Typespecs.index_t(),
          active?: boolean(),
          client: ExRaft.Client.t()
        }

  defstruct id: 0, host: "", port: 0, match: 0, next: 1, active?: true, client: nil

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

  @spec connect(t()) :: t()
  def connect(%__MODULE__{id: id, host: host, client: nil} = m) do
    {:ok, cli} = Client.start_link(id: id, host: host)
    %__MODULE__{m | client: cli}
  end

  def connect(m), do: m

  @spec disconnect(t()) :: t()
  def disconnect(%__MODULE__{client: cli}) do
    Client.stop(cli)
    %__MODULE__{client: nil}
  end

  def disconnect(m), do: m

  def send_msgs(%__MODULE__{id: id, client: nil}, _msgs), do: Logger.warning("peer #{id} not connected, ignore msgs")
  def send_msgs(%__MODULE__{client: cli}, msgs) do
    Client.send_msgs(cli, msgs)
  end
end
