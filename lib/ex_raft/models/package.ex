defmodule ExRaft.Models.Package do
  @moduledoc """
  Package Model
  """

  @behaviour ExRaft.Serialize

  alias ExRaft.Models

  @type t :: %__MODULE__{
          from_id: non_neg_integer(),
          to_id: non_neg_integer(),
          materials: [Models.PackageMaterial.t()]
        }

  defstruct from_id: 0, to_id: 0, materials: []

  @impl ExRaft.Serialize
  @spec encode(t()) :: binary()
  def encode(%__MODULE__{from_id: from_id, to_id: to_id, materials: materials}) do
    <<from_id::size(32), to_id::size(32), Models.PackageMaterial.encode_many(materials)::binary>>
  end

  @impl ExRaft.Serialize
  @spec decode(binary()) :: t()
  def decode(<<from_id::size(32), to_id::size(32), body::binary>>) do
    %__MODULE__{
      from_id: from_id,
      to_id: to_id,
      materials: Models.PackageMaterial.decode_many(body)
    }
  end
end

defmodule ExRaft.Models.PackageMaterial do
  @moduledoc false

  @behaviour ExRaft.Serialize

  alias ExRaft.Models

  @type category_t ::
          Models.RequestVote.Req | Models.RequestVote.Reply | Models.AppendEntries.Req | Models.AppendEntries.Reply

  @type t :: %__MODULE__{
          category: category_t(),
          data: struct()
        }

  defstruct category: Models.RequestVote.Req, data: nil

  defp category_const(category) do
    case category do
      Models.RequestVote.Req -> 1
      Models.RequestVote.Reply -> 2
      Models.AppendEntries.Req -> 3
      Models.AppendEntries.Reply -> 4
    end
  end

  defp category_reverse_const(category) do
    case category do
      1 -> Models.RequestVote.Req
      2 -> Models.RequestVote.Reply
      3 -> Models.AppendEntries.Req
      4 -> Models.AppendEntries.Reply
    end
  end

  @impl ExRaft.Serialize
  @spec encode(t()) :: binary()
  def encode(%__MODULE__{category: category, data: data}) do
    <<category_const(category)::size(32), ExRaft.Serialize.encode(data)::binary>>
  end

  @impl ExRaft.Serialize
  @spec decode(binary()) :: t()
  def decode(<<category::size(32), body::binary>>) do
    category = category_reverse_const(category)

    %__MODULE__{
      category: category,
      data: ExRaft.Serialize.decode(body, category)
    }
  end

  @spec encode_many([t()]) :: binary()
  def encode_many(materials) do
    Enum.map_join(materials, fn m ->
      body = encode(m)
      <<byte_size(body)::size(32), body::binary>>
    end)
  end

  @spec decode_many(binary()) :: [t()]
  def decode_many(data), do: decode_many(data, [])
  defp decode_many(<<>>, acc), do: Enum.reverse(acc)

  defp decode_many(<<m_size::size(32), rest::binary>>, acc) do
    <<m::binary-size(m_size), rest::binary>> = rest
    decode_many(rest, [decode(m) | acc])
  end
end
