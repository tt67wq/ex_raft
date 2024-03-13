defmodule ExRaft.Remote.Client do
  @moduledoc """
  Single node client
  """

  @behaviour :gen_statem

  alias ExRaft.Serialize
  alias ExRaft.Utils.Buffer

  require Logger

  @type t :: pid()

  @type client_opts_t :: [id: non_neg_integer(), host: binary()]

  @impl true
  def callback_mode do
    [:state_functions, :state_enter]
  end

  @spec start_link(client_opts_t()) :: {:ok, pid()} | {:error, term()}
  def start_link(opts) do
    :gen_statem.start_link(__MODULE__, opts, [])
  end

  @spec stop(pid()) :: :ok
  def stop(server) do
    :gen_statem.stop(server)
  end

  @spec send_msgs(pid(), list(ExRaft.Typespecs.message_t())) :: :ok
  def send_msgs(server, msgs), do: :gen_statem.cast(server, {:pipeout, msgs})

  @impl true
  def init(opts) do
    {:ok, buff} = Buffer.start_link(name: :"buff_#{opts[:id]}", size: 2048)

    state = %{
      id: opts[:id],
      host: opts[:host],
      buff: buff
    }

    {:ok, :disconnected, state, [{:next_event, :internal, :connect}]}
  end

  @impl true
  def terminate(reason, state, %{id: id, buff: buff}) do
    Logger.warning("ex_raft remote client #{id} terminated: #{inspect(reason)}, current state: #{inspect(state)}")
    Buffer.stop(buff)
  end

  # --------------- disconnected ----------------

  def disconnected(:enter, :connected, _state), do: {:keep_state_and_data, [{{:timeout, :reconnect}, 2000, nil}]}
  def disconnected(:enter, _old_state, _state), do: :keep_state_and_data

  def disconnected(:internal, :connect, state) do
    do_connect(state)
  end

  def disconnected({:timeout, :reconnect}, _, state) do
    do_connect(state)
  end

  def disconnected(:cast, {:pipeout, _msgs}, _) do
    Logger.warning("drop pipeout, node not connected")
    :keep_state_and_data
  end

  def disconnected(event, data, state) do
    ExRaft.Debug.stacktrace(%{
      event: event,
      data: data,
      state: state
    })

    :keep_state_and_data
  end

  # --------------- connected ----------------
  def connected(:enter, :disconnected, _state),
    do: {:keep_state_and_data, [{{:timeout, :ping}, 2000, nil}, {{:timeout, :pipeout}, 200, nil}]}

  def connected(:enter, _, _state), do: :keep_state_and_data

  def connected({:timeout, :ping}, _, state) do
    state
    |> raft_node()
    |> Node.ping()
    |> case do
      :pong -> {:keep_state_and_data, [{{:timeout, :ping}, 2000, nil}]}
      _ -> {:next_state, :disconnected, state}
    end
  end

  def connected({:timeout, :pipeout}, _, state) do
    delay =
      state
      |> pipeout_msgs()
      |> case do
        m when m > 0 -> 50
        0 -> 200
        _ -> 200
      end

    {:keep_state_and_data, [{{:timeout, :pipeout}, delay, nil}]}
  end

  def connected(:cast, {:pipeout, msgs}, %{buff: buff} = state) do
    buff
    |> Buffer.put(msgs)
    |> case do
      :ok ->
        :ok

      {:error, :full} ->
        pipeout_msgs(state)
    end

    :keep_state_and_data
  end

  def connected(event, data, state) do
    ExRaft.Debug.stacktrace(%{
      event: event,
      data: data,
      state: state
    })

    :keep_state_and_data
  end

  # -------------- private ----------------

  defp do_connect(state) do
    Logger.debug("try connect to node #{info(state)}")

    state
    |> raft_node()
    |> Node.connect()
    |> case do
      true -> {:next_state, :connected, state}
      _ -> {:keep_state_and_data, [{{:timeout, :reconnect}, 2000, nil}]}
    end
  end

  defp info(%{id: id, host: host}), do: "replica #{id}@@#{host}"

  defp raft_node(%{id: id, host: host}), do: :"raft_#{id}@#{host}"
  defp remote_server(state), do: {ExRaft.Server, raft_node(state)}

  @spec pipeout_msgs(map()) :: non_neg_integer() | {:error, ExRaft.Exception.t()}
  defp pipeout_msgs(%{buff: buff} = state) do
    buff
    |> Buffer.take()
    |> case do
      [] ->
        0

      msgs ->
        try do
          state
          |> remote_server()
          |> GenServer.cast({:pipeout, Serialize.batch_encode(msgs)})

          Enum.count(msgs)
        catch
          :exit, _ -> {:error, ExRaft.Exception.new("pipeout msg failed"), state}
          other_exception -> raise other_exception
        end
    end
  end
end
