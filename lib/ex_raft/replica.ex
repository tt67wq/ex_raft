defmodule ExRaft.Replica do
  @moduledoc """
  Replica
  """

  @behaviour :gen_statem

  alias ExRaft.Models

  # alias ExRaft.Models

  defmodule State do
    @moduledoc """
    Replica GenServer State
    """
    alias ExRaft.Models

    @type t :: %__MODULE__{
            self: Models.Replica.t(),
            peers: list(Models.Replica.t()),
            term: non_neg_integer(),
            election_reset_ts: non_neg_integer(),
            election_timeout: non_neg_integer()
          }

    defstruct self: nil,
              peers: [],
              term: 0,
              election_reset_ts: 0,
              election_timeout: 0
  end

  @type state_t :: :follower | :candidate | :leader

  @impl true
  def callback_mode do
    [:state_functions, :state_enter]
  end

  def start_link(opts) do
    :gen_statem.start_link(__MODULE__, opts, [])
  end

  @impl true
  def init(opts) do
    :rand.seed(:exsss, {100, 101, 102})
    actions = [{{:timeout, :election}, 300, nil}]

    {:ok, :follower,
     %State{
       self: Models.Replica.new(opts[:id], Node.self()),
       peers: Keyword.get(opts, :peers, []),
       term: opts[:term],
       election_reset_ts: System.system_time(:millisecond),
       election_timeout: Enum.random(150..300)
     }, actions}
  end

  def follower(
        {:timeout, :election},
        _,
        %State{election_reset_ts: election_reset_ts, election_timeout: election_timeout} = state
      ) do
    if System.system_time(:millisecond) - election_reset_ts > election_timeout do
      {:next_state, :candidate, state}
    else
      {:keep_state_and_data, [{{:timeout, :election}, 15, nil}]}
    end
  end

  # def follower(:enter, :candidate, %State{term: term} = state) do
  # end
end
