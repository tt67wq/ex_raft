defmodule ExRaft.Core.Free do
  @moduledoc """
  Free Role Module
  """
  alias ExRaft.Core.Common
  alias ExRaft.Pb

  require Logger

  # -------------------- pipein msg handle --------------------

  # ----------------- fallback -----------------

  def free(event, data, state) do
    Logger.debug("fallback!", %{
      role: :free,
      event: event,
      data: data,
      state: state
    })

    :keep_state_and_data
  end
end
