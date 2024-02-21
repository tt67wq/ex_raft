defmodule ExRaft.Roles.Common do
  @moduledoc """
  Common behavior for all roles
  """
  alias ExRaft.LogStore
  alias ExRaft.Models
  alias ExRaft.Replica.State

  def do_append_entries(%Models.AppendEntries.Req{prev_log_index: prev_log_index}, %State{commit_index: commit_index})
      when prev_log_index < commit_index do
    {0, commit_index, false}
  end

  def do_append_entries(
        %Models.AppendEntries.Req{prev_log_index: -1, entries: entries, leader_commit: leader_commit},
        %State{log_store_impl: log_store_impl, commit_index: commit_index, last_log_index: last_index}
      ) do
    {:ok, cnt} = LogStore.append_log_entries(log_store_impl, -1, entries)

    if leader_commit > commit_index do
      {cnt, min(leader_commit, last_index), true}
    else
      {cnt, commit_index, false}
    end
  end

  def do_append_entries(
        %Models.AppendEntries.Req{
          prev_log_index: prev_log_index,
          prev_log_term: prev_log_term,
          entries: entries,
          leader_commit: leader_commit
        },
        %State{log_store_impl: log_store_impl, commit_index: commit_index, last_log_index: last_index}
      )
      when prev_log_index <= last_index do
    {:ok, %Models.LogEntry{term: tm}} = LogStore.get_log_entry(log_store_impl, prev_log_index)

    if tm != prev_log_term do
      {0, commit_index, false}
    else
      to_append_entries =
        entries
        |> Enum.with_index(prev_log_index + 1)
        |> Enum.filter(fn {index, _} -> index > last_index end)

      {:ok, cnt} = LogStore.append_log_entries(log_store_impl, last_index, to_append_entries)

      if leader_commit > commit_index do
        {cnt, min(leader_commit, last_index), true}
      else
        {cnt, commit_index, false}
      end
    end
  end

  def do_append_entries(_req, %State{commit_index: commit_index}) do
    {0, commit_index, false}
  end
end
