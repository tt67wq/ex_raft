defimpl Inspect, for: ExRaft.Pb.Entry do
  def inspect(entry, _opts) do
    "#ExRaft.Pb.Entry<#{entry.term}:#{entry.index}, #{entry.type}>"
  end
end

defimpl Inspect, for: ExRaft.Pb.ConfigChange do
  def inspect(%ExRaft.Pb.ConfigChange{type: type, replica_id: id, addr: addr}, _opts) do
    "#ExRaft.Pb.ConfigChange<#{type}:#{id}:#{addr}>"
  end
end

defimpl Inspect, for: ExRaft.Pb.Message do
  def inspect(%ExRaft.Pb.Message{type: type, from: from, to: to, term: term}, _opts) do
    "#ExRaft.Pb.Message<type:#{type}, #{from}==>#{to}, term:#{term}>"
  end
end
