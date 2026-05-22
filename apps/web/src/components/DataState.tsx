export function LoadingState() {
  return <div className="state">Loading market data...</div>;
}

export function EmptyState({ label }: { label: string }) {
  return <div className="state">{label}</div>;
}
