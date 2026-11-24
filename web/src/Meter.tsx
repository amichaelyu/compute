/** A used-of-total bar with its numbers: `[██████░░░░] 8 of 12 GB`. */
export function Meter({ used, total, text, tone = "use" }: { used: number; total: number; text: string; tone?: "use" | "gpu" | "time" }) {
  const frac = total > 0 ? Math.min(1, Math.max(0, used / total)) : 0;
  const level = frac >= 0.9 ? " meter-high" : "";
  return (
    <span className="meter-row">
      <span
        className={`meter meter-${tone}${level}`}
        role="meter"
        aria-valuemin={0}
        aria-valuemax={total}
        aria-valuenow={used}
        aria-valuetext={text}
      >
        <span style={{ width: `${frac * 100}%` }} />
      </span>
      <span className="meter-text">{text}</span>
    </span>
  );
}
