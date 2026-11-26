import { getBudget } from "./api";
import { Meter } from "./Meter";
import { usePoll } from "./util";

const usd = (n: number) => `$${n.toFixed(2)}`;

/** Cloud spend at a glance: what's running against the hourly limit, and this month's total. */
export function Spend() {
  const { data: b } = usePoll(getBudget, 30_000, []);
  return (
    <section className="spend" aria-label="Cloud spend" aria-busy={!b}>
      <span className="spend-label">Cloud</span>
      {b ? (
        <>
          <Meter
            used={b.in_use_usd_per_hour}
            total={b.hourly_usd}
            text={`${usd(b.in_use_usd_per_hour)}/h of ${usd(b.hourly_usd)}/h in use`}
          />
          <span className="spend-month">{usd(b.spent_this_month_usd)} spent this month</span>
        </>
      ) : (
        <>
          <span className="skel skel-phrase" />
          <span className="skel skel-word" />
        </>
      )}
    </section>
  );
}
