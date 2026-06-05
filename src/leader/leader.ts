import { LeaderElector } from '../types';

export type { LeaderElector };

/**
 * Default elector: this process is always the leader. Correct for single-instance
 * deployments and tests. Multi-replica deployments supply a real elector (e.g.
 * `RedisLeaderElector` from `pg-realtime/leader/redis`) so exactly one process
 * consumes the replication slot.
 */
export class NoopLeaderElector implements LeaderElector {
  async acquire(): Promise<boolean> {
    return true;
  }
  async renew(): Promise<boolean> {
    return true;
  }
  async release(): Promise<void> {
    /* nothing to release */
  }
}
