import { CoarseScope, RealtimeRuleGuard, Row } from '../types';

export interface ResolvedModel {
  /** Logical model name clients subscribe by. */
  name: string;
  schema: string;
  table: string;
  /** `${schema}.${table}` — the routing key for bus events. */
  routingKey: string;
  pkCols: string[];
  refetchOnUpdate: boolean;
  mapRow: (raw: Row) => Row;
  guard?: RealtimeRuleGuard<any, Row>;
  coarseScope?: (user: unknown) => CoarseScope;
  /** See `ModelConfig.replicaIdentityFull`. */
  replicaIdentityFull: boolean;
}

export function routingKey(schema: string, table: string): string {
  return `${schema}.${table}`;
}
