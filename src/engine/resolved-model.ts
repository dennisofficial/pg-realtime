import { CoarseScope, RealtimeRuleGuard, Row } from '../types';

/** A ModelConfig with defaults applied and PK columns resolved against the catalog. */
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
}

export function routingKey(schema: string, table: string): string {
  return `${schema}.${table}`;
}
