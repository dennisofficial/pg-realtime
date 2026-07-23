import {
  DynamicModule,
  Inject,
  Injectable,
  InjectionToken,
  Module,
  ModuleMetadata,
  type OnApplicationBootstrap,
  type OnApplicationShutdown,
} from '@nestjs/common';
import { HttpAdapterHost } from '@nestjs/core';
import type { Server, Socket } from 'socket.io';
import { Server as SocketIOServer } from 'socket.io';
import type { DataSource } from 'typeorm';
import { RealtimeEngine } from '../engine/engine';
import { attachMux, type Principal, type QuerySpec } from '../socketio/mux';
import { superjsonParser } from '../socketio/parser-superjson';
import { PG_REALTIME_ENGINE, PgRealtimeModule, type RootEngineConfig } from '../nest/realtime.module';
import type { Logger, ModelConfig, Row } from '../types';
import { buildRealtimeModels } from './build-realtime-models';
import { resolveResource } from './resolve-resource';
import { RealtimeResourceRegistry } from './realtime-resource.registry';

/**
 * Everything the generic gateway/discovery needs, resolved once at boot. Every plug except
 * `dataSource`/`engine` is RLS/auth/DB-specific and MUST come from the host app — this module
 * (and the rest of `@workspace/pg-realtime`) stays free of any dependency on
 * `@workspace/nestjs-rls` or a particular auth scheme.
 */
export interface RealtimeNestConfig<P extends Principal = Principal> {
  /** TypeORM DataSource to discover `@Realtime`-decorated entities off. */
  dataSource: DataSource;
  /** Slot/publication/bus/leader config for the underlying streaming engine. */
  engine: RootEngineConfig;
  /** Resolves the authenticated principal from a socket handshake. Throw/reject to reject the connection. */
  authenticate: (handshake: Socket['handshake']) => P | Promise<P>;
  /** The app's CLS/RLS seeding wrapper — every composed-resource `load()` runs inside this. */
  withPrincipalContext: <T>(principal: P, fn: () => Promise<T> | T) => Promise<T>;
  /** Discovery plug: the `propertyName -> outputName` map of columns exposed for an entity. */
  resolveExposed: (target: Function) => Map<string, string>;
  /** Discovery plug: builds the row-level auth guard for an entity. */
  buildGuard: (target: Function) => ModelConfig['guard'];
  /**
   * Mode B windowed-query loader plug: an RLS-scoped `ORDER BY`/`LIMIT` fetch. Optional — omit
   * to disable windowed (sort/limit/offset/after) subscriptions on plain models.
   */
  scopedFind?: (model: string, spec: QuerySpec, principal: P) => Promise<Array<{ pk: string; row: Row }>>;
  path?: string;
  cors?: { origin: string | string[]; credentials?: boolean };
  logger?: Logger;
}

const REALTIME_NEST_CONFIG = Symbol('REALTIME_NEST_CONFIG');

/**
 * Generic socket.io gateway: binds `attachMux` onto the host's HTTP server once it's listening,
 * dispatching every subscribe through {@link resolveResource} (named composed resources, Mode B
 * windowed entities, or falling through to the engine's plain streaming subscription).
 */
@Injectable()
class RealtimeGateway implements OnApplicationBootstrap, OnApplicationShutdown {
  private io: Server | null = null;

  constructor(
    private readonly httpAdapterHost: HttpAdapterHost,
    @Inject(PG_REALTIME_ENGINE) private readonly engine: RealtimeEngine,
    @Inject(REALTIME_NEST_CONFIG) private readonly config: RealtimeNestConfig,
    private readonly registry: RealtimeResourceRegistry,
  ) {}

  // Runs after `app.listen()` — the underlying http.Server must already be listening before we
  // bind socket.io onto it.
  onApplicationBootstrap(): void {
    const httpServer = this.httpAdapterHost.httpAdapter.getHttpServer();
    this.io = new SocketIOServer(httpServer, {
      parser: superjsonParser,
      path: this.config.path,
      cors: this.config.cors,
    });

    attachMux(this.io, this.engine, {
      authenticate: (handshake) => this.config.authenticate(handshake),
      resolveResource: (name, spec, principal) =>
        resolveResource(name, spec, principal, {
          registry: this.registry,
          scopedFind: this.config.scopedFind,
          withPrincipalContext: this.config.withPrincipalContext,
        }),
      logger: this.config.logger,
    });
  }

  async onApplicationShutdown(): Promise<void> {
    if (!this.io) return;
    await this.io.close();
  }
}

/**
 * Internal-only: provides the resolved `RealtimeNestConfig` as a global token so both
 * `PgRealtimeModule.forRootAsync` (models) and `RealtimeGateway` (everything else) can inject
 * it without an explicit import edge between them — mirrors how `PgRealtimeModule.forFeature`
 * contributions register globally in this same package.
 */
@Module({})
class RealtimeNestConfigModule {
  static forRootAsync(options: {
    imports?: ModuleMetadata['imports'];
    inject?: InjectionToken[];
    useFactory: (...args: any[]) => RealtimeNestConfig | Promise<RealtimeNestConfig>;
  }): DynamicModule {
    return {
      module: RealtimeNestConfigModule,
      global: true,
      imports: options.imports ?? [],
      providers: [
        { provide: REALTIME_NEST_CONFIG, useFactory: options.useFactory, inject: options.inject ?? [] },
      ],
      exports: [REALTIME_NEST_CONFIG],
    };
  }
}

/**
 * Packages the whole socket-gateway glue (discovery → engine → resource registry → gateway)
 * behind one `forRootAsync({...plugs...})`. Deliberately cycle-free: everything RLS/DB/auth
 * specific comes in as a plug on {@link RealtimeNestConfig} — this module never imports
 * `@workspace/nestjs-rls` or any host-specific code.
 */
@Module({})
export class RealtimeNestModule {
  static forRootAsync(options: {
    imports?: ModuleMetadata['imports'];
    inject?: InjectionToken[];
    useFactory: (...args: any[]) => RealtimeNestConfig | Promise<RealtimeNestConfig>;
  }): DynamicModule {
    const configModule = RealtimeNestConfigModule.forRootAsync(options);
    const engineModule = PgRealtimeModule.forRootAsync({
      inject: [REALTIME_NEST_CONFIG],
      useFactory: (config: RealtimeNestConfig) => ({
        ...config.engine,
        models: [
          ...(config.engine.models ?? []),
          ...buildRealtimeModels(config.dataSource, {
            resolveExposed: config.resolveExposed,
            buildGuard: config.buildGuard,
          }),
        ],
      }),
    });

    return {
      module: RealtimeNestModule,
      global: true,
      imports: [configModule, engineModule],
      providers: [RealtimeResourceRegistry, RealtimeGateway],
      // Re-export the engine MODULE (not the bare token) so importers can inject the
      // engine — Nest forbids exporting a token owned by an imported module.
      exports: [RealtimeResourceRegistry, PgRealtimeModule],
    };
  }
}
