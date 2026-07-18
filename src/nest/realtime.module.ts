import {
  DynamicModule,
  Inject,
  InjectionToken,
  Module,
  ModuleMetadata,
  OnApplicationShutdown,
  OnModuleInit,
  Provider,
} from '@nestjs/common';
import { RealtimeEngine } from '../engine/engine';
import { EngineConfig, ModelConfig } from '../types';

/** Injection token for the shared `RealtimeEngine` instance. */
export const PG_REALTIME_ENGINE = 'PG_REALTIME_ENGINE';

/**
 * A feature's realtime contribution — how to build the pg-realtime models it owns, with their DI deps.
 * Registered via {@link PgRealtimeModule.forFeature} in a feature module's `imports` and aggregated into
 * the engine by `forRoot`/`forRootAsync`, so domain knowledge (tables, guards, mapRow) stays local.
 */
export interface RealtimeContribution {
  /** Modules providing the `inject` deps (e.g. `TypeOrmModule.forFeature([Entity])`). */
  imports?: ModuleMetadata['imports'];
  /** DI tokens passed to `useFactory`, in order. */
  inject?: InjectionToken[];
  /** Builds the models this feature owns. */
  useFactory: (...deps: any[]) => ModelConfig[];
}

/** Engine config supplied to `forRoot`/`forRootAsync`; `models` is optional — features add theirs via `forFeature`. */
export type RootEngineConfig = Omit<EngineConfig, 'models'> & { models?: ModelConfig[] };

/**
 * Thin NestJS wrapper. Provides a singleton `RealtimeEngine` (injectable via `PG_REALTIME_ENGINE`) and
 * manages its lifecycle. Deliberately transport-free: wire a socket.io gateway in your app with
 * `attachSocketIO(io, engine)` from `pg-realtime/socketio`, so this module's only peer is `@nestjs/common`.
 *
 * Models are gathered from every {@link forFeature} contribution and merged with any `models` on the
 * root config. Each contribution binds a UNIQUE, global DI token rather than a `multi: true` provider:
 * NestJS does not aggregate a multi token across separately-imported modules — it silently keeps only
 * one. Contributions register at module-load, so `forRoot()` must evaluate after the feature modules
 * (importing them first, as an AppModule does, guarantees this).
 */
@Module({})
export class PgRealtimeModule implements OnModuleInit, OnApplicationShutdown {
  private static readonly contributions: symbol[] = [];

  constructor(@Inject(PG_REALTIME_ENGINE) private readonly engine: RealtimeEngine) {}

  /** A feature registers the realtime models it owns. Put in the feature module's `imports`. */
  static forFeature(contribution: RealtimeContribution): DynamicModule {
    const token = Symbol('PG_REALTIME_MODELS');
    PgRealtimeModule.contributions.push(token);
    // A fresh module class per call — NestJS would otherwise dedupe dynamic modules sharing a class.
    class PgRealtimeFeatureModule {}
    return {
      module: PgRealtimeFeatureModule,
      global: true,
      imports: contribution.imports ?? [],
      providers: [
        { provide: token, useFactory: contribution.useFactory, inject: contribution.inject ?? [] },
      ],
      exports: [token],
    };
  }

  static forRoot(config: RootEngineConfig): DynamicModule {
    return PgRealtimeModule.forRootAsync({ useFactory: () => config });
  }

  /**
   * Async variant — resolve the root config from other providers (e.g. an EnvService). `inject`
   * providers are passed to `useFactory` in order; every `forFeature` contribution is appended to the
   * engine's models.
   */
  static forRootAsync(options: {
    useFactory: (...args: any[]) => RootEngineConfig | Promise<RootEngineConfig>;
    inject?: any[];
    imports?: any[];
  }): DynamicModule {
    const baseInject = options.inject ?? [];
    const tokens = [...PgRealtimeModule.contributions];
    const engineProvider: Provider = {
      provide: PG_REALTIME_ENGINE,
      inject: [...baseInject, ...tokens],
      useFactory: async (...args: any[]) => {
        const config = await options.useFactory(...args.slice(0, baseInject.length));
        const contributed = args.slice(baseInject.length) as ModelConfig[][];
        return new RealtimeEngine({ ...config, models: [...(config.models ?? []), ...contributed.flat()] });
      },
    };
    return {
      module: PgRealtimeModule,
      imports: options.imports ?? [],
      providers: [engineProvider],
      exports: [PG_REALTIME_ENGINE],
      global: true,
    };
  }

  async onModuleInit(): Promise<void> {
    await this.engine.start();
  }

  async onApplicationShutdown(): Promise<void> {
    await this.engine.stop();
  }
}
