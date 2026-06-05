import {
  DynamicModule,
  Inject,
  Module,
  OnApplicationShutdown,
  OnModuleInit,
  Provider,
} from '@nestjs/common';
import { RealtimeEngine } from '../engine/engine';
import { EngineConfig } from '../types';

/** Injection token for the shared `RealtimeEngine` instance. */
export const PG_REALTIME_ENGINE = 'PG_REALTIME_ENGINE';

/**
 * Thin NestJS wrapper. Provides a singleton `RealtimeEngine` (injectable via
 * `PG_REALTIME_ENGINE`) and manages its lifecycle. Deliberately transport-free:
 * wire a socket.io gateway in your app with `attachSocketIO(io, engine)` from
 * `pg-realtime/socketio`, so this module's only peer is `@nestjs/common`.
 */
@Module({})
export class PgRealtimeModule implements OnModuleInit, OnApplicationShutdown {
  constructor(@Inject(PG_REALTIME_ENGINE) private readonly engine: RealtimeEngine) {}

  static forRoot(config: EngineConfig): DynamicModule {
    const engineProvider: Provider = {
      provide: PG_REALTIME_ENGINE,
      useFactory: () => new RealtimeEngine(config),
    };
    return {
      module: PgRealtimeModule,
      providers: [engineProvider],
      exports: [PG_REALTIME_ENGINE],
      global: true,
    };
  }

  /**
   * Async variant — resolve the EngineConfig from other providers (e.g. an
   * EnvService). `inject` providers are passed to `useFactory` in order.
   */
  static forRootAsync(options: {
    useFactory: (...args: any[]) => EngineConfig | Promise<EngineConfig>;
    inject?: any[];
    imports?: any[];
  }): DynamicModule {
    const engineProvider: Provider = {
      provide: PG_REALTIME_ENGINE,
      useFactory: async (...args: any[]) => new RealtimeEngine(await options.useFactory(...args)),
      inject: options.inject ?? [],
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
