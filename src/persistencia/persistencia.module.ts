import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { TypeOrmModule } from '@nestjs/typeorm';

// RabbitMQ (L5 a L8): las tablas de auditoria, notificaciones y mensajes muertos.
import { EventoAuditoria } from './evento-auditoria.entity.js';
import { MensajeMuerto } from './mensaje-muerto.entity.js';
import { Notificacion } from './notificacion.entity.js';

// Kafka (L10): las tablas de lectura de los consumer groups.
import { Alerta } from '../streaming/alertas/alerta.entity.js';
import { VentanaPartidas } from '../streaming/alertas/ventana-partida.entity.js';
import { JuegoCatalogo } from '../streaming/catalogo/juego-catalogo.entity.js';
import { EventoProcesado } from '../streaming/evento-procesado.entity.js';
import { CompraVista } from '../streaming/historial/compra-vista.entity.js';
import { VentaSemana } from '../streaming/historial/venta-semana.entity.js';
import { KpiMinuto } from '../streaming/panel/kpi-minuto.entity.js';

// RabbitMQ (L5 a L8) y Kafka (L10): todas las entidades en un solo lugar.
const ENTIDADES = [
  EventoAuditoria, Notificacion, MensajeMuerto,
  KpiMinuto, EventoProcesado, VentanaPartidas, Alerta, VentaSemana, CompraVista, JuegoCatalogo,
];

@Module({
  imports: [
    TypeOrmModule.forRootAsync({
      imports: [ConfigModule],
      inject: [ConfigService],
      useFactory: (config: ConfigService) => ({
        type: 'postgres' as const,
        host: config.getOrThrow<string>('DB_HOST'),
        port: Number(config.get<string>('DB_PORT', '5432')),
        username: config.getOrThrow<string>('DB_USER'),
        password: config.getOrThrow<string>('DB_PASSWORD'),
        database: config.getOrThrow<string>('DB_NAME'),
        entities: ENTIDADES,
        // ADVERTENCIA: synchronize crea y altera tablas solo. Desarrollo unicamente.
        synchronize: true,
      }),
    }),
    TypeOrmModule.forFeature(ENTIDADES),
  ],
  exports: [TypeOrmModule],
})
export class PersistenciaModule {}
