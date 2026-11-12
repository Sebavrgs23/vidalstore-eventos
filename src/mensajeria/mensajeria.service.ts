import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { connect } from 'amqplib';
import type { ChannelModel, ConfirmChannel, Message } from 'amqplib';
import { randomUUID } from 'node:crypto';
import { declararTopologia } from './topologia.js';
import { correlacionValida, detalle, timestampValido, uuidValido } from './reintentos.js';

export interface MetadatosPublicacion {
  eventoId?: string;
  emitidoEn?: string;
  correlationId?: string;
}

type PublicacionPendiente = {
  rechazar: (error: Error) => void;
  devuelto?: Error;
};

@Injectable()
export class MensajeriaService implements OnModuleInit, OnModuleDestroy {
  private readonly log = new Logger(MensajeriaService.name);
  private readonly url: string;
  private conexion!: ChannelModel;
  private readonly pendientes = new Map<string, PublicacionPendiente>();
  canal!: ConfirmChannel;
  abierto = false;

  constructor(config: ConfigService) {
    this.url = config.getOrThrow<string>('RABBITMQ_URL');
  }

  async onModuleInit(): Promise<void> {
    try {
      this.conexion = await connect(this.url);
      this.conexion.on('error', (e: Error) => this.log.error(`conexion: ${e.message}`));
      this.conexion.on('close', () => this.cerrarPendientes());
      this.canal = await this.conexion.createConfirmChannel();
      this.canal.on('close', () => this.cerrarPendientes());
      this.canal.on('error', (e: Error) => this.log.error(`canal: ${e.message}`));
      this.canal.on('return', (mensaje: Message) => {
        const pendiente = this.pendientes.get(mensaje.properties.messageId ?? '');
        if (pendiente) pendiente.devuelto = new Error('Publicacion mandatory sin una cola de destino');
      });
      await this.canal.prefetch(1);
      await declararTopologia(this.canal);
      this.abierto = true;
    } catch (error) {
      await this.onModuleDestroy();
      throw error;
    }
  }

  private cerrarPendientes(): void {
    this.abierto = false;
    for (const pendiente of this.pendientes.values()) {
      pendiente.rechazar(new Error('Canal cerrado antes de confirmar la publicacion'));
    }
    this.pendientes.clear();
  }

  async onModuleDestroy(): Promise<void> {
    this.cerrarPendientes();
    // Intentar ambos cierres, incluso si RabbitMQ ya cerro el canal o la conexion.
    try {
      await this.canal?.close();
    } catch (error) {
      this.log.warn(`cierre de canal: ${detalle(error)}`);
    }
    try {
      await this.conexion?.close();
    } catch (error) {
      this.log.warn(`cierre de conexion: ${detalle(error)}`);
    }
  }

  async publicar(
    exchange: string,
    routingKey: string,
    payload: unknown,
    metadatos: MetadatosPublicacion = {},
  ): Promise<void> {
    if (!this.abierto) throw new Error('El canal RabbitMQ no esta abierto');
    const eventoId = uuidValido(metadatos.eventoId ?? randomUUID(), 'x-evento-id');
    const emitidoEn = timestampValido(metadatos.emitidoEn ?? new Date().toISOString());
    const correlationId = correlacionValida(metadatos.correlationId);
    // Cada intento necesita otro messageId para correlacionar returns concurrentes del mismo evento.
    const messageId = randomUUID();
    const contenido = Buffer.from(JSON.stringify(payload));
    return new Promise<void>((resolver, rechazar) => {
      const pendiente: PublicacionPendiente = { rechazar };
      this.pendientes.set(messageId, pendiente);
      try {
        this.canal.publish(exchange, routingKey, contenido, {
          persistent: true,
          mandatory: true,
          contentType: 'application/json',
          messageId,
          ...(correlationId === undefined ? {} : { correlationId }),
          headers: {
            'x-evento-id': eventoId,
            'x-emitido-en': emitidoEn,
            ...(correlationId === undefined ? {} : { 'x-correlation-id': correlationId }),
          },
        }, (error: Error | null) => {
          if (!this.pendientes.delete(messageId)) return;
          // RabbitMQ entrega basic.return antes del confirm de una publicacion unrouted.
          if (error || pendiente.devuelto) rechazar(error ?? pendiente.devuelto!);
          else resolver();
        });
      } catch (error) {
        this.pendientes.delete(messageId);
        rechazar(error as Error);
      }
      // El boolean de publish solo indica backpressure; nunca confirma la entrega.
    });
  }
}
