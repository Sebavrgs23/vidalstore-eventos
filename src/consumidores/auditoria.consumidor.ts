import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import type { ConsumeMessage } from 'amqplib';
import { Repository } from 'typeorm';
import { QUEUES } from '../mensajeria/topologia.js';
import { conReintentos, detalle, esDelMensaje, MensajeInvalido, payloadDelMensaje, REINTENTOS, sobreDelMensaje, uuidValido } from '../mensajeria/reintentos.js';
import { MensajeriaService } from '../mensajeria/mensajeria.service.js';
import { EventoAuditoria } from '../persistencia/evento-auditoria.entity.js';

@Injectable()
export class AuditoriaConsumidor implements OnModuleInit {
  private readonly log = new Logger(AuditoriaConsumidor.name);

  constructor(
    private readonly mensajeria: MensajeriaService,
    @InjectRepository(EventoAuditoria)
    private readonly eventos: Repository<EventoAuditoria>,
  ) {}

  async onModuleInit(): Promise<void> {
    await this.mensajeria.canal.consume(
      QUEUES.AUDITORIA,
      (mensaje) => void this.procesar(mensaje).catch((error) => this.log.error(`canal de auditoria: ${detalle(error)}`)),
      { noAck: false },
    );
    this.log.log(`escuchando la cola ${QUEUES.AUDITORIA}`);
  }

  private async procesar(mensaje: ConsumeMessage | null): Promise<void> {
    if (!mensaje) return;
    const canal = this.mensajeria.canal;

    try {
      const payload = payloadDelMensaje(mensaje);
      const { eventoId, emitidoEn } = sobreDelMensaje(mensaje);
      const routingKey = mensaje.fields.routingKey;
      const usuarioSub = mensaje.properties.headers?.['x-usuario-sub'] ?? null;
      if (usuarioSub !== null && (typeof usuarioSub !== 'string' || !usuarioSub.trim())) {
        throw new MensajeInvalido('El mensaje no trae un x-usuario-sub valido');
      }

      await conReintentos(
        async () => {
          const evento = this.eventos.create({
            eventoId,
            routingKey,
            usuarioSub,
            payload,
            emitidoEn: new Date(emitidoEn),
          });
          await this.eventos.save(evento);
        },
        (intento, error) => this.log.warn(`reintento ${intento}/${REINTENTOS.maximo}: ${detalle(error)}`),
      );

      canal.ack(mensaje);
    } catch (error) {
      if ((error as { code?: string } | null)?.code === '23505') {
        canal.ack(mensaje);
        this.log.warn(`reentrega del evento ${this.eventoIdSeguro(mensaje)}: ya estaba guardado, no se duplica`);
        return;
      }
      canal.nack(mensaje, false, false);
      const ruta = esDelMensaje(error) ? 'mensaje invalido' : `la base no respondio en ${REINTENTOS.maximo} intentos`;
      this.log.error(`${ruta}, hacia la DLQ: ${detalle(error)}`);
    }
  }

  private eventoIdSeguro(msg: ConsumeMessage): string {
    try {
      return uuidValido(msg.properties.headers?.['x-evento-id'], 'x-evento-id');
    } catch {
      return 'desconocido';
    }
  }
}
