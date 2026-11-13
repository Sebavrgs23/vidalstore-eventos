import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import type { ConsumeMessage } from 'amqplib';
import { Repository } from 'typeorm';
import { QUEUES } from '../mensajeria/topologia.js';
import { conReintentos, detalle, esCorreoValido, esDelMensaje, MensajeInvalido, payloadDelMensaje, REINTENTOS, sobreDelMensaje, uuidValido } from '../mensajeria/reintentos.js';
import { MensajeriaService } from '../mensajeria/mensajeria.service.js';
import { Notificacion } from '../persistencia/notificacion.entity.js';

@Injectable()
export class CorreosConsumidor implements OnModuleInit {
  private readonly log = new Logger(CorreosConsumidor.name);

  constructor(
    private readonly mensajeria: MensajeriaService,
    @InjectRepository(Notificacion)
    private readonly notificaciones: Repository<Notificacion>,
  ) {}

  async onModuleInit(): Promise<void> {
    await this.mensajeria.canal.consume(
      QUEUES.CORREOS,
      (mensaje) => void this.procesar(mensaje).catch((error) => this.log.error(`canal de correos: ${detalle(error)}`)),
      { noAck: false },
    );
    this.log.log(`escuchando la cola ${QUEUES.CORREOS}`);
  }

  private async procesar(mensaje: ConsumeMessage | null): Promise<void> {
    if (!mensaje) return;
    const canal = this.mensajeria.canal;

    try {
      const payload = payloadDelMensaje(mensaje);
      const sobre = sobreDelMensaje(mensaje);
      const eventoId = uuidValido(payload.eventoIdOrigen, 'eventoIdOrigen');
      if (eventoId.toLowerCase() !== sobre.eventoId.toLowerCase()) {
        throw new MensajeInvalido('eventoIdOrigen no coincide con x-evento-id');
      }
      const para = this.destinatario(payload);
      if (typeof payload.asunto !== 'string' || !payload.asunto.trim()) throw new MensajeInvalido('El comando no trae asunto');

      // «enviada» representa correo simulado; la unicidad por eventoId evita duplicar redeliveries.
      await conReintentos(
        async () => {
          const notificacion = this.notificaciones.create({
            para,
            asunto: payload.asunto as string,
            estado: 'enviada',
            eventoId,
          });
          await this.notificaciones.save(notificacion);
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

  private destinatario(payload: Record<string, unknown>): string {
    if (!esCorreoValido(payload.para)) throw new MensajeInvalido('El comando no tiene un destinatario valido');
    return payload.para;
  }
}
