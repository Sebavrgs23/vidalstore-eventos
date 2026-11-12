import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import type { ConsumeMessage } from 'amqplib';
import { QUEUES, EXCHANGES, ROUTING_KEYS } from '../mensajeria/topologia.js';
import { conReintentos, detalle, esCorreoValido, esDelMensaje, MensajeInvalido, payloadDelMensaje, REINTENTOS, sobreDelMensaje } from '../mensajeria/reintentos.js';
import { MensajeriaService } from '../mensajeria/mensajeria.service.js';

@Injectable()
export class AvisosConsumidor implements OnModuleInit {
  private readonly log = new Logger(AvisosConsumidor.name);

  constructor(
    private readonly mensajeria: MensajeriaService,
    private readonly config: ConfigService,
  ) {}

  async onModuleInit(): Promise<void> {
    await this.mensajeria.canal.consume(
      QUEUES.AVISOS,
      (mensaje) => void this.procesar(mensaje).catch((error) => this.log.error(`canal de avisos: ${detalle(error)}`)),
      { noAck: false },
    );
    this.log.log(`escuchando la cola ${QUEUES.AVISOS}`);
  }

  private async procesar(mensaje: ConsumeMessage | null): Promise<void> {
    if (!mensaje) return;
    const canal = this.mensajeria.canal;

    try {
      const payload = payloadDelMensaje(mensaje);
      const sobre = sobreDelMensaje(mensaje);
      const para = this.destinatario(payload);
      const asunto = `Aviso: ${mensaje.fields.routingKey}`;

      // Una redelivery republica el comando; Correos deduplica por evento origen.
      await conReintentos(
        () => this.mensajeria.publicar(EXCHANGES.COMANDOS.nombre, ROUTING_KEYS.CORREO_ENVIAR, {
          para,
          asunto,
          eventoIdOrigen: sobre.eventoId,
        }, sobre),
        (intento, error) => this.log.warn(`reintento ${intento}/${REINTENTOS.maximo}: ${detalle(error)}`),
      );

      canal.ack(mensaje);
    } catch (error) {
      canal.nack(mensaje, false, false);
      const ruta = esDelMensaje(error) ? 'mensaje invalido' : `RabbitMQ no confirmo en ${REINTENTOS.maximo} intentos`;
      this.log.error(`${ruta}, hacia la DLQ: ${detalle(error)}`);
    }
  }

  private destinatario(payload: Record<string, unknown>): string {
    const explicito = [payload.para, payload.email, payload.destinatario].find(esCorreoValido);
    if (explicito !== undefined) return explicito;
    // Solo un destinatario de laboratorio configurado; nunca inferirlo de sub/JWT.
    const laboratorio = this.config.get<string>('AVISOS_DESTINATARIO');
    if (!esCorreoValido(laboratorio)) {
      throw new MensajeInvalido('No hay destinatario valido ni AVISOS_DESTINATARIO de laboratorio configurado');
    }
    return laboratorio;
  }
}
