import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import type { ConsumeMessage } from 'amqplib';
import { Repository } from 'typeorm';
import { QUEUES } from '../mensajeria/topologia.js';
import { MensajeriaService } from '../mensajeria/mensajeria.service.js';
import { conReintentos, detalle, REINTENTOS } from '../mensajeria/reintentos.js';
import { MensajeMuerto } from '../persistencia/mensaje-muerto.entity.js';

@Injectable()
export class CartasMuertasConsumidor implements OnModuleInit {
  private readonly log = new Logger(CartasMuertasConsumidor.name);

  constructor(
    private readonly mensajeria: MensajeriaService,
    @InjectRepository(MensajeMuerto)
    private readonly mensajesMuertos: Repository<MensajeMuerto>,
  ) {}

  async onModuleInit(): Promise<void> {
    const colas = [QUEUES.AVISOS_DLQ, QUEUES.AUDITORIA_DLQ, QUEUES.CORREOS_DLQ];
    for (const cola of colas) {
      await this.mensajeria.canal.consume(cola, (msg) => {
        void this.procesar(cola, msg).catch((error) => this.log.error(`canal DLQ: ${detalle(error)}`));
      }, { noAck: false });
    }
    this.log.log(`escuchando ${colas.length} colas DLQ`);
  }

  private async procesar(colaDlq: string, mensaje: ConsumeMessage | null): Promise<void> {
    if (!mensaje) return;
    const canal = this.mensajeria.canal;

    try {
      const xDeath = mensaje.properties.headers?.['x-death'] as Array<Record<string, unknown>> | undefined;
      const deadLetter = xDeath?.[0];
      const routingKeys = deadLetter?.['routing-keys'] as string[] | undefined;
      const colaOrigen = typeof deadLetter?.queue === 'string' ? deadLetter.queue : colaDlq;
      const intentos = Number(deadLetter?.count ?? 1);
      const motivo = String(deadLetter?.reason ?? 'nack');
      const routingKey = routingKeys?.[0] ?? mensaje.fields.routingKey;

      const registro = this.mensajesMuertos.create({
        colaOrigen,
        routingKey,
        motivo,
        intentos,
        payload: mensaje.content.toString(),
      });
      await conReintentos(
        () => this.mensajesMuertos.save(registro),
        (intento, error) => this.log.warn(`reintento DLQ ${intento}/${REINTENTOS.maximo}: ${detalle(error)}`),
      );

      this.log.log(`[DLQ] origen=${colaOrigen} routingKey=${routingKey} motivo=${motivo} intentos=${intentos}`);
      canal.ack(mensaje);
    } catch (error) {
      this.log.error(`[DLQ ${colaDlq}] falla:`, error);
      // Ya es la cola final: conservar el registro si SQL falla. Sin timer que
      // pueda lanzar un error no manejado sobre un canal cerrado.
      canal.nack(mensaje, false, true);
    }
  }
}
