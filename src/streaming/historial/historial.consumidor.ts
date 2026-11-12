import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { ConsumidorKafka } from '../consumidor-kafka.js';
import { EventoDto } from '../evento.dto.js';
import { TOPICS } from '../topics.js';
import { HistorialNegocio } from './historial.negocio.js';

/** Consumer group vidalstore-historial: solo compras, hacia ventas_semana. Es el que se rebobina. */
@Injectable()
export class HistorialConsumidor extends ConsumidorKafka<EventoDto> {
  constructor(config: ConfigService, historial: HistorialNegocio) {
    super(config, { grupo: 'vidalstore-historial', topics: [TOPICS.compras], dto: EventoDto }, historial);
  }
}
