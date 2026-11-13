import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { ConsumidorKafka } from '../consumidor-kafka.js';
import { EventoDto } from '../evento.dto.js';
import { TOPICS } from '../topics.js';
import { AlertasNegocio } from './alertas.negocio.js';

/** Consumer group vidalstore-alertas: solo actividad, hacia ventanas_partida y alertas. */
@Injectable()
export class AlertasConsumidor extends ConsumidorKafka<EventoDto> {
  constructor(config: ConfigService, alertas: AlertasNegocio) {
    super(config, { grupo: 'vidalstore-alertas', topics: [TOPICS.actividad], dto: EventoDto }, alertas);
  }
}
