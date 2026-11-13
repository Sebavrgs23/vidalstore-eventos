import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { ConsumidorKafka } from '../consumidor-kafka.js';
import { TOPICS } from '../topics.js';
import { CatalogoNegocio } from './catalogo.negocio.js';
import { EventoCatalogoDto } from './evento-catalogo.dto.js';

/**
 * Consumer group vidalstore-catalogo: vidalstore.catalogo, compactado, hacia catalogo_juegos.
 * Lee SIEMPRE desde el offset 0: en cada arranque reconstruye la tabla con el ultimo estado de cada juego.
 */
@Injectable()
export class CatalogoConsumidor extends ConsumidorKafka<EventoCatalogoDto> {
  constructor(config: ConfigService, catalogo: CatalogoNegocio) {
    super(
      config,
      { grupo: 'vidalstore-catalogo', topics: [TOPICS.catalogo], dto: EventoCatalogoDto, desdeElPrincipio: true },
      catalogo,
    );
  }
}
