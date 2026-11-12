import { Injectable } from '@nestjs/common';
import { DataSource } from 'typeorm';
import type { Procesador, Resultado } from '../consumidor-kafka.js';
import type { DatosCompraRealizadaDto, EventoDto } from '../evento.dto.js';
import { minutoDe } from '../ventanas.js';

const CUENTAN = ['partida.iniciada', 'partida.terminada', 'compra.realizada'];

/**
 * Lo que hace vidalstore-panel con un evento valido: una fila en kpi_minuto. Si el evento_id ya estaba, el
 * INSERT no hace nada y se informa como duplicado. No sabe nada de Kafka.
 */
@Injectable()
export class PanelNegocio implements Procesador<EventoDto> {
  constructor(private readonly base: DataSource) {}

  async procesar(e: EventoDto): Promise<Resultado> {
    if (!CUENTAN.includes(e.tipo)) return 'ignorado';
    const filas: unknown[] = await this.base.query(
      `INSERT INTO kpi_minuto (evento_id, minuto, juego_id, tipo, monto_clp) VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (evento_id) DO NOTHING RETURNING evento_id`,
      [e.eventoId, minutoDe(e.emitidoEn), e.juegoId, e.tipo,
        e.tipo === 'compra.realizada' ? (e.datos as DatosCompraRealizadaDto).precioPagadoClp : null],
    );
    return filas.length === 1 ? 'nuevo' : 'duplicado';
  }
}
