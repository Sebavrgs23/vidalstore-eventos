import { Injectable, Logger } from '@nestjs/common';
import { DataSource } from 'typeorm';
import type { Procesador, Resultado } from '../consumidor-kafka.js';
import type { DatosCompraRealizadaDto, DatosLicenciaRevocadaDto, EventoDto } from '../evento.dto.js';

const GRUPO = 'vidalstore-historial';

/**
 * Lo que hace vidalstore-historial con un evento valido, en UNA transaccion con eventos_procesados:
 * - compra.realizada: guarda la compra (semana y juego) y suma una compra y un activa.
 * - licencia.revocada: busca su compra por licenciaId, la marca revocada y, en la semana y el juego DE LA
 *   COMPRA, suma una revocada y resta un activa. La semana es el lunes, en hora de Chile, de emitidoEn.
 */
@Injectable()
export class HistorialNegocio implements Procesador<EventoDto> {
  private readonly log = new Logger(GRUPO);

  constructor(private readonly base: DataSource) {}

  async procesar(e: EventoDto): Promise<Resultado> {
    const { licenciaId } = e.datos as DatosCompraRealizadaDto | DatosLicenciaRevocadaDto;
    return this.base.transaction(async (tx) => {
      const nuevo: unknown[] = await tx.query(
        `INSERT INTO eventos_procesados (grupo, evento_id) VALUES ($1, $2)
         ON CONFLICT DO NOTHING RETURNING evento_id`,
        [GRUPO, e.eventoId],
      );
      if (nuevo.length === 0) return 'duplicado';

      if (e.tipo === 'compra.realizada') {
        const [p] = await tx.query<{ semana: string }[]>(
          `INSERT INTO compras_vistas (licencia_id, semana, juego_id, monto, revocada)
           VALUES ($1, date_trunc('week', $2::timestamptz AT TIME ZONE 'America/Santiago')::date, $3, $4, false)
           ON CONFLICT (licencia_id) DO NOTHING RETURNING semana::text AS semana`,
          [licenciaId, e.emitidoEn, e.juegoId, (e.datos as DatosCompraRealizadaDto).precioPagadoClp],
        );
        if (!p) {
          this.log.warn(`compra ${licenciaId} ya registrada con otro evento: no suma`);
          return 'nuevo';
        }
        await tx.query(
          `INSERT INTO ventas_semana (semana, juego_id, compras, revocadas, activas, monto_total, monto_neto)
           VALUES ($1, $2, 1, 0, 1, $3, $3)
           ON CONFLICT (semana, juego_id) DO UPDATE
             SET compras = ventas_semana.compras + 1, activas = ventas_semana.activas + 1,
                 monto_total = ventas_semana.monto_total + EXCLUDED.monto_total,
                 monto_neto = ventas_semana.monto_neto + EXCLUDED.monto_neto`,
          [p.semana, e.juegoId, (e.datos as DatosCompraRealizadaDto).precioPagadoClp],
        );
        return 'nuevo';
      }

      if (e.tipo === 'licencia.revocada') {
        // Envuelto en un WITH ... SELECT: con un UPDATE ... RETURNING a secas, TypeORM devuelve [filas, cantidad]
        // en vez de las filas, y [p] seria el arreglo, no la compra.
        const [p] = await tx.query<{ semana: string; juego_id: string; monto: number }[]>(
          `WITH revocada AS (
             UPDATE compras_vistas SET revocada = true WHERE licencia_id = $1 AND NOT revocada
             RETURNING semana::text AS semana, juego_id, monto)
           SELECT semana, juego_id, monto FROM revocada`,
          [licenciaId],
        );
        if (!p) {
          this.log.warn(`revocacion de la licencia ${licenciaId} sin compra pendiente: no resta`);
          return 'nuevo';
        }
        await tx.query(
          `UPDATE ventas_semana SET revocadas = revocadas + 1, activas = activas - 1,
             monto_neto = monto_neto - $3
            WHERE semana = $1 AND juego_id = $2`,
          [p.semana, p.juego_id, p.monto],
        );
        return 'nuevo';
      }
      return 'ignorado';
    });
  }
}
