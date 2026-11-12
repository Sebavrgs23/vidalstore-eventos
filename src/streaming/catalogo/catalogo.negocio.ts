import { Injectable } from '@nestjs/common';
import { DataSource } from 'typeorm';
import type { Procesador, Resultado } from '../consumidor-kafka.js';
import type { EventoCatalogoDto } from './evento-catalogo.dto.js';

/**
 * Lo que hace vidalstore-catalogo: deja en catalogo_juegos el ultimo estado de cada juego. Cada evento trae el
 * juego completo, asi que basta con escribirlo encima. El WHERE evita volver atras si se relee un evento mas
 * antiguo que el guardado (por ejemplo, al reconstruir desde el offset 0 antes de que el topic se compacte).
 */
@Injectable()
export class CatalogoNegocio implements Procesador<EventoCatalogoDto> {
  constructor(private readonly base: DataSource) {}

  async procesar(e: EventoCatalogoDto): Promise<Resultado> {
    const d = e.datos;
    const filas: unknown[] = await this.base.query(
      `INSERT INTO catalogo_juegos
         (juego_id, titulo, genero, anio, precio, plataforma, ultimo_tipo, ultimo_evento_id, emitido_en)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
       ON CONFLICT (juego_id) DO UPDATE SET
         titulo = EXCLUDED.titulo, genero = EXCLUDED.genero, anio = EXCLUDED.anio, precio = EXCLUDED.precio,
         plataforma = EXCLUDED.plataforma,
         ultimo_tipo = EXCLUDED.ultimo_tipo, ultimo_evento_id = EXCLUDED.ultimo_evento_id, emitido_en = EXCLUDED.emitido_en
       WHERE catalogo_juegos.emitido_en < EXCLUDED.emitido_en
       RETURNING juego_id`,
      [e.juegoId, d.titulo, d.genero, d.anio ?? null, d.precio ?? null, d.plataforma ?? null,
        e.tipo, e.eventoId, e.emitidoEn],
    );
    return filas.length === 1 ? 'nuevo' : 'duplicado';
  }
}
