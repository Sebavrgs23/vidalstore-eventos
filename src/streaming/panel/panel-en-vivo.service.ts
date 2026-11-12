import { Injectable } from '@nestjs/common';
import { DataSource } from 'typeorm';
import { MINIMO_TERMINADAS, UMBRAL_ERROR } from '../alertas/alertas.negocio.js';

const ZONA = 'America/Santiago';

type FilaMinuto = { minuto: Date; hora: string; iniciadas: number; terminadas: number; en_curso: number; compras: number; ventas_clp: number | string };
type FilaJuego = { juego_id: string; titulo: string | null; en_curso: number; compras: number; ventas_clp: number | string };
type FilaAlerta = { juego_id: string; titulo: string | null; desde: string; hasta: string; terminadas: number; en_error: number };

/**
 * Lo que lee el panel. vidalstore-eventos es el dueno de kpi_minuto y alertas: el BFF pide esto por HTTP y
 * nunca se conecta a Postgres. Las horas salen en hora de Chile; lo guardado esta en UTC.
 */
@Injectable()
export class PanelEnVivoService {
  constructor(private readonly base: DataSource) {}

  async enVivo() {
    const [minutos, juegos, alertas] = await Promise.all([
      this.base.query<FilaMinuto[]>(
        `SELECT minuto, to_char(minuto AT TIME ZONE '${ZONA}', 'HH24:MI') AS hora,
                iniciadas, terminadas, compras, ventas_clp,
                sum(iniciadas - terminadas) OVER (ORDER BY minuto)::int AS en_curso
           FROM (SELECT minuto,
                        count(*) FILTER (WHERE tipo = 'partida.iniciada')::int  AS iniciadas,
                        count(*) FILTER (WHERE tipo = 'partida.terminada')::int AS terminadas,
                        count(*) FILTER (WHERE tipo = 'compra.realizada')::int   AS compras,
                        coalesce(sum(monto_clp) FILTER (WHERE tipo = 'compra.realizada'), 0) AS ventas_clp
                   FROM kpi_minuto GROUP BY minuto) AS m
          ORDER BY minuto`,
      ),
      this.base.query<FilaJuego[]>(
        `SELECT k.juego_id, c.titulo,
                (count(*) FILTER (WHERE k.tipo = 'partida.iniciada') - count(*) FILTER (WHERE k.tipo = 'partida.terminada'))::int AS en_curso,
                count(*) FILTER (WHERE k.tipo = 'compra.realizada')::int AS compras,
                coalesce(sum(k.monto_clp) FILTER (WHERE k.tipo = 'compra.realizada'), 0) AS ventas_clp
           FROM kpi_minuto k LEFT JOIN catalogo_juegos c ON c.juego_id = k.juego_id
          GROUP BY k.juego_id, c.titulo
          ORDER BY en_curso DESC, compras DESC, k.juego_id`,
      ),
      this.base.query<FilaAlerta[]>(
        `SELECT a.juego_id, c.titulo, a.terminadas, a.en_error,
                to_char(a.ventana AT TIME ZONE '${ZONA}', 'HH24:MI') AS desde,
                to_char((a.ventana + interval '5 minutes') AT TIME ZONE '${ZONA}', 'HH24:MI') AS hasta
           FROM alertas a LEFT JOIN catalogo_juegos c ON c.juego_id = a.juego_id
          ORDER BY a.ventana, a.juego_id`,
      ),
    ]);

    const ultimo = minutos.at(-1);
    const maximo = minutos.reduce<FilaMinuto | undefined>((max, m) => (!max || m.en_curso > max.en_curso ? m : max), undefined);
    return {
      zonaHoraria: ZONA,
      generadoEn: new Date().toISOString(),
      hasta: ultimo ? ultimo.hora : null,
      totales: {
        partidasIniciadas: minutos.reduce((s, m) => s + m.iniciadas, 0),
        partidasTerminadas: minutos.reduce((s, m) => s + m.terminadas, 0),
        partidasEnCurso: ultimo?.en_curso ?? 0,
        compras: minutos.reduce((s, m) => s + m.compras, 0),
        ventasClp: minutos.reduce((s, m) => s + Number(m.ventas_clp), 0),
      },
      maximoEnCurso: maximo ? { hora: maximo.hora, partidas: maximo.en_curso } : null,
      porMinuto: minutos.map((m) => ({
        minuto: m.minuto, hora: m.hora, iniciadas: m.iniciadas, terminadas: m.terminadas,
        enCurso: m.en_curso, compras: m.compras, ventasClp: Number(m.ventas_clp),
      })),
      porJuego: juegos.map((l) => ({
        juegoId: l.juego_id, titulo: l.titulo, enCurso: l.en_curso, compras: l.compras, ventasClp: Number(l.ventas_clp),
      })),
      regla: { umbralError: UMBRAL_ERROR, minimoTerminadas: MINIMO_TERMINADAS, ventanaMinutos: 5 },
      alertas: alertas.map((a) => ({
        juegoId: a.juego_id, titulo: a.titulo, desde: a.desde, hasta: a.hasta,
        terminadas: a.terminadas, enError: a.en_error,
        porcentaje: Math.round((1000 * a.en_error) / a.terminadas) / 10,
      })),
    };
  }
}
