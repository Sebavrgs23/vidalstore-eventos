import { Column, Entity, Index, PrimaryColumn } from 'typeorm';

/**
 * vidalstore-panel: UNA FILA POR EVENTO. La clave primaria es evento_id, asi que un duplicado no entra
 * (ON CONFLICT DO NOTHING) y el panel suma con GROUP BY al leer. Solo se guardan los tipos que el
 * panel cuenta: partida.iniciada, partida.terminada y compra.realizada.
 */
@Entity({ name: 'kpi_minuto' })
@Index('ix_kpi_minuto_minuto', ['minuto'])
export class KpiMinuto {
  @PrimaryColumn({ name: 'evento_id', type: 'uuid' })
  eventoId!: string;

  // El minuto de emitidoEn (la hora del evento, en UTC), no la hora en que llego a Kafka.
  @Column({ type: 'timestamptz' })
  minuto!: Date;

  @Column({ name: 'juego_id', type: 'text' })
  juegoId!: string;

  @Column({ type: 'text' })
  tipo!: string;

  // Las filas historicas sin snapshot de precio conservan NULL.
  @Column({ name: 'monto_clp', type: 'int', nullable: true })
  montoClp!: number | null;
}
