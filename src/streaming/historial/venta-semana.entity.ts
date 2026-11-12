import { Column, Entity, PrimaryColumn } from 'typeorm';

/** vidalstore-historial: por semana (lunes, hora de Chile) y juego: compras, revocadas y activas. */
@Entity({ name: 'ventas_semana' })
export class VentaSemana {
  @PrimaryColumn({ type: 'date' })
  semana!: string;

  @PrimaryColumn({ name: 'juego_id', type: 'text' })
  juegoId!: string;

  @Column({ type: 'int' })
  compras!: number;

  @Column({ type: 'int' })
  revocadas!: number;

  @Column({ type: 'int' })
  activas!: number;

  @Column({ name: 'monto_total', type: 'int' })
  montoTotal!: number;

  @Column({ name: 'monto_neto', type: 'int' })
  montoNeto!: number;
}
