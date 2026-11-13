import { Column, Entity, PrimaryColumn, UpdateDateColumn } from 'typeorm';

/** vidalstore-alertas: las ventanas que cumplen la regla (mas de 20% en error, con el minimo de terminadas). */
@Entity({ name: 'alertas' })
export class Alerta {
  @PrimaryColumn({ name: 'juego_id', type: 'text' })
  juegoId!: string;

  @PrimaryColumn({ type: 'timestamptz' })
  ventana!: Date;

  @Column({ type: 'int' })
  terminadas!: number;

  @Column({ name: 'en_error', type: 'int' })
  enError!: number;

  @UpdateDateColumn({ name: 'actualizada_en', type: 'timestamptz' })
  actualizadaEn!: Date;
}
