import { Column, Entity, PrimaryColumn } from 'typeorm';

/** vidalstore-alertas: cuantas partidas terminaron y cuantas en error, por juego y ventana fija de 5 minutos. */
@Entity({ name: 'ventanas_partida' })
export class VentanaPartidas {
  @PrimaryColumn({ name: 'juego_id', type: 'text' })
  juegoId!: string;

  // Inicio de la ventana: emitidoEn truncado a 5 minutos (UTC).
  @PrimaryColumn({ type: 'timestamptz' })
  ventana!: Date;

  @Column({ type: 'int' })
  terminadas!: number;

  @Column({ name: 'en_error', type: 'int' })
  enError!: number;
}
