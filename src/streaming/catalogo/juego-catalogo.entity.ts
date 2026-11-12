import { Column, Entity, PrimaryColumn } from 'typeorm';

/** vidalstore-catalogo: el ultimo estado de cada juego, reconstruido desde el offset 0 de vidalstore.catalogo. */
@Entity({ name: 'catalogo_juegos' })
export class JuegoCatalogo {
  @PrimaryColumn({ name: 'juego_id', type: 'text' })
  juegoId!: string;

  @Column({ type: 'text' })
  titulo!: string;

  @Column({ type: 'text', nullable: true })
  genero!: string | null;

  @Column({ type: 'int', nullable: true })
  anio!: number | null;

  @Column({ type: 'int', nullable: true })
  precio!: number | null;

  @Column({ type: 'text', nullable: true })
  plataforma!: string | null;

  @Column({ name: 'ultimo_tipo', type: 'text' })
  ultimoTipo!: string;

  @Column({ name: 'ultimo_evento_id', type: 'uuid' })
  ultimoEventoId!: string;

  @Column({ name: 'emitido_en', type: 'timestamptz' })
  emitidoEn!: Date;
}
