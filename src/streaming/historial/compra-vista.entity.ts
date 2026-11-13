import { Column, Entity, PrimaryColumn } from 'typeorm';

/**
 * vidalstore-historial: cada compra realizada, para asociarle su revocacion. licencia.revocada no dice en que
 * semana se compro: se busca aqui por licenciaId. Llega despues que la compra y a la misma particion, porque
 * los dos llevan la key usuarioSub.
 */
@Entity({ name: 'compras_vistas' })
export class CompraVista {
  @PrimaryColumn({ name: 'licencia_id', type: 'text' })
  licenciaId!: string;

  @Column({ type: 'date' })
  semana!: string;

  @Column({ name: 'juego_id', type: 'text' })
  juegoId!: string;

  @Column({ type: 'int' })
  monto!: number;

  @Column({ type: 'boolean' })
  revocada!: boolean;
}
