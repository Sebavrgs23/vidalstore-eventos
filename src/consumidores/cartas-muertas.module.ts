import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { MensajeriaModule } from '../mensajeria/mensajeria.module.js';
import { PersistenciaModule } from '../persistencia/persistencia.module.js';
import { CartasMuertasConsumidor } from './cartas-muertas.consumidor.js';
import { MensajeMuerto } from '../persistencia/mensaje-muerto.entity.js';

@Module({
  imports: [PersistenciaModule, MensajeriaModule, TypeOrmModule.forFeature([MensajeMuerto])],
  providers: [CartasMuertasConsumidor],
})
export class CartasMuertasModule {}
