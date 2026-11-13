import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { MensajeriaModule } from '../mensajeria/mensajeria.module.js';
import { PersistenciaModule } from '../persistencia/persistencia.module.js';
import { CorreosConsumidor } from './correos.consumidor.js';
import { Notificacion } from '../persistencia/notificacion.entity.js';

@Module({
  imports: [PersistenciaModule, MensajeriaModule, TypeOrmModule.forFeature([Notificacion])],
  providers: [CorreosConsumidor],
})
export class CorreosModule {}
