import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { MensajeriaModule } from '../mensajeria/mensajeria.module.js';
import { AvisosConsumidor } from './avisos.consumidor.js';

@Module({
  imports: [ConfigModule, MensajeriaModule],
  providers: [AvisosConsumidor],
})
export class AvisosModule {}
