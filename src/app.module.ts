import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { AuditoriaModule } from './consumidores/auditoria.module.js';
import { AvisosModule } from './consumidores/avisos.module.js';
import { CartasMuertasModule } from './consumidores/cartas-muertas.module.js';
import { CorreosModule } from './consumidores/correos.module.js';
import { MensajeriaModule } from './mensajeria/mensajeria.module.js';
import { StreamingModule } from './streaming/streaming.module.js';
import { PersistenciaModule } from './persistencia/persistencia.module.js';
import { SaludController } from './salud.controller.js';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    PersistenciaModule,
    MensajeriaModule,
    AuditoriaModule,
    AvisosModule,
    CorreosModule,
    CartasMuertasModule,
    StreamingModule,
  ],
  controllers: [SaludController],
})
export class AppModule {}
