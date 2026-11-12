import { Module } from '@nestjs/common';
import { PersistenciaModule } from '../../persistencia/persistencia.module.js';
import { PanelConsumidor } from './panel.consumidor.js';
import { PanelNegocio } from './panel.negocio.js';
import { PanelEnVivoService } from './panel-en-vivo.service.js';
import { PanelEnVivoController } from './panel-en-vivo.controller.js';

@Module({
  imports: [PersistenciaModule],
  controllers: [PanelEnVivoController],
  providers: [PanelNegocio, PanelConsumidor, PanelEnVivoService],
})
export class PanelModule {}
