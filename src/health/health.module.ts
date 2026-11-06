import { Module } from '@nestjs/common';
import { DatabaseModule } from '../database/database.module.js';
import { HealthController } from './health.controller.js';
import { HealthStatusService } from './health-status.service.js';

@Module({
  imports: [DatabaseModule],
  controllers: [HealthController],
  providers: [HealthStatusService],
  exports: [HealthStatusService],
})
export class HealthModule {}
