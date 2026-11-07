import { IsString, IsNotEmpty, IsISO8601, IsNumber } from 'class-validator';

/**
 * DTO del sobre de eventos de Kafka según punto 7 de la Guía EP3.
 * Usado por el consumidor para validar mensajes entrantes.
 */
export class EventoKafkaDto {
  @IsString()
  @IsNotEmpty()
  eventoId!: string;

  @IsString()
  @IsNotEmpty()
  tipo!: string;

  @IsNumber()
  version!: number;

  @IsISO8601()
  emitidoEn!: string;

  @IsString()
  @IsNotEmpty()
  juegoId!: string;

  @IsString()
  @IsNotEmpty()
  usuarioSub!: string;

  payload!: Record<string, any>;
}
