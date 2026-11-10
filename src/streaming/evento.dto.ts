import { IsString, IsNotEmpty, IsISO8601, IsNumber, IsObject } from 'class-validator';

/**
 * DTO del sobre de eventos de Kafka según punto 7 de la Guía EP3.
 * Usado por el consumidor para validar mensajes entrantes.
 */
export class EventoKafkaDto {
  @IsString({ message: 'eventoId debe ser un string' })
  @IsNotEmpty({ message: 'eventoId no puede estar vacío' })
  eventoId!: string;

  @IsString({ message: 'tipo debe ser un string' })
  @IsNotEmpty({ message: 'tipo no puede estar vacío' })
  tipo!: string;

  @IsNumber({}, { message: 'version debe ser un número' })
  version!: number;

  @IsISO8601({}, { message: 'emitidoEn debe ser una fecha ISO8601 válida' })
  emitidoEn!: string;

  @IsString({ message: 'juegoId debe ser un string' })
  @IsNotEmpty({ message: 'juegoId no puede estar vacío' })
  juegoId!: string;

  @IsString({ message: 'usuarioSub debe ser un string' })
  @IsNotEmpty({ message: 'usuarioSub no puede estar vacío' })
  usuarioSub!: string;

  @IsObject({ message: 'payload debe ser un objeto' })
  payload!: Record<string, any>;
}
