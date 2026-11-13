import { Type } from 'class-transformer';
import {
  Equals, IsDefined, IsIn, IsInt, IsNotEmpty, IsObject, IsString, IsUUID, Max, Min, ValidateIf, ValidateNested,
} from 'class-validator';
import { EsFechaUtc } from '../contrato.js';

export const TIPOS_CATALOGO = ['juego.publicado', 'precio.cambiado'] as const;

/** El estado completo del juego: cada evento del catalogo lo trae entero (por eso se puede compactar). */
export class DatosJuegoDto {
  @IsString() @IsNotEmpty() titulo!: string;
  @ValidateIf((_dto, valor) => valor !== null) @IsString() @IsNotEmpty() genero!: string | null;
  @ValidateIf((_dto, valor) => valor !== null) @IsInt() @Min(-2_147_483_648) @Max(2_147_483_647) anio!: number | null;
  @ValidateIf((_dto, valor) => valor !== null) @IsInt() @Min(0) @Max(2_147_483_647) precio!: number | null;
  @ValidateIf((_dto, valor) => valor !== null) @IsString() plataforma!: string | null;
}

/** Sobre de vidalstore.catalogo (key juegoId). Sin usuarioSub: lo publica el servicio, no un jugador. */
export class EventoCatalogoDto {
  @IsUUID() eventoId!: string;
  @IsIn(TIPOS_CATALOGO) tipo!: (typeof TIPOS_CATALOGO)[number];
  @Equals(1, { message: 'version tiene que ser 1' }) version!: number;
  @EsFechaUtc() emitidoEn!: string;
  @IsString() @IsNotEmpty() juegoId!: string;
  @IsDefined() @IsObject() @ValidateNested() @Type(() => DatosJuegoDto) datos!: DatosJuegoDto;
}
