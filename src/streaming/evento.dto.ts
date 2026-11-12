import { Type } from 'class-transformer';
import {
  Equals, IsDefined, IsIn, IsInt, IsNotEmpty, IsNumber, IsObject, IsString, IsUUID, Max, Min, ValidateNested,
} from 'class-validator';
import { EsFechaUtc } from './contrato.js';

export const TIPOS_EVENTO = [
  'partida.iniciada', 'partida.terminada', 'logro.desbloqueado',
  'compra.realizada', 'licencia.revocada',
] as const;

export class DatosPartidaIniciadaDto {
  @IsUUID() partidaId!: string;
  @IsIn(['windows', 'macos', 'linux']) plataforma!: string;
}

export class DatosPartidaTerminadaDto {
  @IsUUID() partidaId!: string;
  @IsInt() @Min(0) duracionSeg!: number;
  @IsIn(['completada', 'abandonada', 'error']) resultado!: string;
}

export class DatosLogroDesbloqueadoDto {
  @IsUUID() partidaId!: string;
  @IsString() @IsNotEmpty() logroId!: string;
}

export class DatosCompraRealizadaDto {
  @IsUUID() licenciaId!: string;
  @EsFechaUtc() adquiridaEn!: string;
  @IsInt() @Min(0) @Max(2_147_483_647) precioBaseClp!: number;
  @IsNumber() @Min(0) @Max(100) descuentoPct!: number;
  @IsInt() @Min(0) @Max(2_147_483_647) precioPagadoClp!: number;
}

export class DatosLicenciaRevocadaDto {
  @IsUUID() licenciaId!: string;
  @IsString() @IsNotEmpty() revocadaPor!: string;
  @IsString() @IsNotEmpty() motivo!: string;
}

export type DatosEvento = DatosPartidaIniciadaDto | DatosPartidaTerminadaDto | DatosLogroDesbloqueadoDto
  | DatosCompraRealizadaDto | DatosLicenciaRevocadaDto;

/** Sobre de vidalstore.actividad y vidalstore.compras. Los datos van dentro segun el tipo. */
export class EventoDto {
  @IsUUID() eventoId!: string;
  @IsIn(TIPOS_EVENTO) tipo!: (typeof TIPOS_EVENTO)[number];
  @Equals(1, { message: 'version tiene que ser 1' }) version!: number;
  @EsFechaUtc() emitidoEn!: string;
  @IsString() @IsNotEmpty() juegoId!: string;
  @IsString() @IsNotEmpty() usuarioSub!: string;
  @IsDefined() @IsObject() @ValidateNested()
  @Type((opciones) => {
    switch (opciones?.object?.tipo) {
      case 'partida.terminada': return DatosPartidaTerminadaDto;
      case 'logro.desbloqueado': return DatosLogroDesbloqueadoDto;
      case 'compra.realizada': return DatosCompraRealizadaDto;
      case 'licencia.revocada': return DatosLicenciaRevocadaDto;
      default: return DatosPartidaIniciadaDto;
    }
  })
  datos!: DatosEvento;
}
