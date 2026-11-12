import type { ConsumeMessage } from 'amqplib';
import { isEmail, isISO8601, isUUID } from 'class-validator';

// Configuración de reintentos y clasificación de errores para mensajería.
export const REINTENTOS = { maximo: 3, esperaMs: 2000 } as const;

// Un error es del MENSAJE cuando reintentar no lo va a arreglar nunca.
export class MensajeInvalido extends Error {}

export function payloadDelMensaje(mensaje: ConsumeMessage): Record<string, unknown> {
  const payload: unknown = JSON.parse(mensaje.content.toString());
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    throw new MensajeInvalido('El payload no es un objeto');
  }
  return payload as Record<string, unknown>;
}

export function uuidValido(value: unknown, campo: string): string {
  if (typeof value !== 'string' || !isUUID(value)) {
    throw new MensajeInvalido(`El mensaje no trae un ${campo} UUID valido`);
  }
  return value;
}

export function timestampValido(value: unknown): string {
  if (typeof value !== 'string' || !/T.*(?:Z|[+-]\d{2}:\d{2})$/.test(value)
    || !isISO8601(value, { strict: true, strictSeparator: true }) || Number.isNaN(Date.parse(value))) {
    throw new MensajeInvalido('El mensaje no trae un x-emitido-en ISO-8601 valido con zona horaria');
  }
  return value;
}

export function correlacionValida(value: unknown): string | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== 'string' || !value.trim() || /[\r\n]/.test(value) || Buffer.byteLength(value) > 255) {
    throw new MensajeInvalido('El mensaje no trae una correlacion valida');
  }
  return value;
}

export function sobreDelMensaje(mensaje: ConsumeMessage) {
  const headers = mensaje.properties.headers;
  const eventoId = uuidValido(headers?.['x-evento-id'], 'x-evento-id');
  const emitidoEn = timestampValido(headers?.['x-emitido-en']);
  const propiedad = correlacionValida(mensaje.properties.correlationId);
  const header = correlacionValida(headers?.['x-correlation-id']);
  if (propiedad !== undefined && header !== undefined && propiedad !== header) {
    throw new MensajeInvalido('La correlacion AMQP no coincide con x-correlation-id');
  }
  return { eventoId, emitidoEn, correlationId: propiedad ?? header };
}

export function esCorreoValido(value: unknown): value is string {
  return typeof value === 'string' && isEmail(value);
}

export function esDelMensaje(error: unknown): boolean {
  if (error instanceof SyntaxError || error instanceof MensajeInvalido) return true;
  const codigo = (error as { code?: unknown } | null)?.code;
  // SQLSTATE 22 = dato mal formado, 23 = restricción violada: está mal el dato, no la base.
  return typeof codigo === 'string' && (codigo.startsWith('22') || codigo.startsWith('23'));
}

const esperar = (ms: number) => new Promise<void>((listo) => setTimeout(listo, ms));

// Un AggregateError (localhost = ::1 y 127.0.0.1) trae message vacío; el code sí viene.
export function detalle(error: unknown): string {
  const e = error as { message?: string; code?: unknown } | null;
  return e?.message || String(e?.code ?? error);
}

// El contador es la variable intento: vive en memoria mientras el mensaje sigue unacked.
export async function conReintentos<T>(
  trabajo: () => Promise<T>,
  alFallar: (intento: number, error: Error) => void,
): Promise<T> {
  for (let intento = 1; ; intento++) {
    try {
      return await trabajo();
    } catch (error) {
      if (esDelMensaje(error) || intento >= REINTENTOS.maximo) throw error;
      alFallar(intento, error as Error);
      await esperar(REINTENTOS.esperaMs * intento);
    }
  }
}
