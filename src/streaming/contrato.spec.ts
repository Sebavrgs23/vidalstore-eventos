import 'reflect-metadata';
import { describe, expect, it } from 'vitest';
import { Contrato, EventoInvalido, type Mensaje } from './contrato.js';
import { cabecerasDlt, topicDlt } from './dlt.js';
import { EventoDto } from './evento.dto.js';

const UUID = '3f2b8c1e-9a4d-4e7f-8b2c-1d5e6f7a8b9c';

const valido = {
  eventoId: UUID,
  tipo: 'partida.iniciada',
  version: 1,
  emitidoEn: '2026-11-06T21:00:00.000Z',
  juegoId: '1328670',
  usuarioSub: 'sub-de-prueba',
  datos: { partidaId: UUID, plataforma: 'windows' },
};

function mensaje(value: string): Mensaje {
  return {
    topic: 'vidalstore.actividad',
    partition: 1,
    offset: 33n,
    key: '1328670',
    value,
    headers: new Map([['content-type', 'application/json'], ['x-evento-id', UUID]]),
  } as unknown as Mensaje;
}

function motivoDe(value: string): string {
  try {
    new Contrato(EventoDto).validar(mensaje(value));
  } catch (error) {
    if (error instanceof EventoInvalido) return error.motivo;
    throw error;
  }
  throw new Error('se esperaba un EventoInvalido');
}

describe('Contrato de vidalstore-eventos', () => {
  it('acepta un evento valido', () => {
    const evento = new Contrato(EventoDto).validar(mensaje(JSON.stringify(valido)));
    expect(evento.eventoId).toBe(UUID);
    expect(evento.tipo).toBe('partida.iniciada');
  });

  it('JSON cortado: json-invalido', () => {
    expect(motivoDe(JSON.stringify(valido).slice(0, 80))).toBe('json-invalido');
  });

  it.each(['eventoId', 'tipo', 'version', 'emitidoEn', 'juegoId', 'usuarioSub', 'datos'])(
    'falta %s: falta-%s',
    (campo) => {
      const crudo = { ...valido } as Record<string, unknown>;
      delete crudo[campo];
      expect(motivoDe(JSON.stringify(crudo))).toBe(`falta-${campo}`);
    },
  );

  it('version 99: version-desconocida', () => {
    expect(motivoDe(JSON.stringify({ ...valido, version: 99 }))).toBe('version-desconocida');
  });

  it('falta un campo y version 99: gana falta-eventoId', () => {
    const crudo = { ...valido, version: 99 } as Record<string, unknown>;
    delete crudo.eventoId;
    expect(motivoDe(JSON.stringify(crudo))).toBe('falta-eventoId');
  });

  it.each([
    ['eventoId que no es uuid', { ...valido, eventoId: 'abc' }],
    ['tipo que no existe', { ...valido, tipo: 'partida.perdida' }],
    ['plataforma fuera del contrato', { ...valido, datos: { partidaId: UUID, plataforma: 'android' } }],
    ['emitidoEn sin UTC', { ...valido, emitidoEn: '2026-11-06T21:00:00' }],
  ])('%s: dto-invalido', (_nombre, crudo) => {
    expect(motivoDe(JSON.stringify(crudo))).toBe('dto-invalido');
  });

  it.each(['[1,2]', 'null'])('value %s: dto-invalido', (value) => {
    expect(motivoDe(value)).toBe('dto-invalido');
  });
});

describe('cabeceras del .dlt', () => {
  it('conserva las del original y agrega de donde salio y por que', () => {
    const m = mensaje(JSON.stringify(valido));
    const cabeceras = cabecerasDlt(m, 'vidalstore-panel', 'json-invalido', 'no es un JSON de evento');
    expect(cabeceras.get('content-type')).toBe('application/json');
    expect(cabeceras.get('x-evento-id')).toBe(UUID);
    expect(cabeceras.get('x-motivo')).toBe('json-invalido');
    expect(cabeceras.get('x-grupo')).toBe('vidalstore-panel');
    expect(cabeceras.get('x-topic-origen')).toBe('vidalstore.actividad');
    expect(cabeceras.get('x-particion')).toBe('1');
    expect(cabeceras.get('x-offset')).toBe('33');
    expect(cabeceras.get('x-detalle')).toBe('no es un JSON de evento');
    expect(m.headers.has('x-motivo')).toBe(false);
  });

  it('corta el detalle a 500 caracteres', () => {
    const cabeceras = cabecerasDlt(mensaje('{}'), 'vidalstore-panel', 'dto-invalido', 'x'.repeat(900));
    expect(cabeceras.get('x-detalle')?.length).toBe(500);
  });

  it('el topic desviado es el original con sufijo .dlt', () => {
    expect(topicDlt('vidalstore.actividad')).toBe('vidalstore.actividad.dlt');
    expect(topicDlt('vidalstore.compras')).toBe('vidalstore.compras.dlt');
    expect(topicDlt('vidalstore.catalogo')).toBe('vidalstore.catalogo.dlt');
  });
});