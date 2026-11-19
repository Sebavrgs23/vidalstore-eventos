// E3, paso 1: publica en vidalstore.actividad (o vidalstore.compras) los seis mensajes de prueba: JSON cortado,
// sin eventoId, version 99, eventoId que no es uuid, uno bueno y el mismo bueno otra vez. Los cuatro malos van al
// .dlt con su x-motivo, una vez por cada grupo que lee el topic (actividad: panel y alertas; compras: panel e
// historial). El bueno no cambia ninguna cifra del panel ni del historial.
//
//   node herramientas/eventos-malos.mjs            -> vidalstore.actividad
//   node herramientas/eventos-malos.mjs compras    -> vidalstore.compras
//   KAFKA_BROKERS=host:9092 node herramientas/eventos-malos.mjs
//
// El bueno de actividad es un logro.desbloqueado: ni panel ni alertas lo cuentan. El de compras es una
// licencia.revocada de una licencia sin compra: el historial avisa "sin compra pendiente: no resta". En compras
// la segunda copia se informa como DUPLICADO; en actividad ambos grupos la ignoran.
import { randomUUID } from 'node:crypto';
import { Producer, ProduceAcks, compatibilityPartitioner, stringSerializers } from '@platformatic/kafka';

const compras = process.argv[2] === 'compras';
const topic = compras ? 'vidalstore.compras' : 'vidalstore.actividad';
const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092,localhost:9094,localhost:9096').split(',');
const juegoId = '1328670';
const usuarioSub = 'sub-prueba-e3';

function bueno() {
  const datos = compras
    ? { licenciaId: randomUUID(), revocadaPor: 'prueba-e3', motivo: 'evento de prueba E3' }
    : { partidaId: randomUUID(), logroId: 'logro-prueba-e3' };
  return {
    eventoId: randomUUID(),
    tipo: compras ? 'licencia.revocada' : 'logro.desbloqueado',
    version: 1,
    emitidoEn: new Date().toISOString(),
    juegoId,
    usuarioSub,
    datos,
  };
}

const productor = new Producer({
  clientId: 'eventos-malos-e3',
  bootstrapBrokers: brokers,
  serializers: stringSerializers,
  idempotent: true,
  acks: ProduceAcks.ALL,
  retries: 10,
  partitioner: compatibilityPartitioner,
  autocreateTopics: false,
});

async function publicar(nombre, value, eventoId) {
  const r = await productor.send({
    messages: [{
      topic,
      key: compras ? usuarioSub : juegoId,
      value,
      headers: new Map([
        ['content-type', 'application/json'],
        ['x-prueba', 'e3-paso-1'],
        ...(eventoId ? [['x-evento-id', eventoId]] : []),
      ]),
    }],
  });
  const d = r.offsets?.[0];
  console.log(`${nombre}: ${topic}[${d?.partition}]@${d?.offset}`);
}

const evento = bueno();
const completo = JSON.stringify(evento);
const sinEventoId = { ...evento };
delete sinEventoId.eventoId;

await publicar('JSON cortado', completo.slice(0, 80));
await publicar('sin eventoId', JSON.stringify(sinEventoId));
await publicar('version 99', JSON.stringify({ ...evento, version: 99 }));
await publicar('eventoId no-uuid', JSON.stringify({ ...evento, eventoId: 'no-es-uuid' }));
await publicar('bueno', completo, evento.eventoId);
await publicar('bueno otra vez', completo, evento.eventoId);
await productor.close();
console.log(compras
  ? 'Los cuatro malos quedan en vidalstore.compras.dlt (panel e historial); el bueno no cambia cifras y su copia es DUPLICADO.'
  : 'Los cuatro malos quedan en vidalstore.actividad.dlt (panel y alertas); el bueno lo ignoran ambos grupos.');