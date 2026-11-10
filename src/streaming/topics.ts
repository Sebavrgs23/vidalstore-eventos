export const TOPICS = {
  COMPRAS: 'vidalstore.compras',
  ACTIVIDAD: 'vidalstore.actividad',
  COMPRAS_DLT: 'vidalstore.compras.dlt',
  ACTIVIDAD_DLT: 'vidalstore.actividad.dlt',
} as const;

/**
 * Mapea un topic a su correspondiente Dead Letter Topic.
 */
export function getDLTTopic(topic: string): string {
  switch (topic) {
    case TOPICS.COMPRAS:
      return TOPICS.COMPRAS_DLT;
    case TOPICS.ACTIVIDAD:
      return TOPICS.ACTIVIDAD_DLT;
    default:
      return `${topic}.dlt`;
  }
}
