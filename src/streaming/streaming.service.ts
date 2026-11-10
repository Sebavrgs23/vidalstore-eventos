import { Injectable, OnModuleInit, OnModuleDestroy, Logger } from '@nestjs/common';
import { MessagesStreamModes, MessagesStreamFallbackModes, Producer } from '@platformatic/kafka';
import { plainToInstance } from 'class-transformer';
import { validate, ValidationError } from 'class-validator';
import { consumidor } from './consumidor.js';
import { TOPICS, getDLTTopic } from './topics.js';
import { EventoKafkaDto } from './evento.dto.js';

interface KafkaMessage {
  topic: string;
  partition: number;
  offset: number;
  key: string | null;
  value: Buffer;
  timestamp: string;
}

interface ErrorMetadata {
  errorType: 'json_parse_error' | 'schema_validation_error' | 'processing_error';
  errorMessage: string;
  originalTopic: string;
  originalPartition: number;
  originalOffset: number;
  timestamp: string;
  validationErrors?: string[];
}

@Injectable()
export class StreamingService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(StreamingService.name);
  private flujo: any;
  private producer: Producer;

  constructor() {
    // Inicializar producer para enviar mensajes a DLT
    this.producer = new Producer({
      clientId: 'vidalstore-eventos-dlt',
      bootstrapBrokers: process.env.KAFKA_BROKERS!.split(','),
    });
  }

  async onModuleInit() {
    this.logger.log('Iniciando producer DLT...');
    await this.producer.connect();
    this.logger.log('✓ Producer DLT conectado');

    this.logger.log('Iniciando consumidor Kafka grupo vidalstore-panel...');
    this.flujo = await consumidor.consume({
      topics: [TOPICS.COMPRAS, TOPICS.ACTIVIDAD],
      autocommit: false,
      mode: MessagesStreamModes.COMMITTED,
      fallbackMode: MessagesStreamFallbackModes.EARLIEST,
    });
    void this.leer(); // sin await: si no, Nest no termina de arrancar
    this.logger.log('✓ Consumidor Kafka iniciado');
  }

  private async leer() {
    this.logger.log('Leyendo mensajes de Kafka...');
    for await (const mensaje of this.flujo) {
      await this.processMessage(mensaje);
      await this.flujo.commit();
    }
  }

  private async processMessage(mensaje: KafkaMessage): Promise<void> {
    let parsedPayload: any;

    // Nivel 1: Parsing de JSON
    try {
      parsedPayload = JSON.parse(mensaje.value.toString());
    } catch (error) {
      await this.handleInvalidMessage(
        mensaje,
        'json_parse_error',
        error instanceof Error ? error.message : 'Unknown parsing error',
      );
      return;
    }

    // Nivel 2: Validación de schema con class-validator
    try {
      const dto = plainToInstance(EventoKafkaDto, parsedPayload);
      const errors = await validate(dto);

      if (errors.length > 0) {
        const validationMessages = this.formatValidationErrors(errors);
        await this.handleInvalidMessage(
          mensaje,
          'schema_validation_error',
          'Schema validation failed',
          validationMessages,
        );
        return;
      }

      // Nivel 3: Procesamiento del mensaje válido
      await this.processValidMessage(dto, mensaje);
    } catch (error) {
      await this.handleInvalidMessage(
        mensaje,
        'processing_error',
        error instanceof Error ? error.message : 'Unknown processing error',
      );
    }
  }

  private async processValidMessage(dto: EventoKafkaDto, mensaje: KafkaMessage): Promise<void> {
    this.logger.log(
      `[${mensaje.topic}] P${mensaje.partition} offset ${mensaje.offset}: ` +
        `${dto.tipo} (${dto.eventoId}) usuario=${dto.usuarioSub} juego=${dto.juegoId}`,
    );
  }

  private async handleInvalidMessage(
    mensaje: KafkaMessage,
    errorType: ErrorMetadata['errorType'],
    errorMessage: string,
    validationErrors?: string[],
  ): Promise<void> {
    const metadata: ErrorMetadata = {
      errorType,
      errorMessage,
      originalTopic: mensaje.topic,
      originalPartition: mensaje.partition,
      originalOffset: Number(mensaje.offset),
      timestamp: new Date().toISOString(),
      ...(validationErrors && { validationErrors }),
    };

    this.logger.warn(
      `[${mensaje.topic}] P${mensaje.partition} offset ${mensaje.offset}: ${errorType} - ${errorMessage}`,
      validationErrors ? { validationErrors } : undefined,
    );

    // Enviar a DLT
    await this.sendToDLT(mensaje, metadata);
  }

  private async sendToDLT(mensaje: KafkaMessage, metadata: ErrorMetadata): Promise<void> {
    const dltTopic = getDLTTopic(mensaje.topic);

    try {
      await this.producer.send({
        topic: dltTopic,
        messages: [
          {
            key: mensaje.key,
            value: JSON.stringify({
              originalMessage: mensaje.value.toString(),
              error: metadata,
            }),
            headers: {
              'original-topic': mensaje.topic,
              'original-partition': String(mensaje.partition),
              'original-offset': String(mensaje.offset),
              'error-type': metadata.errorType,
            },
          },
        ],
      });

      this.logger.log(
        `[DLT] Mensaje enviado a ${dltTopic}: ${metadata.errorType} from ${mensaje.topic} P${mensaje.partition} offset ${mensaje.offset}`,
      );
    } catch (error) {
      // NO reintentar envío a DLT para evitar loops infinitos
      this.logger.error(
        `[DLT] CRITICAL: Failed to send message to ${dltTopic}`,
        error instanceof Error ? error.stack : String(error),
      );
    }
  }

  private formatValidationErrors(errors: ValidationError[]): string[] {
    const messages: string[] = [];
    
    for (const error of errors) {
      if (error.constraints) {
        for (const constraint of Object.values(error.constraints)) {
          messages.push(`${error.property}: ${constraint}`);
        }
      }
      
      // Recursivo para nested errors
      if (error.children && error.children.length > 0) {
        messages.push(...this.formatValidationErrors(error.children));
      }
    }
    
    return messages;
  }

  async onModuleDestroy() {
    this.logger.log('Cerrando consumidor Kafka...');
    if (this.flujo) {
      await this.flujo.close();
    }
    await consumidor.close();
    this.logger.log('✓ Consumidor Kafka cerrado');

    this.logger.log('Cerrando producer DLT...');
    await this.producer.disconnect();
    this.logger.log('✓ Producer DLT cerrado');
  }
}
