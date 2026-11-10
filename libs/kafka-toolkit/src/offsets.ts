import { Logger } from '@nestjs/common';
import { AssignerProtocol, Consumer, Kafka } from 'kafkajs';
import { errorMessage } from './retry';

type Assignment = Record<string, number[]>;

/** Lo que usamos de INestMicroservice (estructural, por las copias de @nestjs/microservices; ver KafkaCtx). */
interface Unwrappable {
  unwrap<T>(): T;
}

/**
 * Confirma un offset inicial en cada partición asignada donde el group todavía no tiene ninguno.
 *
 * Con commit manual, una partición a la que nunca le llegó un mensaje queda sin offset confirmado. Eso tiene
 * dos efectos: Kafka (y Kafka UI) no calcula lag para esa partición (escenario 3: con 3 activaciones caídas en
 * particiones distintas se veía lag 2 en vez de 3) y, si el group usara "latest", al volver perdería lo publicado
 * mientras estaba caído (RNF-05). Se confirma el inicio del log (o el final si fromBeginning=false), que es
 * exactamente donde el consumidor iba a empezar: no se saltea nada.
 *
 * Se llama después de listen() (el consumer ya se unió al group) y además en cada GROUP_JOIN (rebalanceos).
 * Si justo se procesa un mensaje de esa partición mientras tanto, a lo sumo se reentrega y la idempotencia lo descarta.
 */
export async function initGroupOffsets(microservice: Unwrappable, groupId: string, fromBeginning = true): Promise<void> {
  const logger = new Logger('Offsets');
  const [kafka, consumer] = microservice.unwrap<[Kafka, Consumer, unknown]>();

  const run = async (assignment: Assignment) => {
    const topics = Object.keys(assignment).filter((t) => assignment[t].length > 0);
    if (topics.length === 0) return;
    const admin = kafka.admin();
    await admin.connect();
    try {
      const committed = await admin.fetchOffsets({ groupId, topics });
      for (const { topic, partitions } of committed) {
        const missing = partitions
          .filter((p) => p.offset === '-1' && assignment[topic].includes(p.partition))
          .map((p) => p.partition);
        if (missing.length === 0) continue;
        const bounds = await admin.fetchTopicOffsets(topic);
        const offsets = missing.map((partition) => {
          const b = bounds.find((x) => x.partition === partition)!;
          return { topic, partition, offset: fromBeginning ? b.low : b.high };
        });
        await consumer.commitOffsets(offsets);
        logger.log(`${groupId}: offset inicial confirmado en ${offsets.map((o) => `${topic}/${o.partition}@${o.offset}`).join(', ')}`);
      }
    } finally {
      await admin.disconnect();
    }
  };
  const safeRun = (assignment: Assignment) =>
    run(assignment).catch((err) => logger.warn(`${groupId}: no se pudieron inicializar offsets (${errorMessage(err)})`));

  consumer.on(consumer.events.GROUP_JOIN, (e) => void safeRun(e.payload.memberAssignment as Assignment));

  // El primer GROUP_JOIN ya ocurrió dentro de listen(): se reconstruye la asignación de todos los miembros.
  const group = await consumer.describeGroup();
  const assignment: Assignment = {};
  for (const m of group.members) {
    const decoded = AssignerProtocol.MemberAssignment.decode(m.memberAssignment);
    for (const [topic, partitions] of Object.entries(decoded?.assignment ?? {})) {
      assignment[topic] = [...(assignment[topic] ?? []), ...partitions];
    }
  }
  await safeRun(assignment);
}
