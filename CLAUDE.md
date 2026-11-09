# CLAUDE.md — POC Activación de Servicios con Kafka

> **Fuente de verdad:** `docs/consigna.pdf` (TP6, Sistemas Distribuidos 2026). Este archivo lo resume.
> Ante cualquier duda o contradicción **manda el PDF**: seguirlo y avisar de la diferencia.

## 1. Resumen

La POC demuestra que la activación de un plan se resuelve publicando **un único evento** en Kafka,
**sin que ningún servicio llame directamente a otro**. Un cliente contrata un plan desde una UI React;
eso dispara facturación, aprovisionamiento, notificación y registro en CRM, coordinados por eventos.

- **Coreografía con agregador**: billing y provisioning reaccionan solos a `ActivationRequested` (en paralelo);
  `activation-api` solo junta resultados y decide `ActivationCompleted` / `ActivationFailed`. No hay orquestador.
- `activation-api` es el **único punto de entrada** (REST + WebSocket); todo lo demás reacciona a eventos.
- Todo corre en local con Docker Compose; los sistemas externos se simulan.

**Fuera de alcance:** BSS/checkout/proveedores reales, autenticación/autorización, alta disponibilidad
(multi-broker, réplicas, nube), pruebas de carga/métricas, Schema Registry y trazas distribuidas (fase 4 opcional).

### Componentes
| Componente | Rol |
|---|---|
| **demo-ui** (React + Vite) | Formulario "Contratar plan" (cliente, plan, selector de fallo forzado en billing/provisioning). Timeline de eventos por WebSocket. |
| **activation-api** (NestJS) | `POST /activations`, `GET /activations/:id`. Guarda la activación y publica `ActivationRequested`. Consume resultados de billing y provisioning, mantiene el estado de la saga, publica `ActivationCompleted`/`ActivationFailed`. `@WebSocketGateway` empuja cada cambio a la UI. |
| **billing-service** | Consume `ActivationRequested` → crea cuenta de facturación simulada. Consume `ActivationFailed` → anula la cuenta (compensación). |
| **provisioning-service** | Consume `ActivationRequested` → simula aprovisionamiento con retardo de 1 a 3 s. Puede fallar a pedido. |
| **notification-service** | Consume `activation.events` → email de bienvenida o de error a Mailhog. |
| **crm-analytics-service** | Consume **todos** los topics, no publica nada. Demuestra sumar un consumidor sin tocar a los demás. |
| **MongoDB** | Una base por servicio: estado, outbox, eventos procesados. |
| **Kafka UI** (kafbat) | Topics, mensajes, particiones, consumer groups y lag en vivo durante la demo. |
| **Mailhog** | Bandeja de entrada visible para los emails. |

## 2. Stack

| Componente | Tecnología |
|---|---|
| Mensajería | Apache Kafka, modo **KRaft**, **1 broker** (sin ZooKeeper) |
| Servicios | NestJS + `@nestjs/microservices`, transporte Kafka (`ClientKafka` para publicar, `@EventPattern` para consumir; sobre kafkajs) |
| Frontend | React + Vite + socket.io-client |
| Persistencia | MongoDB en **replica set de 1 nodo** (necesario para transacciones; desde la fase 1) |
| Observabilidad | Kafka UI (kafbat/kafka-ui) |
| Email simulado | Mailhog |
| Entorno | Docker Compose (un solo comando) |
| Monorepo | **pnpm workspaces**; contratos compartidos en una librería |

## 3. Topics y contratos de eventos

Cuatro topics de negocio + sus DLQ. **Todos: 3 particiones, retención 7 días (`retention.ms=604800000`), clave del mensaje = `customerId`.**

| Topic | Eventos | Productor | Consumidores (consumer group) |
|---|---|---|---|
| `activation.requested` | `ActivationRequested` | activation-api | billing-svc, provisioning-svc, crm-analytics |
| `billing.events` | `BillingAccountCreated`, `BillingFailed`, `BillingAccountCancelled` | billing | activation-api, crm-analytics |
| `provisioning.events` | `ProvisioningCompleted`, `ProvisioningFailed` | provisioning | activation-api, crm-analytics |
| `activation.events` | `ActivationCompleted`, `ActivationFailed` | activation-api | notification-svc, billing-svc, crm-analytics |
| `<topic>.dlq` | mensajes que fallaron 3 veces | el consumidor que falló | inspección manual en Kafka UI |

DLQs: `activation.requested.dlq`, `billing.events.dlq`, `provisioning.events.dlq`, `activation.events.dlq`.

### Sobre común (todos los eventos)

```json
{
  "eventId": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "eventType": "ActivationRequested",
  "version": 1,
  "occurredAt": "2026-09-27T16:40:00Z",
  "correlationId": "act-0001",
  "customerId": "C-1234",
  "source": "activation-api",
  "payload": {
    "planId": "Plan Digital",
    "channel": "web",
    "simulateFailure": "none"
  }
}
```

| Campo | Uso |
|---|---|
| `eventId` | Identificador único; base de la idempotencia. |
| `eventType` + `version` | Evolucionar el contrato sin romper consumidores. |
| `correlationId` | El `activationId`; agrupa todos los eventos de una activación en la timeline. |
| `customerId` | También es la **clave del mensaje** en Kafka; garantiza orden por cliente. |
| `source` | Servicio que publicó el evento. |
| `simulateFailure` | Solo demo (`none` / `billing` / `provisioning`): indica a billing o provisioning que deben fallar. |

**Convenciones:** eventos en **pasado** (describen un hecho); topics `dominio.tipo`; cambios compatibles = sumar
campos opcionales; cambios incompatibles = subir `version`. `libs/contracts` es la **única fuente de verdad** de los eventos.

## 4. Saga de activación

| Estado | Cuándo | Siguiente |
|---|---|---|
| `PENDING` | Al crear la activación | `IN_PROGRESS` |
| `IN_PROGRESS` | Llegó el primer resultado de billing o provisioning | `ACTIVE` o `FAILED` |
| `ACTIVE` | Billing **y** provisioning confirmaron | final |
| `FAILED` | Alguno de los dos falló | final (con compensación) |

**Regla de agregación (activation-api):** guarda el resultado de cada servicio en el documento de la activación (`steps`).
Con los dos OK publica `ActivationCompleted`; con el **primer fallo** publica `ActivationFailed` e **ignora resultados
posteriores** de esa activación, salvo registrarlos en `history`.

**Caso borde:** si provisioning falla antes de que billing cree la cuenta, billing puede recibir `ActivationFailed`
antes de terminar. **Billing debe anular la cuenta también si la crea después de haber visto el fallo.**

Flujo: UI → `POST /activations` → 202 `PENDING` → `ActivationRequested` → (en paralelo) billing → `BillingAccountCreated`/`BillingFailed`
y provisioning → `ProvisioningCompleted`/`ProvisioningFailed` → activation-api agrega →
- ambos OK: `ActivationCompleted` → notification envía email de bienvenida.
- alguno falló: `ActivationFailed` → billing publica `BillingAccountCancelled` (si había cuenta) y notification envía email de error.
- Cada cambio de estado se envía a la UI por WebSocket.

## 5. Modelo de datos (MongoDB)

Cada servicio tiene su propia base; **ninguno lee la base de otro**. `outbox` y `processed_events` son comunes a los servicios que publican/consumen.

| Colección | Servicio | Contenido | Índices |
|---|---|---|---|
| `activations` | activation-api | Estado, resultados parciales e historial de eventos | `_id` = activationId; `customerId` |
| `billing_accounts` | billing | Cuenta simulada por activación, estado `CREATED` o `CANCELLED` | `activationId` único |
| `provisioning_orders` | provisioning | Orden simulada de aprovisionamiento | `activationId` único |
| `notifications` | notification | Emails enviados | `activationId` + `eventType` |
| `event_log` | crm-analytics | Copia de cada evento recibido | `eventId` único; `correlationId` |
| `outbox` | activation-api, billing, provisioning | Eventos pendientes de publicar | `status` + `createdAt` |
| `processed_events` | todos los consumidores | `eventId` ya procesados | `eventId` único; **TTL 7 días** |

El TTL de `processed_events` coincide con la retención de los topics: un evento más viejo ya no puede volver a llegar.

Ejemplo de documento `activations`:

```json
{
  "_id": "act-0001",
  "customerId": "C-1234",
  "planId": "PLAN DIGITAL",
  "status": "ACTIVE",
  "simulateFailure": "none",
  "steps": {
    "billing": { "status": "OK", "at": "2026-09-27T16:40:02Z" },
    "provisioning": { "status": "OK", "at": "2026-09-27T16:40:03Z" }
  },
  "history": [
    { "eventType": "ActivationRequested", "at": "2026-09-27T16:40:00Z" },
    { "eventType": "BillingAccountCreated", "at": "2026-09-27T16:40:02Z" },
    { "eventType": "ProvisioningCompleted", "at": "2026-09-27T16:40:03Z" },
    { "eventType": "ActivationCompleted", "at": "2026-09-27T16:40:03Z" }
  ],
  "createdAt": "2026-09-27T16:40:00Z",
  "updatedAt": "2026-09-27T16:40:03Z"
}
```

## 6. Decisiones técnicas

1. **Idempotencia en cada consumidor.** Kafka entrega *al menos una vez*. Cada consumidor inserta el `eventId` en
   `processed_events` (índice único) **en la misma transacción que su efecto**. Si la inserción falla por duplicado,
   descarta el evento y confirma el offset.
2. **Reintentos y DLQ.** Ante un error técnico: 3 reintentos con espera creciente (**1 s, 2 s, 4 s**). Si sigue fallando,
   publica el mensaje **original** en `<topic>.dlq` con el motivo del error en los **headers** y continúa con el siguiente
   (no bloquea la partición). Los errores de negocio simulados (`simulateFailure`) **no** son errores técnicos: se publican
   como `BillingFailed` / `ProvisioningFailed`, **no van a la DLQ**.
3. **Outbox transaccional (fase 4).** Activación + evento en la misma transacción Mongo; un proceso aparte lee `outbox`
   cada **500 ms**, publica pendientes y los marca enviados. **Fases 1 a 3: se publica directo después de guardar** y se
   documenta el riesgo (si la API cae entre guardar y publicar, el evento se pierde).
4. **Cliente Kafka.** Transporte Kafka de `@nestjs/microservices` (kafkajs). Para un piloto, revisar el mantenimiento de
   kafkajs y evaluar el cliente oficial de Confluent. **Aislar el cliente en `libs/kafka-toolkit`** para poder reemplazarlo.

Otras:
- **Coreografía con agregador** (no orquestador).
- **Commit de offset manual, después de procesar, nunca antes.**
- **Mongo en replica set de un nodo** (transacciones).

## 7. Requerimientos

### Funcionales
- **RF-01** La UI permite crear una activación indicando `customerId`, `planId` y un modo de fallo opcional (ninguno, billing, provisioning).
- **RF-02** `POST /activations` responde **202 Accepted** con el `activationId` y el estado `PENDING`, sin esperar a los demás servicios.
- **RF-03** activation-api publica `ActivationRequested` por cada activación creada.
- **RF-04** billing y provisioning procesan el mismo evento en paralelo y publican su resultado (éxito o fallo).
- **RF-05** Con billing y provisioning OK, la activación pasa a `ACTIVE` y se publica `ActivationCompleted`.
- **RF-06** Si alguno falla, la activación pasa a `FAILED`, se publica `ActivationFailed` y billing anula la cuenta si la había creado.
- **RF-07** notification envía un email por cada `ActivationCompleted` o `ActivationFailed`.
- **RF-08** La UI muestra cada evento de la activación en una timeline, en tiempo real, vía WebSocket.
- **RF-09** `GET /activations/:id` devuelve el estado actual y el historial de eventos.
- **RF-10** crm-analytics registra todos los eventos y puede reprocesar el historial desde el inicio.

### No funcionales
- **RNF-01** Todo el entorno se levanta con `docker compose up`.
- **RNF-02** Los eventos de un mismo cliente se procesan en orden: clave de partición = `customerId`.
- **RNF-03** Cada consumidor es idempotente: un evento repetido no produce efectos duplicados.
- **RNF-04** Un mensaje que falla 3 veces va a la DLQ y no bloquea la partición.
- **RNF-05** Si un servicio se cae y vuelve, procesa los eventos pendientes sin perder ninguno.
- **RNF-06** Todos los eventos llevan `correlationId` para rastrear una activación de punta a punta.
- **RNF-07** Los contratos de eventos viven en una librería compartida y tienen versión.

## 8. Plan de fases

| Fase | Contenido | Criterio de cierre |
|---|---|---|
| **1 · Base** | Infra en Compose, activation-api, billing, provisioning, camino feliz con logs | **RF-01 a RF-05 OK** |
| **2 · Visible** | UI con timeline WS, notification + Mailhog, fallos forzados, compensación | **Escenarios 1 y 2 → demo presentable** (fase mínima para presentar) |
| **3 · Robusta** | Idempotencia, reintentos y DLQ, crm-analytics, loyalty, escenarios 3 a 5 | **Escenarios 1 a 5 sin duplicados** |
| **4 · Opcional** | Outbox transaccional, Schema Registry, OpenTelemetry | **Ningún evento perdido si cae la API** (solo si la POC pasa a piloto) |

Decisiones de implementación acordadas para la fase 1: demo-ui con **formulario mínimo** (POST + consultar estado; sin WS/timeline,
que son fase 2). El agregador ya implementa la rama `FAILED`; fallos forzados y compensación se validan en fase 2.

## 9. Guión de la demo (≈15 min; Demo UI a la izquierda, Kafka UI a la derecha)

| # | Escenario | Qué se hace | Qué se ve | Concepto |
|---|---|---|---|---|
| 1 | Camino feliz | Contratar un plan sin fallo | Timeline con 4 eventos en verde; email en Mailhog | Fan-out |
| 2 | Fallo y compensación | Contratar con fallo en provisioning | `ProvisioningFailed`, `ActivationFailed`, `BillingAccountCancelled`; email de error | Saga sin llamadas directas |
| 3 | Servicio caído | Detener notification, crear 3 activaciones, volver a levantarlo | Kafka UI muestra lag de 3; al volver llegan los 3 emails | Offsets: nadie pierde mensajes |
| 4 | Escalar | Segunda instancia de billing | Kafka UI reparte las 3 particiones entre 2 consumidores | Consumer group y particiones |
| 5 | Nuevo consumidor | Levantar `loyalty-service` con un group nuevo que lee desde el inicio | Procesa todo el historial sin tocar a los demás | Replay del log |

```bash
# Escenario 3
docker compose stop notification-service
docker compose start notification-service
# Escenario 4
docker compose up -d --scale billing-service=2
# Escenario 5 (loyalty con auto.offset.reset=earliest)
docker compose --profile loyalty up -d loyalty-service
```

## 10. Criterios de aceptación de la POC
- [ ] `docker compose up` levanta todo sin pasos manuales.
- [ ] Los 5 escenarios del guion se ejecutan de punta a punta en menos de 15 minutos.
- [ ] Reenviar un evento ya procesado no genera cuentas ni emails duplicados.
- [ ] Un mensaje inválido termina en la DLQ y el resto del flujo sigue.
- [ ] Todos los eventos de una activación se pueden rastrear por `correlationId`.

## 11. Riesgos y mitigaciones
| Riesgo | Impacto | Mitigación |
|---|---|---|
| Kafka tarda en arrancar y los servicios fallan al conectar | La demo no arranca | healthcheck en Compose + `depends_on` con `condition: service_healthy` |
| Rebalanceo lento al escalar billing (esc. 4) | Pausa visible de varios segundos | Explicarlo en la demo; ajustar `session.timeout.ms` |
| Transacciones Mongo requieren replica set | Outbox e idempotencia no funcionan | Replica set de un nodo desde la fase 1 |
| Mantenimiento limitado de kafkajs | Deuda técnica si pasa a piloto | Aislar el cliente en `libs/kafka-toolkit` |
| La demo depende de la red local | Fallo en vivo | Grabar video de respaldo de los 5 escenarios |
| *(agregado)* Kafka 4.x eliminó versiones viejas del protocolo (KIP-896); compatibilidad dudosa con kafkajs | Clientes que no conectan | Fijar `apache/kafka` en la rama **3.9.x** |
| *(agregado)* MongoDB 8.0+ no arranca en kernels Linux ≥ 6.19 (SERVER-121912, tcmalloc/rseq); esta máquina tiene kernel 7.0 | Mongo sale con "cannot start" | Usar **`mongo:7.0.45`** (soporta replica set y transacciones; el documento no fija versión) |

## 12. Estructura del repo y entorno

```
activation-poc/
├── apps/
│   ├── activation-api/          REST + WebSocket + agregador de la saga
│   ├── billing-service/
│   ├── provisioning-service/
│   ├── notification-service/
│   ├── crm-analytics-service/
│   └── demo-ui/                 React + Vite + socket.io-client
├── libs/
│   ├── contracts/               tipos de eventos, sobre común, nombres de topics
│   └── kafka-toolkit/           idempotencia, reintentos, DLQ, outbox
├── docker/
│   └── create-topics.sh         crea los topics con 3 particiones
├── docker-compose.yml
└── docs/
    ├── consigna.pdf             (fuente de verdad)
    ├── architecture.md
    └── demo-script.md
```

### Servicios de Docker Compose
| Servicio | Imagen o build | Puerto local |
|---|---|---|
| kafka | apache/kafka (KRaft, 1 nodo) | 9092 |
| kafka-init | apache/kafka + create-topics.sh | — |
| kafka-ui | kafbat/kafka-ui | 8080 |
| mongo | mongo (replica set de 1 nodo) | 27017 |
| mailhog | mailhog/mailhog | 8025 |
| activation-api | build apps/activation-api | 3000 |
| billing-service | build apps/billing-service | — |
| provisioning-service | build apps/provisioning-service | — |
| notification-service | build apps/notification-service | — |
| crm-analytics-service | build apps/crm-analytics-service | — |
| demo-ui | build apps/demo-ui | 5173 |

Variables de entorno comunes: `KAFKA_BROKERS`, `KAFKA_CLIENT_ID`, `KAFKA_GROUP_ID`, `MONGO_URI`.
**Las versiones exactas de las imágenes se fijan en `docker-compose.yml`** (nunca `latest`).

## 13. Restricciones del entorno y convenciones de trabajo

- **Máquina:** 7,5 GB de RAM, Linux Mint, Docker 29, Compose v5, Node 22, pnpm 12. **El compose debe ser liviano:**
  - `mem_limit` en cada contenedor; JVMs con heap acotado (Kafka `-Xmx512m`, Kafka UI `-Xmx256m`).
  - Mongo con `--wiredTigerCacheSizeGB 0.25`.
  - Apps Nest como imágenes de producción compiladas (`node dist/main.js`, `--max-old-space-size=128`), un único Dockerfile multi-stage parametrizado por app.
  - demo-ui en Docker como build estático (nginx), no dev server.
  - Objetivo: < ~2 GB en fase 1, < ~3 GB con todo.
- **Desarrollo:** infra en Docker (`kafka`, `kafka-init`, `kafka-ui`, `mongo`, `mailhog`) y las apps en el host con `pnpm dev`.
  Kafka expone dos listeners: interno `kafka:19092` (contenedores) y externo `localhost:9092` (host).
  Desde el host, Mongo con `?replicaSet=rs0&directConnection=true`.
- **Kafka:** `auto.create.topics.enable=false`; los topics los crea solo `kafka-init`. Consumer groups con nombres exactos
  (en Nest usar `postfixId: ''`). `autoCommit: false` + commit manual tras procesar. Publicar siempre con `key = customerId`
  y headers `eventType`, `correlationId`.
- **Código:** TypeScript strict. Paquetes `@poc/*`. Ningún servicio importa código de otro servicio; solo `@poc/contracts`
  y `@poc/kafka-toolkit`. Ningún servicio hace llamadas HTTP a otro servicio.
- **Logs:** incluir siempre `correlationId` y `eventType`.

## 14. Decisiones de implementación (registro)

Decisiones tomadas al implementar, eligiendo siempre lo más fiel al PDF. Agregar acá las nuevas.

| # | Fase | Decisión | Motivo |
|---|---|---|---|
| D-01 | 1 | Imágenes fijadas: `apache/kafka:3.9.2`, `kafbat/kafka-ui:v1.5.0`, `mongo:7.0.45`, `mailhog/mailhog:v1.0.1`, `node:22.23.4-alpine`, `nginx:1.30.6-alpine` | El documento pide fijarlas al armar el compose |
| D-02 | 1 | **Mongo 7.0.45** en lugar de 8.x | 8.0.32 y 8.0.34 se niegan a arrancar en el kernel 7.0 de la máquina (SERVER-121912). 7.0 tiene replica set y transacciones, que es lo que pide el documento |
| D-03 | 1 | NestJS **11.2.8**, mongoose 8, TypeScript 5.9, Vite 7, React 19.2 (no Nest 12 ni TS 7) | Versiones con al menos dos semanas y APIs conocidas. Nest 11 tiene `postfixId` y `producerOnlyMode` en el transporte Kafka |
| D-04 | 1 | `postfixId: ''` en servidor y cliente Kafka | Los consumer groups se llaman exactamente `activation-api`, `billing-svc`, `provisioning-svc`, etc. |
| D-05 | 1 | El `ClientKafka` usa `producerOnlyMode: true` y un clientId `<servicio>-producer` | Sin esto, el cliente crea su propio consumer y, con `postfixId: ''`, se uniría al mismo consumer group que el servidor |
| D-06 | 1 | Commit manual con `commitAfter(ctx, fn)` de `libs/kafka-toolkit` (`run.autoCommit: false`) | Documento §08: commit después de procesar, nunca antes. En fase 1, si el handler falla no se commitea, pero un mensaje posterior de la misma partición avanza el offset. Los reintentos y la DLQ llegan en fase 3 |
| D-07 | 1 | Publicación directa después de guardar, con `TODO(fase 4 · outbox)` en cada publish | Documento §08, decisión 3 (riesgo documentado en el código) |
| D-08 | 1 | Agregador con updates condicionales en Mongo: el paso se registra solo si la activación está abierta; la transición `IN_PROGRESS → ACTIVE/FAILED` es un `findOneAndUpdate` condicional y publica solo quien la gana | Regla de agregación sin doble publicación, aunque haya resultados concurrentes o escalado. Los resultados tardíos quedan en `history` con `ignored: true` |
| D-09 | 1→2 | En fase 1 la rama `FAILED` del agregador ya estaba y la compensación no (la cuenta quedaba `CREATED`). **Fase 2: compensación implementada** (ver D-19) | La compensación es de la fase 2 |
| D-10 | 1 | billing y provisioning hacen upsert por `activationId` (índice único) | Una redelivery no duplica la cuenta ni la orden. La idempotencia completa con `processed_events` es de la fase 3 |
| D-11 | 1 | `activationId` = `act-` + 12 caracteres hex | El documento usa ids con forma `act-0001` |
| D-12 | 1 | `GET /health` en activation-api | Para el healthcheck de Compose (demo-ui depende de `service_healthy`) |
| D-13 | 1 | Un único `Dockerfile` (`ARG APP`) para las apps Nest, con `pnpm deploy --prod`, `--max-old-space-size=128` y `mem_limit 200m`. demo-ui se sirve estática con nginx | RAM. Uso medido en fase 1: unos 1 GB en total |
| D-14 | 1 | `allowBuilds: { esbuild: true }` en `pnpm-workspace.yaml` | pnpm 12 corta la instalación si hay build scripts sin aprobar |
| D-15 | 1 | En el primer arranque, los consumidores pueden loguear una vez `The group coordinator is not available` | Kafka todavía está creando `__consumer_offsets`; kafkajs reintenta solo. No es un error |
| D-16 | 2 | WebSocket: socket.io en el mismo puerto que la API (3000). Un solo mensaje, `activation.updated` = `{ correlationId, event, activation }` (entrada de historial que causó el cambio + foto completa con el formato de `GET /activations/:id`), emitido a todos los clientes por cada entrada nueva del historial. La UI agrupa por `correlationId` y, si llegan fotos desordenadas (REST vs. WS), se queda con la de historial más largo | Un mensaje por evento recibido o publicado (RF-08) y la UI no necesita reconstruir estado |
| D-17 | 2 | `GET /activations?limit=20` (más nuevas primero). No está en el documento | La lista de activaciones recientes de la UI tiene que mostrar también las creadas por curl o desde otra pestaña. No cambia RF-02 ni RF-09 |
| D-18 | 2 | Historial (RF-09): cada entrada tiene `eventId`, `eventType`, `source`, `at` (= `occurredAt`), `detail` (cuenta, orden, motivo) e `ignored`. `BillingAccountCancelled` se registra sin `ignored` (es la compensación, no un resultado tardío); un resultado que llega con la activación ya final queda con `ignored: true` | Documento §06: ignora resultados posteriores "salvo registrarlos en el historial" |
| D-19 | 2 | Compensación en billing: consume `activation.events` (group `billing-svc`), `ActivationFailed` → `CREATED → CANCELLED` condicional + `BillingAccountCancelled`; `ActivationCompleted` no hace nada. **Caso borde:** el fallo se recuerda en la colección `activation_failures` (billing, `activationId` único; no figura en la tabla del documento) y, al crear la cuenta, si el fallo ya se vio se publica `BillingAccountCreated` y enseguida se anula. Cada camino escribe primero y lee después, así al menos uno ve al otro aunque corran en paralelo; el update condicional evita doble anulación | Documento §06: "billing debe anular la cuenta también si la crea después de haber visto el fallo". Probado publicando a mano `ActivationFailed` antes de `ActivationRequested` |
| D-20 | 2 | notification-service: `nodemailer 8.0.11` por SMTP a `mailhog:1025`. Como el contrato no trae email, el destinatario es `<customerId>@clientes.example.com`. Colección `notifications` con índice **único** `activationId + eventType`: si ya existe, una redelivery no reenvía el email. Riesgo hasta la fase 3: si cae entre el envío y el insert, se reenvía | Documento §07 (índice `activationId + eventType`). nodemailer 10.x tiene releases de hace días; 8.0.11 tiene meses y tipos estables |
| D-21 | 2 | `libs/kafka-toolkit` tipa el contexto como interfaz estructural (`KafkaCtx`), no como la clase `KafkaContext` | Con `@nestjs/websockets` en activation-api, pnpm resuelve otra copia de `@nestjs/microservices` (peer opcional) y la clase no es asignable entre copias |
| D-22 | 2 | Mailhog publica también SMTP en `127.0.0.1:1025` (solo para `pnpm dev:notification` en el host) | Convención de §13: apps en el host contra la infra de Docker |
| D-23 | 2 | Los consumer groups/suscripciones nuevos (`notification-svc`, `billing-svc` sobre `activation.events`) arrancan en el último offset (`fromBeginning: false`): no procesan eventos anteriores a su primer arranque | Igual que los demás servicios; el replay desde el inicio es el escenario 5 (loyalty) |

## 15. Estado

- **Fase 1: completa.** RF-01 a RF-05 verificados con `docker compose up -d --build` desde cero (volúmenes borrados) y curl: camino feliz → `ACTIVE`; `simulateFailure=billing` → `FAILED`; `simulateFailure=provisioning` → `FAILED`. Tests de `decide()` en `apps/activation-api` (`pnpm test`).
- **Fase 2: completa.** RF-06 a RF-09 con `docker compose up -d --build`: escenario 1 (4 eventos en verde + email de bienvenida) y escenario 2 (`ProvisioningFailed`, `ActivationFailed`, `BillingAccountCancelled` + email de error), verificados por REST, por WebSocket (un `activation.updated` por entrada del historial), en la API de Mailhog y en la UI desde el navegador. `simulateFailure=billing` muestra el `ProvisioningCompleted` tardío como ignorado. Caso borde de la compensación probado publicando los eventos a mano. Memoria total ≈ 1 GB.
