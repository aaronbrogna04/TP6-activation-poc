# Guion de la demo — POC Activación de Servicios con Kafka

> Duración: **≈ 15 minutos** para los 5 escenarios (más 3–4 minutos si se suman las dos demos opcionales).
> Fuente: `docs/consigna.pdf`, §10. Arquitectura: [`architecture.md`](architecture.md).

## Disposición de pantalla

```
┌─────────────────────────────┬─────────────────────────────┐
│  Demo UI                    │  Kafka UI                   │
│  http://localhost:5173      │  http://localhost:8080      │
│  (formulario + timeline)    │  (topics, consumers, lag)   │
├─────────────────────────────┴─────────────────────────────┤
│  Terminal (comandos)   ·   Mailhog en otra pestaña         │
│                            http://localhost:8025           │
└───────────────────────────────────────────────────────────┘
```

- **Izquierda:** Demo UI. **Derecha:** Kafka UI. Mailhog en una pestaña del navegador de la izquierda (o de la
  derecha) para alternar cuando llega un email.
- Una terminal abajo, en la raíz del repo, con la fuente grande.
- En Kafka UI el cluster se llama **`activation-poc`**. Menús que se usan: **Topics** y **Consumers**.

Atajos de Kafka UI (pegar en la barra de direcciones):

| Vista | URL |
|---|---|
| Topics | `http://localhost:8080/ui/clusters/activation-poc/all-topics` |
| Mensajes de un topic | `http://localhost:8080/ui/clusters/activation-poc/all-topics/activation.requested/messages` |
| Consumer groups | `http://localhost:8080/ui/clusters/activation-poc/consumer-groups` |
| Un group | `http://localhost:8080/ui/clusters/activation-poc/consumer-groups/notification-svc` |

---

## Checklist previo (10 minutos antes)

1. **Arrancar de cero** (borra topics, offsets y bases):

   ```bash
   docker compose --profile loyalty down -v
   docker compose up -d --build
   ```

   El `--profile loyalty` en el `down` asegura que no quede vivo un loyalty-service de un ensayo anterior.

2. **Esperar los healthchecks** (≈ 1 minuto):

   ```bash
   docker compose ps
   ```

   Tiene que verse: `kafka`, `mongo` y `activation-api` en **`(healthy)`**; `demo-ui`, `kafka-ui`, `mailhog`,
   `billing-service`, `provisioning-service`, `notification-service` y `crm-analytics-service` en **`Up`**.
   `kafka-init` no aparece (ya terminó); para confirmarlo:

   ```bash
   docker compose ps -a kafka-init          # Exited (0)
   docker compose logs kafka-init | tail -9 # los 8 topics, PartitionCount: 3
   curl -s localhost:3000/health            # {"status":"ok"}
   ```

3. **Ensayo en seco** (calienta los consumer groups; en el primer arranque alguno puede loguear una vez
   `The group coordinator is not available`, es normal):

   ```bash
   curl -s -X POST localhost:3000/activations -H 'content-type: application/json' \
     -d '{"customerId":"C-0000","planId":"Plan Warmup"}'
   ```

   A los 3–4 s, `curl -s localhost:3000/activations | jq '.[0].status'` debe dar `"ACTIVE"`.

4. **Vaciar Mailhog** para empezar con la bandeja limpia:

   ```bash
   curl -s -X DELETE localhost:8025/api/v1/messages
   ```

5. **Abrir las pestañas:**
   - Demo UI: <http://localhost:5173> → arriba a la derecha tiene que decir **"En vivo"** (WebSocket conectado).
   - Kafka UI: <http://localhost:8080> → Topics (deben verse los 4 topics + 4 `.dlq`, cada uno con 3 particiones).
   - Mailhog: <http://localhost:8025>.

6. **Verificar todos los groups en lag 0:**

   ```bash
   scripts/consumer-groups.sh
   ```

7. Tener a mano el **video de respaldo** por si algo falla en vivo.

---

## Introducción (≈ 1 min)

**Decir:** "Un cliente contrata un plan. Eso tiene que disparar facturación, aprovisionamiento, un email y un
registro en CRM. La idea de la POC es que `activation-api` publica **un solo evento** en Kafka y cada servicio
reacciona por su cuenta. Ningún servicio llama a otro: no hay ni un HTTP entre servicios."

Mostrar en Kafka UI → **Topics**: los 4 topics de negocio y sus DLQ, 3 particiones cada uno.

---

## Escenario 1 · Camino feliz (≈ 2 min) — *Fan-out*

**Preparación:** ninguna (estado limpio del checklist).

**En la Demo UI:**
1. Cliente `C-1234`, Plan `Plan Digital`, Forzar fallo: **Ninguno**.
2. Clic en **Contratar plan**.
3. Señalar la línea de respuesta: `POST /activations → 202 Accepted · act-… · PENDING`. La API contestó **antes**
   de que pasara nada más.
4. Ver la timeline llenarse en 1–3 s: `ActivationRequested` → `BillingAccountCreated` →
   `ProvisioningCompleted` → `ActivationCompleted`, los **4 en verde**. Los chips Billing / Provisioning pasan a OK
   y el estado a **ACTIVE**.

**En Kafka UI:**
- Topics → `activation.requested` → **Messages**: el mensaje tiene **Key = `C-1234`**, el JSON con el sobre común
  y los headers `eventType`, `correlationId`, `eventId`.
- Consumers: tres groups distintos leyeron ese mismo mensaje: `billing-svc`, `provisioning-svc`, `crm-analytics`.

**En Mailhog:** email "¡Bienvenido! Tu plan Plan Digital ya está activo" para `c-1234@clientes.example.com`.

**Concepto para decir:**
> "Esto es *fan-out*: un único evento en un topic y varios consumer groups que lo leen cada uno por su lado.
> activation-api no sabe quién escucha; billing y provisioning trabajan en paralelo y publican su resultado.
> activation-api solo junta los dos resultados y decide."

**Volver al estado limpio:** no hace falta.

---

## Escenario 2 · Fallo y compensación (≈ 2,5 min) — *Saga sin llamadas directas*

**Preparación:** ninguna.

**En la Demo UI:**
1. Cliente `C-2002`, Plan `Plan Digital`, Forzar fallo: **Provisioning**.
2. **Contratar plan**.
3. En la timeline: `ActivationRequested` → `BillingAccountCreated` (billing sí creó la cuenta) →
   **`ProvisioningFailed`** → **`ActivationFailed`** → **`BillingAccountCancelled`** (marcado *compensación*).
   Estado **FAILED**.

**En Kafka UI:**
- Topics → `billing.events` → Messages: se ven `BillingAccountCreated` y, después, `BillingAccountCancelled`
  con la misma key `C-2002`.
- Topics → `activation.events`: el `ActivationFailed` con `failedStep: "provisioning"`.

**En Mailhog:** email "No pudimos activar tu plan Plan Digital" con el motivo.

**Concepto para decir:**
> "Una saga es una transacción repartida en varios servicios, donde cada paso tiene su compensación. Acá nadie le
> pidió a billing que anule la cuenta: billing escucha `ActivationFailed` y se compensa solo. Si mañana se suma
> otro servicio que tenga que deshacer algo, se suscribe al mismo evento sin tocar a nadie."

*Opcional (20 s):* repetir con fallo en **Billing**. Se ve `BillingFailed` → `ActivationFailed` y un
`ProvisioningCompleted` **gris, "ignorado"**: llegó cuando la activación ya estaba cerrada, queda en el historial
pero no cambia el resultado. No hay `BillingAccountCancelled` porque nunca hubo cuenta.

**Volver al estado limpio:** no hace falta.

---

## Escenario 3 · Servicio caído (≈ 3 min) — *Offsets: nadie pierde mensajes*

**Preparación:** Mailhog visible para contar emails.

**Terminal:**

```bash
docker compose stop notification-service
```

**En la Demo UI:** crear **3 activaciones** sin fallo, con clientes distintos (`C-3001`, `C-3002`, `C-3003`).
Las tres terminan **ACTIVE**: el resto del sistema no se enteró de que notification está caído.

**En Kafka UI:** Consumers → **`notification-svc`**: estado **EMPTY** (sin miembros) y **lag = 3** (sumando las
particiones de `activation.events`). Por CLI, lo mismo:

```bash
scripts/consumer-groups.sh notification-svc
```

**En Mailhog:** no llegó ningún email nuevo.

**Terminal:**

```bash
docker compose start notification-service
```

En unos segundos: **llegan los 3 emails** a Mailhog y en Kafka UI `notification-svc` vuelve a **STABLE** con
**lag 0**.

**Concepto para decir:**
> "Kafka no borra un mensaje cuando alguien lo lee: lo guarda 7 días. Cada consumer group recuerda hasta dónde
> leyó, eso es el *offset*. Mientras notification estaba caído su offset quedó quieto y la diferencia con el final
> del log es el *lag*. Cuando vuelve, sigue desde donde se había quedado."

**Volver al estado limpio:** ya quedó (el servicio está arriba y con lag 0).

---

## Escenario 4 · Escalar (≈ 3 min) — *Consumer group y particiones*

**Preparación:** en Kafka UI abrir Consumers → **`billing-svc`**: hoy tiene **1 miembro** con las 3 particiones
de `activation.requested` y las 3 de `activation.events`.

**Terminal:**

```bash
docker compose up -d --scale billing-service=2
```

Esperar unos segundos (rebalanceo). **Avisar en voz alta** que la pausa es esperable.

**En Kafka UI:** `billing-svc` pasa a **2 miembros** (STABLE). Cada topic queda repartido **2 + 1** entre las dos
instancias (columna *Consumer ID* / *Host* distinta por partición). Por CLI:

```bash
scripts/consumer-groups.sh billing-svc
```

**En la Demo UI:** crear 3 o 4 activaciones con clientes distintos (`C-4001` … `C-4004`), alguna con fallo en
provisioning. Todas terminan bien y la compensación sigue funcionando.

**Terminal** (para ver quién procesó qué):

```bash
docker compose logs --since 1m billing-service | grep 'ActivationRequested ←'
```

Las líneas salen de `billing-service-1` y `billing-service-2`: el trabajo se repartió por partición, y **ninguna
activación se procesó dos veces**.

**Concepto para decir:**
> "Las instancias de un mismo consumer group se reparten las particiones: cada partición la lee una sola
> instancia a la vez. Por eso escalar es levantar otra copia. El límite es la cantidad de particiones: con 3
> particiones, una tercera instancia de billing tendría trabajo y una cuarta quedaría ociosa. Como la clave es el
> `customerId`, todos los eventos de un mismo cliente van a la misma partición y se procesan en orden."

**Volver al estado limpio:**

```bash
docker compose up -d --scale billing-service=1
```

---

## Escenario 5 · Nuevo consumidor (≈ 2,5 min) — *Replay del log*

**Preparación:** en Kafka UI, Consumers: mostrar que **no existe** el group `loyalty-svc`.

**Terminal:**

```bash
docker compose --profile loyalty up -d loyalty-service
docker compose logs -f loyalty-service | grep -A12 RESUMEN
```

(Cortar con Ctrl+C cuando aparezca el resumen.) Se ve
`RESUMEN: procesados N eventos de activation.events (X completadas, Y fallidas) …` y una línea por cliente con
sus puntos (100 por activación completada). Incluye las activaciones de **todos** los escenarios anteriores,
creadas antes de que loyalty existiera.

**En Kafka UI:** aparece el group **`loyalty-svc`** con lag 0. Los otros groups **no cambiaron**: mismos offsets,
mismos miembros.

**Concepto para decir:**
> "Este servicio no existía cuando se crearon esas activaciones. Como Kafka guarda el log, un consumer group
> nuevo puede leerlo desde el principio y reconstruir su propio estado, sin pedirle nada a nadie y sin tocar a los
> demás. Con llamadas directas entre APIs esto no se puede hacer: esos mensajes ya no existirían."

**Cierre (≈ 1 min):** "Un evento, cinco servicios, ninguna llamada directa. Se cae uno y no se pierde nada, se
escala con un comando y se suma un servicio nuevo que procesa todo el historial."

**Volver al estado limpio** (para repetir el escenario desde cero):

```bash
docker compose --profile loyalty rm -sf loyalty-service
docker compose exec -T mongo mongosh --quiet loyalty --eval 'db.dropDatabase()'
docker compose exec -T -e KAFKA_HEAP_OPTS=-Xmx64m kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --delete --group loyalty-svc
```

---

## Demo opcional A · Idempotencia: reenviar un evento ya procesado (≈ 1,5 min)

**Preparación:** anotar cuántos emails hay en Mailhog:

```bash
curl -s localhost:8025/api/v2/messages | jq .total
```

**Terminal:** reenviar el último `ActivationCompleted`/`ActivationFailed` (mismo `eventId`, misma key, mismos
headers):

```bash
scripts/replay-event.sh -t activation.events
docker compose logs --since 30s notification-service billing-service crm-analytics-service | grep 'ya procesado'
```

También se puede reenviar el último `ActivationRequested` (`scripts/replay-event.sh`, sin argumentos) y ver que ni
billing ni provisioning crean otra cuenta u orden.

**Qué se ve:** en Kafka UI el mensaje aparece **dos veces** en el topic (offsets distintos, mismo `eventId`). En
los logs cada consumidor dice `eventId … ya procesado (processed_events); se descarta y se confirma el offset`.
El total de Mailhog **no cambia** y la timeline de la UI tampoco.

**Concepto para decir:**
> "Kafka garantiza entrega *al menos una vez*: si un consumidor se cae justo después de procesar y antes de
> confirmar el offset, recibe el mensaje de nuevo. Por eso cada consumidor guarda el `eventId` de lo que ya
> procesó, en la misma transacción que su efecto. Si llega repetido, lo descarta."

**Volver al estado limpio:** no hace falta.

---

## Demo opcional B · DLQ: mensaje inválido (≈ 1,5 min)

**Terminal:**

```bash
scripts/send-invalid-message.sh            # JSON roto
# o: scripts/send-invalid-message.sh contract   # JSON válido que no cumple el contrato
```

El script publica el mensaje en `activation.requested` con key `C-INVALID`, espera unos segundos y muestra lo que
llegó a la DLQ.

**En Kafka UI:** Topics → **`activation.requested.dlq`** → Messages: **3 copias** del mensaje original (una por
cada group que lo leyó: billing-svc, provisioning-svc, crm-analytics). Abrir una y mostrar los **headers**:
`dlq-reason: INVALID_MESSAGE`, `dlq-error` (el motivo), `dlq-service`, `dlq-original-topic`,
`dlq-original-partition`, `dlq-original-offset`.

**En la Demo UI:** crear una activación normal → termina **ACTIVE**. Consumers: todos los groups en lag 0.

**Concepto para decir:**
> "Un mensaje que no se puede procesar no puede frenar a toda la partición, porque los que vienen atrás quedarían
> esperando. Se aparta a una *dead letter queue* con el motivo en los headers para revisarlo a mano, y el
> consumidor sigue. Los errores técnicos se reintentan 3 veces (1, 2 y 4 segundos) antes de ir a la DLQ; un
> mensaje mal formado va directo, porque reintentarlo no lo arregla. Los fallos de negocio, como los forzados del
> escenario 2, no son errores: son eventos."

*Variante con error técnico (1 min, fuera del guion principal):* `docker compose stop mailhog`, crear una
activación; en `docker compose logs -f notification-service` se ven los intentos 1→2→3→4 y el mensaje va a
`activation.events.dlq` con `dlq-reason: PROCESSING_ERROR`. Después `docker compose start mailhog`.

**Volver al estado limpio:** no hace falta (la DLQ queda con los mensajes; no afecta a nada).

---

## Resumen de tiempos

| Bloque | Tiempo |
|---|---|
| Introducción | 1 min |
| 1 · Camino feliz | 2 min |
| 2 · Fallo y compensación | 2,5 min |
| 3 · Servicio caído | 3 min |
| 4 · Escalar | 3 min |
| 5 · Nuevo consumidor + cierre | 3,5 min |
| **Total** | **≈ 15 min** |
| Opcionales A y B | + 3 min |

## Si algo sale mal en vivo

| Síntoma | Qué hacer |
|---|---|
| La UI dice "Sin conexión" | `docker compose ps activation-api`; recargar la página (reconecta y recarga la lista). |
| Una activación queda en PENDING / IN_PROGRESS | `docker compose logs --tail 50 billing-service provisioning-service activation-api`; ver que los servicios estén `Up`. |
| No llega el email | `docker compose ps notification-service mailhog`; `scripts/consumer-groups.sh notification-svc` para ver el lag. |
| Kafka UI no carga | Esperar: tarda más que Kafka en arrancar. `docker compose restart kafka-ui`. |
| Todo raro | Pasar al video de respaldo. |
