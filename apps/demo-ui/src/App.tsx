import { FormEvent, useEffect, useMemo, useState } from 'react';
import {
  Activation,
  ActivationUpdated,
  connectSocket,
  createActivation,
  fetchActivation,
  fetchRecent,
  HistoryEntry,
  SimulateFailure,
  WS_ACTIVATION_UPDATED,
} from './api';
import { clock, elapsed, isOpen, kindOf, newest, SOURCE_LABEL, STATUS_LABEL } from './events';

type WsState = 'connecting' | 'online' | 'offline';

const FAILURE_OPTIONS: { value: SimulateFailure; label: string }[] = [
  { value: 'none', label: 'Ninguno' },
  { value: 'billing', label: 'Billing' },
  { value: 'provisioning', label: 'Provisioning' },
];

export function App() {
  const [activations, setActivations] = useState<Record<string, Activation>>({});
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [ws, setWs] = useState<WsState>('connecting');
  const [lastResponse, setLastResponse] = useState<{ httpStatus: number; activationId: string; status: string } | null>(null);
  const [error, setError] = useState<string | null>(null);

  const upsert = (a: Activation) => setActivations((prev) => ({ ...prev, [a.activationId]: newest(prev[a.activationId], a) }));

  // RF-08: timeline en vivo por WebSocket. Al (re)conectar se recarga la lista para no perder cambios.
  useEffect(() => {
    const socket = connectSocket();
    const load = () =>
      fetchRecent()
        .then((list) => {
          list.forEach(upsert);
          setSelectedId((cur) => cur ?? list[0]?.activationId ?? null);
        })
        .catch((e) => setError(`No se pudo cargar la lista: ${e.message}`));
    socket.on('connect', () => {
      setWs('online');
      setError(null);
      load();
    });
    socket.on('disconnect', () => setWs('offline'));
    socket.on('connect_error', () => setWs('offline'));
    socket.on(WS_ACTIVATION_UPDATED, (msg: ActivationUpdated) => upsert(msg.activation));
    return () => {
      socket.close();
    };
  }, []);

  const recent = useMemo(
    () => Object.values(activations).sort((a, b) => b.createdAt.localeCompare(a.createdAt)),
    [activations],
  );
  const selected = selectedId ? activations[selectedId] : undefined;

  async function onCreated(res: { httpStatus: number; activationId: string; status: string }) {
    setLastResponse(res);
    setSelectedId(res.activationId);
    // Por si el WS todavía no trajo la foto inicial (o se perdió por una reconexión).
    fetchActivation(res.activationId).then(upsert).catch(() => undefined);
  }

  return (
    <div className="app">
      <header className="topbar">
        <div>
          <h1>Activación de servicios</h1>
          <p className="subtitle">Coreografía por eventos con Kafka · sin llamadas directas entre servicios</p>
        </div>
        <span className={`ws ws-${ws}`} title="WebSocket con activation-api">
          <span className="ws-dot" />
          {ws === 'online' ? 'En vivo' : ws === 'connecting' ? 'Conectando…' : 'Sin conexión'}
        </span>
      </header>

      <OrderForm onCreated={onCreated} onError={setError} />

      {lastResponse && (
        <p className="response" aria-live="polite">
          <code>POST /activations</code> → <strong>{lastResponse.httpStatus} Accepted</strong> ·{' '}
          <code>{lastResponse.activationId}</code> · {lastResponse.status}
        </p>
      )}
      {error && <p className="error">{error}</p>}

      <div className="layout">
        <section className="card recent" aria-label="Activaciones recientes">
          <h2>Recientes</h2>
          {recent.length === 0 && <p className="empty">Todavía no hay activaciones.</p>}
          <ul>
            {recent.map((a) => (
              <li key={a.activationId}>
                <button
                  className={`recent-item ${a.activationId === selectedId ? 'selected' : ''}`}
                  onClick={() => setSelectedId(a.activationId)}
                >
                  <span className="recent-main">
                    <code>{a.activationId}</code>
                    <StatusBadge status={a.status} small />
                  </span>
                  <span className="recent-meta">
                    {a.customerId} · {a.planId}
                    {a.simulateFailure !== 'none' && <span className="tag">fallo: {a.simulateFailure}</span>}
                  </span>
                </button>
              </li>
            ))}
          </ul>
        </section>

        <section className="card timeline-card" aria-label="Timeline de la activación">
          {selected ? <Timeline activation={selected} /> : <p className="empty">Contratá un plan para ver su timeline.</p>}
        </section>
      </div>
    </div>
  );
}

function OrderForm({
  onCreated,
  onError,
}: {
  onCreated: (r: { httpStatus: number; activationId: string; status: string }) => void;
  onError: (e: string | null) => void;
}) {
  const [customerId, setCustomerId] = useState('C-1234');
  const [planId, setPlanId] = useState('Plan Digital');
  const [simulateFailure, setSimulateFailure] = useState<SimulateFailure>('none');
  const [busy, setBusy] = useState(false);

  async function submit(e: FormEvent) {
    e.preventDefault();
    setBusy(true);
    onError(null);
    try {
      onCreated(await createActivation({ customerId, planId, simulateFailure }));
    } catch (err) {
      onError(`No se pudo crear la activación: ${(err as Error).message}`);
    } finally {
      setBusy(false);
    }
  }

  return (
    <form className="card order" onSubmit={submit} aria-label="Contratar plan">
      <label>
        Cliente
        <input value={customerId} onChange={(e) => setCustomerId(e.target.value)} required maxLength={64} />
      </label>
      <label>
        Plan
        <input value={planId} onChange={(e) => setPlanId(e.target.value)} required maxLength={64} />
      </label>
      <fieldset>
        <legend>Forzar fallo</legend>
        <div className="segmented" role="radiogroup">
          {FAILURE_OPTIONS.map((o) => (
            <button
              key={o.value}
              type="button"
              role="radio"
              aria-checked={simulateFailure === o.value}
              className={simulateFailure === o.value ? `on ${o.value !== 'none' ? 'danger' : ''}` : ''}
              onClick={() => setSimulateFailure(o.value)}
            >
              {o.label}
            </button>
          ))}
        </div>
      </fieldset>
      <button type="submit" className="primary" disabled={busy}>
        {busy ? 'Enviando…' : 'Contratar plan'}
      </button>
    </form>
  );
}

function StatusBadge({ status, small }: { status: Activation['status']; small?: boolean }) {
  return <span className={`badge status-${status} ${small ? 'small' : ''}`}>{STATUS_LABEL[status]}</span>;
}

function StepChip({ name, step, open }: { name: string; step?: Activation['steps']['billing']; open: boolean }) {
  const state = step ? (step.status === 'OK' ? 'ok' : 'ko') : open ? 'wait' : 'none';
  const text = step ? (step.status === 'OK' ? 'OK' : 'Falló') : open ? 'esperando…' : 'sin resultado';
  return (
    <span className={`step step-${state}`}>
      <span className="step-dot" />
      {name}: {text}
    </span>
  );
}

function Timeline({ activation: a }: { activation: Activation }) {
  const open = isOpen(a);
  const start = a.history[0]?.at ?? a.createdAt;
  return (
    <>
      <div className="tl-head">
        <div>
          <h2>
            <code>{a.activationId}</code>
          </h2>
          <p className="tl-sub">
            {a.customerId} · {a.planId}
            {a.simulateFailure !== 'none' && <span className="tag">fallo forzado: {a.simulateFailure}</span>}
          </p>
        </div>
        <StatusBadge status={a.status} />
      </div>
      <div className="steps">
        <StepChip name="Billing" step={a.steps.billing} open={open} />
        <StepChip name="Provisioning" step={a.steps.provisioning} open={open} />
      </div>

      <ol className="timeline">
        {a.history.map((h) => (
          <TimelineItem key={h.eventId} entry={h} start={start} />
        ))}
        {open && (
          <li className="tl-item waiting">
            <span className="tl-dot" />
            <div className="tl-body">
              <span className="tl-type">Esperando resultados…</span>
            </div>
          </li>
        )}
      </ol>

      <p className="legend">
        <span className="lg lg-success">éxito</span>
        <span className="lg lg-failure">fallo</span>
        <span className="lg lg-compensation">compensación</span>
        <span className="lg lg-ignored">ignorado (llegó con la activación cerrada)</span>
      </p>
    </>
  );
}

function TimelineItem({ entry: h, start }: { entry: HistoryEntry; start: string }) {
  const kind = kindOf(h);
  return (
    <li className={`tl-item ${kind} ${h.ignored ? 'ignored' : ''}`} data-event-type={h.eventType}>
      <span className="tl-dot" />
      <div className="tl-body">
        <div className="tl-line">
          <span className="tl-type">{h.eventType}</span>
          <span className="tl-source">{SOURCE_LABEL[h.source] ?? h.source}</span>
          {h.ignored && <span className="tl-flag">ignorado · activación ya cerrada</span>}
          {kind === 'compensation' && <span className="tl-flag comp">compensación</span>}
        </div>
        {h.detail && <div className="tl-detail">{h.detail}</div>}
      </div>
      <time className="tl-time" dateTime={h.at}>
        {clock(h.at)}
        <small>{elapsed(start, h.at)}</small>
      </time>
    </li>
  );
}
