import { FormEvent, useState } from 'react';

// URL pública de activation-api (la consume el navegador, no el contenedor).
const API_URL = import.meta.env.VITE_API_URL ?? 'http://localhost:3000';

type SimulateFailure = 'none' | 'billing' | 'provisioning';

interface Activation {
  activationId: string;
  status: string;
  customerId?: string;
  planId?: string;
  steps?: Record<string, { status: string; at: string; reason?: string }>;
  history?: { eventType: string; source: string; at: string; ignored?: boolean }[];
}

export function App() {
  const [customerId, setCustomerId] = useState('C-1234');
  const [planId, setPlanId] = useState('Plan Digital');
  const [simulateFailure, setSimulateFailure] = useState<SimulateFailure>('none');
  const [created, setCreated] = useState<{ httpStatus: number; body: Activation } | null>(null);
  const [current, setCurrent] = useState<Activation | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function submit(e: FormEvent) {
    e.preventDefault();
    setBusy(true);
    setError(null);
    setCurrent(null);
    try {
      const res = await fetch(`${API_URL}/activations`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ customerId, planId, simulateFailure }),
      });
      const body = await res.json();
      if (!res.ok) throw new Error(JSON.stringify(body));
      setCreated({ httpStatus: res.status, body });
    } catch (err) {
      setError(String(err));
    } finally {
      setBusy(false);
    }
  }

  async function refresh() {
    if (!created) return;
    setError(null);
    try {
      const res = await fetch(`${API_URL}/activations/${created.body.activationId}`);
      const body = await res.json();
      if (!res.ok) throw new Error(JSON.stringify(body));
      setCurrent(body);
    } catch (err) {
      setError(String(err));
    }
  }

  return (
    <main>
      <h1>Contratar plan</h1>
      <p className="hint">Fase 1: formulario mínimo. La timeline en vivo por WebSocket llega en la fase 2.</p>

      <form onSubmit={submit}>
        <label>
          Cliente (customerId)
          <input value={customerId} onChange={(e) => setCustomerId(e.target.value)} required />
        </label>
        <label>
          Plan (planId)
          <input value={planId} onChange={(e) => setPlanId(e.target.value)} required />
        </label>
        <label>
          Forzar fallo
          <select value={simulateFailure} onChange={(e) => setSimulateFailure(e.target.value as SimulateFailure)}>
            <option value="none">Ninguno</option>
            <option value="billing">Billing</option>
            <option value="provisioning">Provisioning</option>
          </select>
        </label>
        <button type="submit" disabled={busy}>
          {busy ? 'Enviando…' : 'Contratar'}
        </button>
      </form>

      {error && <pre className="error">{error}</pre>}

      {created && (
        <section>
          <h2>
            Respuesta <span className="badge">HTTP {created.httpStatus}</span>
          </h2>
          <pre>{JSON.stringify(created.body, null, 2)}</pre>
          <button onClick={refresh}>Consultar estado</button>
        </section>
      )}

      {current && (
        <section>
          <h2>
            Estado <span className={`badge status-${current.status}`}>{current.status}</span>
          </h2>
          <ol>
            {current.history?.map((h, i) => (
              <li key={i}>
                <code>{h.at}</code> · <strong>{h.eventType}</strong> <small>({h.source})</small>
                {h.ignored && <small> · ignorado (activación ya final)</small>}
              </li>
            ))}
          </ol>
          <details>
            <summary>JSON completo</summary>
            <pre>{JSON.stringify(current, null, 2)}</pre>
          </details>
        </section>
      )}
    </main>
  );
}
