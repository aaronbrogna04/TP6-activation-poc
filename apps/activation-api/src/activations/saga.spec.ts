import { decide } from './saga';

const ok = (at: string) => ({ status: 'OK' as const, at, eventType: 'x' });
const ko = (at: string, reason = 'boom') => ({ status: 'FAILED' as const, at, eventType: 'x', reason });

describe('decide (regla de agregación)', () => {
  it('espera mientras falta un resultado', () => {
    expect(decide({ status: 'IN_PROGRESS', steps: { billing: ok('1') } })).toEqual({ kind: 'WAIT' });
    expect(decide({ status: 'PENDING', steps: {} })).toEqual({ kind: 'WAIT' });
  });

  it('completa con billing y provisioning OK', () => {
    expect(decide({ status: 'IN_PROGRESS', steps: { billing: ok('1'), provisioning: ok('2') } })).toEqual({
      kind: 'COMPLETE',
    });
  });

  it('falla con el primer fallo, sin esperar al otro paso', () => {
    expect(decide({ status: 'IN_PROGRESS', steps: { provisioning: ko('1', 'sin cobertura') } })).toEqual({
      kind: 'FAIL',
      failedStep: 'provisioning',
      reason: 'sin cobertura',
    });
  });

  it('falla aunque el otro paso haya dado OK', () => {
    expect(decide({ status: 'IN_PROGRESS', steps: { billing: ok('1'), provisioning: ko('2') } }).kind).toBe('FAIL');
  });

  it('si fallan los dos, informa el primero en el tiempo', () => {
    const d = decide({ status: 'IN_PROGRESS', steps: { billing: ko('2026-01-01T00:00:02Z'), provisioning: ko('2026-01-01T00:00:01Z') } });
    expect(d).toMatchObject({ kind: 'FAIL', failedStep: 'provisioning' });
  });

  it('ignora resultados tardíos cuando la activación ya es final', () => {
    expect(decide({ status: 'FAILED', steps: { billing: ok('1'), provisioning: ko('2') } })).toEqual({ kind: 'WAIT' });
    expect(decide({ status: 'ACTIVE', steps: { billing: ok('1'), provisioning: ok('2') } })).toEqual({ kind: 'WAIT' });
  });

  it('es idempotente ante el mismo resultado repetido', () => {
    const state = { status: 'IN_PROGRESS' as const, steps: { billing: ok('1') } };
    expect(decide(state)).toEqual(decide(state));
  });
});
