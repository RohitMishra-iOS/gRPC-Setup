import { useState } from 'react';
import { checkStatus, PROXY_URL } from '../grpc/statusService.js';
import './GrpcDemo.css';

export default function GrpcDemo() {
  const [message, setMessage] = useState('');
  const [result, setResult]   = useState(null);
  const [error, setError]     = useState(null);
  const [loading, setLoading] = useState(false);

  async function handleSubmit(e) {
    e.preventDefault();
    if (!message.trim()) return;

    setLoading(true);
    setResult(null);
    setError(null);

    try {
      const data = await checkStatus({ text: message.trim() });
      setResult(data);
    } catch (err) {
      setError(`[${err.code ?? 'ERR'}] ${err.message}`);
    } finally {
      setLoading(false);
    }
  }

  const proxyLabel = PROXY_URL || `${window.location.protocol}//${window.location.hostname}:8080`;

  return (
    <section className="grpc-demo" aria-label="gRPC status check demo">
      <h2>gRPC — CheckStatus</h2>
      <p className="grpc-demo__desc">
        Calls <code>StatusService/CheckStatus</code> via the gRPC-Web proxy at{' '}
        <code>{proxyLabel}</code>.
      </p>

      <form className="grpc-demo__form" onSubmit={handleSubmit}>
        <label htmlFor="grpc-message">Message</label>
        <div className="grpc-demo__row">
          <input
            id="grpc-message"
            type="text"
            placeholder="e.g. ping"
            value={message}
            onChange={(e) => setMessage(e.target.value)}
            disabled={loading}
            autoComplete="off"
          />
          <button type="submit" disabled={loading || !message.trim()}>
            {loading ? 'Calling…' : 'Send'}
          </button>
        </div>
      </form>

      {result && (
        <div className="grpc-demo__result grpc-demo__result--ok" role="status">
          <span className="grpc-demo__label">Response</span>
          <table className="grpc-demo__table">
            <tbody>
              <tr>
                <td className="grpc-demo__key">code</td>
                <td className="grpc-demo__val">{result.code}</td>
              </tr>
              <tr>
                <td className="grpc-demo__key">status</td>
                <td className="grpc-demo__val">{result.status}</td>
              </tr>
              <tr>
                <td className="grpc-demo__key">message</td>
                <td className="grpc-demo__val">{JSON.stringify(result.message)}</td>
              </tr>
            </tbody>
          </table>
        </div>
      )}

      {error && (
        <div className="grpc-demo__result grpc-demo__result--error" role="alert">
          <span className="grpc-demo__label">Error</span>
          <span className="grpc-demo__value">{error}</span>
        </div>
      )}
    </section>
  );
}
