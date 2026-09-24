import React, { useState, useEffect } from 'react';
import { Shield, AlertTriangle, Terminal, Activity, CheckCircle } from 'lucide-react';

function App() {
  const [events, setEvents] = useState([]);
  const [wsStatus, setWsStatus] = useState('Connecting...');

  useEffect(() => {
    // Connect to Go backend WebSocket server
    const socket = new WebSocket('ws://127.0.0.1:8080/ws');

    socket.onopen = () => {
      setWsStatus('Connected');
    };

    socket.onmessage = (event) => {
      try {
        const parsedEvent = JSON.parse(event.data);
        setEvents((prevEvents) => [parsedEvent, ...prevEvents.slice(0, 49)]); // Keep last 50 events
      } catch (err) {
        console.error("Failed to parse event JSON:", err);
      }
    };

    socket.onerror = (err) => {
      console.error("WebSocket Error:", err);
      setWsStatus('Error Connecting');
    };

    socket.onclose = () => {
      setWsStatus('Disconnected');
    };

    return () => socket.close();
  }, []);

  return (
    <div style={{ backgroundColor: '#0f172a', color: '#f8fafc', minHeight: '100vh', fontFamily: 'sans-serif', padding: '24px' }}>
      {/* Header */}
      <header style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', borderBottom: '1px solid #1e293b', paddingBottom: '16px', marginBottom: '24px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
          <Shield style={{ color: '#38bdf8', width: '32px', height: '32px' }} />
          <h1 style={{ fontSize: '24px', fontWeight: 'bold', margin: 0 }}>IsolationSIEM Control Center</h1>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <Activity style={{ color: wsStatus === 'Connected' ? '#22c55e' : '#ef4444', width: '18px', height: '18px' }} />
          <span style={{ fontSize: '14px', color: wsStatus === 'Connected' ? '#22c55e' : '#ef4444' }}>
            {wsStatus}
          </span>
        </div>
      </header>

      {/* Main Grid */}
      <main style={{ display: 'grid', gridTemplateColumns: '1fr', gap: '24px' }}>
        {/* Streamed Log Table */}
        <section style={{ backgroundColor: '#1e293b', borderRadius: '8px', border: '1px solid #334155', padding: '20px' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '16px' }}>
            <Terminal style={{ color: '#38bdf8' }} />
            <h2 style={{ fontSize: '18px', margin: 0 }}>Live Telemetry Stream</h2>
          </div>

          {events.length === 0 ? (
            <div style={{ textAlign: 'center', padding: '40px', color: '#64748b' }}>
              Waiting for incoming agent telemetry...
            </div>
          ) : (
            <div style={{ overflowX: 'auto' }}>
              <table style={{ width: '100%', borderCollapse: 'collapse', textAlign: 'left', fontSize: '14px' }}>
                <thead>
                  <tr style={{ borderBottom: '1px solid #334155', color: '#94a3b8' }}>
                    <th style={{ padding: '12px' }}>Timestamp</th>
                    <th style={{ padding: '12px' }}>Host</th>
                    <th style={{ padding: '12px' }}>Category / Action</th>
                    <th style={{ padding: '12px' }}>Raw Payload</th>
                  </tr>
                </thead>
                <tbody>
                  {events.map((evt, idx) => (
                    <tr key={idx} style={{ borderBottom: '1px solid #334155' }}>
                      <td style={{ padding: '12px', color: '#94a3b8', whiteSpace: 'nowrap' }}>
                        {new Date(evt['@timestamp']).toLocaleTimeString()}
                      </td>
                      <td style={{ padding: '12px', fontWeight: 'bold', color: '#38bdf8' }}>
                        {evt.host?.name || 'Unknown'}
                      </td>
                      <td style={{ padding: '12px' }}>
                        <span style={{ backgroundColor: '#0f172a', padding: '4px 8px', borderRadius: '4px', border: '1px solid #334155' }}>
                          {evt.event?.category} : {evt.event?.action}
                        </span>
                      </td>
                      <td style={{ padding: '12px', fontFamily: 'monospace', color: '#cbd5e1' }}>
                        {evt.log?.original}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </section>
      </main>
    </div>
  );
}

export default App;