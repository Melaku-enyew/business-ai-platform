import React, { Component, ErrorInfo, FormEvent } from 'react';
import { createRoot } from 'react-dom/client';
import { BrowserRouter } from 'react-router-dom';
import './styles.css';

const AUTH_TOKEN_KEY = 'metenovaSessionToken';
const SESSION_ACTIVITY_KEY = 'metenovaLastActivityAt';

const rootElement = document.getElementById('root');

if (rootElement) {
  rootElement.innerHTML = `
    <main class="auth-shell boot-auth-fallback">
      <section class="auth-card">
        <div class="brand auth-brand">
          <span class="brand-icon">AI</span>
          <span>Metenova AI</span>
        </div>
        <p class="eyebrow">Secure workspace</p>
        <h1>Loading Metenova AI</h1>
        <p class="auth-copy">Preparing the login workspace...</p>
      </section>
    </main>
  `;
}

type RootRuntimeBoundaryState = {
  email: string;
  error?: Error;
  message: string;
  password: string;
  submitting: boolean;
};

class RootRuntimeBoundary extends Component<{ children: React.ReactNode; initialError?: Error }, RootRuntimeBoundaryState> {
  state: RootRuntimeBoundaryState = {
    email: '',
    error: this.props.initialError,
    message: this.props.initialError ? this.props.initialError.message : 'Login is available while the workspace recovers.',
    password: '',
    submitting: false
  };

  static getDerivedStateFromError(error: Error) {
    return {
      error,
      message: error.message || 'The workspace shell could not render. Login remains available.'
    };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error('[Metenova Root] Runtime render failure', { error, info });
  }

  async submitEmergencyLogin(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    this.setState({ submitting: true, message: 'Signing in...' });
    try {
      const response = await fetch('/api/auth/login', {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          email: this.state.email,
          password: this.state.password
        })
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok || !payload.token) {
        throw new Error(payload.error || payload.message || 'Login failed. Please verify credentials and retry.');
      }
      sessionStorage.setItem(AUTH_TOKEN_KEY, payload.token);
      sessionStorage.setItem(SESSION_ACTIVITY_KEY, String(Date.now()));
      window.location.assign('/');
    } catch (error) {
      this.setState({
        message: error instanceof Error ? error.message : 'Login failed.',
        submitting: false
      });
    }
  }

  render() {
    if (!this.state.error) return this.props.children;
    return (
      <main className="auth-shell">
        <section className="auth-card">
          <div className="brand auth-brand">
            <span className="brand-icon">AI</span>
            <span>Metenova AI</span>
          </div>
          <p className="eyebrow">Runtime recovery</p>
          <h1>Welcome back</h1>
          <p className="auth-copy">{this.state.message}</p>
          <form className="auth-form" onSubmit={(event) => void this.submitEmergencyLogin(event)}>
            <label>
              Email
              <input
                autoComplete="email"
                type="email"
                value={this.state.email}
                onChange={(event) => this.setState({ email: event.target.value })}
              />
            </label>
            <label>
              Password
              <input
                autoComplete="current-password"
                minLength={8}
                type="password"
                value={this.state.password}
                onChange={(event) => this.setState({ password: event.target.value })}
              />
            </label>
            <button type="submit" disabled={this.state.submitting}>{this.state.submitting ? 'Signing in...' : 'Log in'}</button>
          </form>
          <button className="link-button" type="button" onClick={() => window.location.reload()}>Retry workspace</button>
        </section>
      </main>
    );
  }
}

async function mountApp() {
  if (!rootElement) throw new Error('Metenova root element is missing.');
  const { App } = await import('./App');
  createRoot(rootElement).render(
    <React.StrictMode>
      <RootRuntimeBoundary>
        <BrowserRouter>
          <App />
        </BrowserRouter>
      </RootRuntimeBoundary>
    </React.StrictMode>
  );
}

mountApp().catch((error) => {
  console.error('[Metenova Root] App bootstrap failed', error);
  if (!rootElement) return;
  createRoot(rootElement).render(
    <RootRuntimeBoundary initialError={error instanceof Error ? error : new Error('App bootstrap failed.')} />
  );
});
