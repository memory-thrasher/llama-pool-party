// Tiny hand-rolled store (no state framework) + the portal API client.
// Same-origin fetch for everything; the session is the httpOnly cookie.

function createStore(initial) {
  let value = initial;
  const subs = new Set();
  return {
    get: () => value,
    set(v) {
      value = v;
      subs.forEach((fn) => fn(value));
    },
    subscribe(fn) {
      subs.add(fn);
      fn(value);
      return () => subs.delete(fn);
    }
  };
}

// me: null while unknown, an object when logged in, the string "anon"
// after a 401 (the app is not logged in).
export const me = createStore(null);

// fetchMe loads GET /portal/me; a 401 away from the login page redirects
// home (the degrade path, brief Part 2).
export async function fetchMe() {
  const res = await fetch('/portal/me', { headers: { Accept: 'application/json' } });
  if (res.status === 401) {
    me.set('anon');
    if (typeof location !== 'undefined' && location.pathname !== '/') {
      location.href = '/';
    }
    return null;
  }
  if (!res.ok) {
    me.set('anon');
    return null;
  }
  const data = await res.json();
  me.set(data);
  return data;
}

export async function logout() {
  await fetch('/portal/logout', { method: 'POST' });
  me.set('anon');
  location.href = '/';
}

async function req(path, body) {
  const opts = { headers: { Accept: 'application/json' } };
  if (body !== undefined) {
    opts.method = 'POST';
    opts.headers['Content-Type'] = 'application/json';
    opts.body = JSON.stringify(body);
  }
  const res = await fetch(path, opts);
  const text = await res.text();
  let data = null;
  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      data = text;
    }
  }
  return { status: res.status, data };
}

export const api = {
  login: (token) => req('/portal/login', { token }),
  createToken: (name) => req('/portal/tokens', { name }),
  revokeToken: (ref) => req('/portal/tokens/revoke', ref),
  getSettings: () => req('/portal/settings'),
  saveSettings: (priority_models) => req('/portal/settings', { priority_models }),
  adminUsers: () => req('/portal/admin/users'),
  adminInvite: (email, display_name) => req('/portal/admin/users/invite', { email, display_name }),
  adminSuspend: (id) => req('/portal/admin/users/' + id + '/suspend'),
  adminActivate: (id) => req('/portal/admin/users/' + id + '/activate')
};

// fmtAge renders an ISO instant as a quiet relative age.
export function fmtAge(iso) {
  if (!iso) return 'never';
  const t = Date.parse(iso);
  if (Number.isNaN(t)) return iso;
  const s = Math.max(0, (Date.now() - t) / 1000);
  if (s < 60) return Math.floor(s) + 's ago';
  if (s < 3600) return Math.floor(s / 60) + 'm ago';
  if (s < 86400) return Math.floor(s / 3600) + 'h ago';
  return Math.floor(s / 86400) + 'd ago';
}

// fmtWhen renders an ISO instant as a short fixed string.
export function fmtWhen(iso) {
  if (!iso) return '';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toISOString().slice(0, 16).replace('T', ' ');
}
