<script>
  import { me, api, fmtWhen } from '$lib/store.js';

  let data = $derived($me && $me.user ? $me : null);
  let users = $state(null);
  let err = $state('');

  let email = $state('');
  let name = $state('');
  let inviteRes = $state(null); // {token} one-shot result, or {error}
  let busy = $state(false);
  let copied = $state(false);
  let note = $state('');

  $effect(() => {
    if (typeof location === 'undefined') return;
    if (location.pathname === '/admin' && data && data.user.role === 'admin') {
      load();
    }
  });

  async function load() {
    const { status, data: d } = await api.adminUsers();
    if (status === 200) {
      users = d.users;
    } else if (status === 403) {
      err = 'not an admin';
      users = [];
    } else {
      err = 'load failed (' + status + ')';
      users = [];
    }
  }

  async function invite(e) {
    e.preventDefault();
    if (!email || !name) return;
    busy = true;
    inviteRes = null;
    copied = false;
    const { status, data: d } = await api.adminInvite(email.trim(), name.trim());
    busy = false;
    if (status === 201) {
      inviteRes = { token: d.token, email: d.user.email, name: d.user.display_name };
      email = '';
      name = '';
      load();
    } else {
      inviteRes = {
        error: status === 409 ? 'that email is already invited' : 'invite failed (' + status + ')'
      };
    }
  }

  async function act(u, kind) {
    if (kind === 'suspend' && !confirm('suspend ' + u.email + '? their sessions and tokens stop working.')) return;
    const { status } = kind === 'suspend' ? await api.adminSuspend(u.id) : await api.adminActivate(u.id);
    if (status === 200) {
      note = u.email + (kind === 'suspend' ? ' suspended' : ' activated');
      load();
    } else if (status === 400) {
      note = 'not allowed: ' + u.email;
    } else {
      note = 'action failed (' + status + ')';
    }
  }

  async function copy(text) {
    try {
      await navigator.clipboard.writeText(text);
      copied = true;
    } catch {
      copied = false;
    }
  }
</script>

{#if $me === 'anon'}
  <div class="card narrow"><p>not logged in.</p><p><a href="/">log in</a></p></div>
{:else if data && data.user.role !== 'admin'}
  <div class="card narrow"><p>not an admin.</p></div>
{:else if users === null}
  <div class="card"><p class="dim">loading...</p></div>
{:else}
  <div class="grid">
    <div class="card">
      <h2>users <span class="dim">({users.length})</span></h2>
      <table>
        <thead><tr><th>email</th><th>name</th><th>role</th><th>status</th><th>routers</th><th>created</th><th></th></tr></thead>
        <tbody>
          {#each users as u}
            <tr>
              <td>{u.email}</td>
              <td>{u.display_name}</td>
              <td>{u.role}</td>
              <td>{u.status}</td>
              <td>{u.routers}</td>
              <td class="dim">{fmtWhen(u.created_at)}</td>
              <td class="right">
                {#if u.role !== 'admin'}
                  {#if u.status !== 'suspended'}
                    <button class="dim" onclick={() => act(u, 'suspend')}>suspend</button>
                  {:else}
                    <button onclick={() => act(u, 'activate')}>activate</button>
                  {/if}
                {/if}
              </td>
            </tr>
          {/each}
        </tbody>
      </table>
      <p class="dim note">{note}</p>
    </div>

    <div class="card">
      <h2>invite</h2>
      <p class="dim">creates a user and a default token; the plaintext is shown once.</p>
      <form onsubmit={invite} class="stack">
        <input type="email" placeholder="email" bind:value={email} disabled={busy} />
        <input placeholder="display name" bind:value={name} disabled={busy} />
        <button type="submit" disabled={busy || !email || !name}>
          {#if busy}creating...{:else}invite{/if}
        </button>
      </form>

      {#if inviteRes}
        <div class="panel">
          {#if inviteRes.token}
            <p class="warn">shown once. copy it now and hand it to {inviteRes.name} out of band; it cannot be retrieved again.</p>
            <code class="secret mono">{inviteRes.token}</code>
            <p>
              <button onclick={() => copy(inviteRes.token)}>{copied ? 'copied' : 'copy'}</button>
            </p>
          {:else}
            <p class="err">{inviteRes.error}</p>
          {/if}
        </div>
      {/if}
    </div>
  </div>
{/if}
