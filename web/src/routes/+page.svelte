<script>
  import { page } from '$app/state';
  import { api, fetchMe, me } from '$lib/store.js';

  let token = $state('');
  let error = $state('');
  let busy = $state(false);

  // Already logged in? Straight to the dashboard.
  $effect(() => {
    if (typeof location === 'undefined') return;
    if (location.pathname !== '/') return;
    const data = $me;
    if (data && data.user) {
      location.href = '/dashboard';
    }
  });

  async function submit(e) {
    e.preventDefault();
    if (busy || !token) return;
    busy = true;
    error = '';
    const { status } = await api.login(token.trim());
    busy = false;
    if (status === 200) {
      await fetchMe();
      location.href = '/dashboard';
      return;
    }
    error = 'not a valid active token';
    token = '';
  }
</script>

<div class="login">
  <div class="card">
    <h1>pool portal</h1>
    <p class="dim">invite only. paste the token an admin gave you; it is shown once, when it is created.</p>
    <form onsubmit={submit} class="stack">
      <input
        type="password"
        autocomplete="off"
        spellcheck="false"
        placeholder="token"
        bind:value={token}
        disabled={busy}
      />
      <button type="submit" disabled={busy || !token}>
        {#if busy}checking...{:else}log in{/if}
      </button>
      {#if error}<p class="err">{error}</p>{/if}
    </form>
  </div>

  <div class="card trust">
    <h2>what we commit about your data</h2>
    <ul>
      <li>The coordinator never stores prompts or responses after a request completes (bodies are held only while queued).</li>
      <li>It logs metadata only: sizes, timings, requested class, servicing model, errors, user.</li>
      <li>Honest trust model: your prompt transits the coordinator while in flight, and it is served on the machine of the member who services it.</li>
      <li>Use the pool as you would a favor: for work you would be comfortable running on a friend's machine.</li>
    </ul>
  </div>
</div>
