<script>
  import { me, api, fetchMe, fmtAge, fmtWhen } from '$lib/store.js';

  let data = $derived($me && $me.user ? $me : null);
  let msg = $state('');

  // token modal state: null = closed; {} = form; {token} = one-shot result
  let modal = $state(null);
  let modalName = $state('');
  let modalError = $state('');
  let copied = $state(false);

  function openModal() {
    modal = {};
    modalName = '';
    modalError = '';
    copied = false;
  }

  async function createToken(e) {
    e.preventDefault();
    if (!modalName) return;
    const { status, data: d } = await api.createToken(modalName.trim());
    if (status === 201) {
      modal = d; // d.token is the plaintext, shown once
      copied = false;
    } else {
      modalError = status === 409 ? 'name taken' : 'failed to create token';
    }
  }

  async function revoke(tok) {
    if (!confirm('revoke token "' + tok.name + '"?')) return;
    const { status } = await api.revokeToken({ name: tok.name });
    if (status === 200) {
      msg = 'revoked ' + tok.name;
      await fetchMe();
    } else {
      msg = 'revoke failed (' + status + ')';
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

{#if data}
  <div class="grid">
    <div class="card">
      <h2>profile</h2>
      <dl>
        <dt>email</dt><dd>{data.user.email}</dd>
        <dt>name</dt><dd>{data.user.display_name}</dd>
        <dt>role</dt><dd>{data.user.role}</dd>
        <dt>status</dt><dd>{data.user.status}</dd>
        {#if data.user.activated_at}<dt>since</dt><dd>{fmtWhen(data.user.activated_at)}</dd>{/if}
      </dl>
    </div>

    <div class="card">
      <h2>routers <span class="dim">({data.routers.length})</span></h2>
      {#if data.routers.length === 0}
        <p class="dim">none registered yet.</p>
      {:else}
        <table>
          <thead><tr><th>id</th><th>status</th><th>last seen</th></tr></thead>
          <tbody>
            {#each data.routers as r}
              <tr>
                <td class="mono">{r.id.slice(0, 8)}...</td>
                <td><span class="dot" class:online={r.status === 'online'}></span>{r.status}</td>
                <td class="dim">{fmtAge(r.last_seen_at)}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      {/if}
    </div>

    <div class="card">
      <h2>models <span class="dim">({data.models.length})</span></h2>
      {#if data.models.length === 0}
        <p class="dim">none loaded on your routers.</p>
      {:else}
        <table>
          <thead><tr><th>model</th><th>loaded</th><th>tags</th><th>excluded</th></tr></thead>
          <tbody>
            {#each data.models as m}
              <tr>
                <td class="mono">{m.name}</td>
                <td>{m.loaded ? 'yes' : 'no'}</td>
                <td class="dim">{m.tags.join(', ')}</td>
                <td>{m.excluded ? 'yes' : ''}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      {/if}
    </div>

    <div class="card">
      <h2>tokens</h2>
      <p class="dim">a token works on any of your routers; the plaintext is shown once, when it is created.</p>
      <table>
        <thead><tr><th>name</th><th>created</th><th>state</th><th></th></tr></thead>
        <tbody>
          {#each data.tokens as tok}
            <tr>
              <td>{tok.name}</td>
              <td class="dim">{fmtWhen(tok.created_at)}</td>
              <td>{tok.revoked ? 'revoked' : 'active'}</td>
              <td class="right">{#if !tok.revoked}<button class="dim" onclick={() => revoke(tok)}>revoke</button>{/if}</td>
            </tr>
          {/each}
        </tbody>
      </table>
      <p><button onclick={openModal}>new token</button></p>
    </div>
  </div>

  <p class="dim note">{msg}</p>

  {#if modal !== null}
    <div class="overlay" onclick={() => (modal = null)}>
      <div class="card modal" onclick={(e) => e.stopPropagation()}>
        {#if modal.token}
          <h2>token created</h2>
          <p class="warn">shown once. copy it now; it cannot be retrieved again.</p>
          <code class="secret mono">{modal.token}</code>
          <p>
            <button onclick={() => copy(modal.token)}>{copied ? 'copied' : 'copy'}</button>
          </p>
          <p class="dim">hand it out of band; the pool never shows it again.</p>
        {:else}
          <h2>new token</h2>
          <form onsubmit={createToken} class="stack">
            <input placeholder="name (e.g. laptop key)" bind:value={modalName} />
            <button type="submit" disabled={!modalName}>create</button>
            {#if modalError}<p class="err">{modalError}</p>{/if}
          </form>
        {/if}
        <p><button class="dim" onclick={() => (modal = null)}>close</button></p>
      </div>
    </div>
  {/if}
{:else if $me === 'anon'}
  <div class="card"><p>not logged in.</p><p><a href="/">log in</a></p></div>
{:else}
  <div class="card"><p class="dim">loading...</p></div>
{/if}
