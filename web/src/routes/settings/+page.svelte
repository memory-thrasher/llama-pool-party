<script>
  import { me, api } from '$lib/store.js';

  let lines = $state('');
  let status = $state('');
  let err = $state('');
  let loaded = $state(false);

  $effect(() => {
    if (typeof location === 'undefined') return;
    if (location.pathname !== '/settings') return;
    if ($me && $me.user && !loaded) load();
  });

  async function load() {
    const { status: s, data } = await api.getSettings();
    if (s === 200 && Array.isArray(data.priority_models)) {
      lines = data.priority_models.join('\n');
    }
    loaded = true;
  }

  function names() {
    return lines
      .split('\n')
      .map((l) => l.trim())
      .filter((l) => l !== '');
  }

  async function save(e) {
    e.preventDefault();
    err = '';
    status = '';
    const { status: s, data } = await api.saveSettings(names());
    if (s === 200 && Array.isArray(data.priority_models)) {
      lines = data.priority_models.join('\n');
      status = 'saved';
    } else {
      err = s === 400 ? 'bad entry (no commas, max 32 entries)' : 'save failed (' + s + ')';
    }
  }
</script>

{#if $me === 'anon'}
  <div class="card narrow"><p>not logged in. <a href="/">log in</a>.</p></div>
{:else}
  <div class="card narrow">
    <h2>priority models</h2>
    <p class="dim">
      one model name per line, in order of preference. used to pick among your loaded models
      when several are free.
    </p>
    <form onsubmit={save} class="stack">
      <textarea rows="8" spellcheck="false" placeholder="model-a&#10;model-b" bind:value={lines} class="mono"></textarea>
      <div class="row">
        <button type="submit" disabled={!loaded}>save</button>
        <span class="dim ok">{status}</span>
        <span class="err">{err}</span>
      </div>
      <p class="dim note">saved; dispatch honors this list in a later release.</p>
    </form>
  </div>
{/if}
