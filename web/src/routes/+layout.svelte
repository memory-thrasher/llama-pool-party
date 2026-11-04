<script>
  import '../app.css';
  import { page } from '$app/state';
  import { me, logout, fetchMe } from '$lib/store.js';

  let path = $derived(page.url.pathname);

  // The layout mounts once per browser session (client-side navigation keeps
  // it alive): fetch the profile once; a 401 degrades to the login page.
  $effect(() => {
    fetchMe();
  });
</script>

<div class="shell">
  <header>
    <span class="brand">pool</span>
    <nav>
      {#if $me && $me.user}
        <a href="/dashboard" class:active={path.startsWith('/dashboard')}>dashboard</a>
        <a href="/settings" class:active={path.startsWith('/settings')}>settings</a>
        {#if $me.user.role === 'admin'}
          <a href="/admin" class:active={path.startsWith('/admin')}>admin</a>
        {/if}
        <a href="#" class="dim" onclick={(e) => { e.preventDefault(); logout(); }}>log out</a>
      {:else if path !== '/'}
        <a href="/" class:active={path === '/'}>log in</a>
      {/if}
    </nav>
  </header>
  <main>
    <slot />
  </main>
</div>
