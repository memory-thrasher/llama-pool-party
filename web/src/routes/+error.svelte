<script>
  import '../app.css';
  import { page } from '$app/state';
</script>

<div class="shell">
  <main>
    <div class="card">
      <h1>error {page.status}</h1>
      <p>something went wrong.</p>
      <p><a href="/">home</a></p>
    </div>
  </main>
</div>
