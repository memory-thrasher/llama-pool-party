// SPA, no SSR: the shell is prerendered, the client router takes over
// (D-N4: served as static files). Every route prerenders so adapter-static
// needs no fallback page.
export const ssr = false;
export const prerender = true;
