import adapter from '@sveltejs/adapter-static';

/** @type {import('@sveltejs/kit').Config} */
const config = {
  kit: {
    // SPA: everything client-side (D-N4). Every route prerenders as an app
    // shell; the coordinator serves index.html for the non-asset paths.
    adapter: adapter({
      pages: 'build',
      assets: 'build',
      precompress: false
    })
  }
};

export default config;
