import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vite';

export default defineConfig({
	plugins: [sveltekit()],
	server: {
		proxy: {
			// Forward /api/* to the Axum server in development
			'/api': {
				target:       'http://localhost:3000',
				changeOrigin: true,
			}
		}
	}
});
