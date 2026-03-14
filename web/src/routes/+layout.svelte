<script lang="ts">
	import '../app.css';
	import { page } from '$app/stores';

	let { children } = $props();

	const navLinks = [
		{ href: '/',             label: 'Dashboard'    },
		{ href: '/observations', label: 'Observations' },
		{ href: '/sources',      label: 'Sources'      },
	];
</script>

<div class="shell">
	<nav class="sidebar">
		<div class="brand">State Search</div>
		{#each navLinks as link}
			<a href={link.href} class:active={$page.url.pathname === link.href}>
				{link.label}
			</a>
		{/each}
	</nav>

	<main class="content">
		{@render children()}
	</main>
</div>
