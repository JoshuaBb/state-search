<script lang="ts">
	import { onMount } from 'svelte';
	import { api, type ImportSource } from '$lib/api';

	let sources: ImportSource[] = $state([]);
	let loading = $state(true);
	let error: string | null = $state(null);

	// New source form
	let formName = $state('');
	let formDescription = $state('');
	let formFieldMap = $state('{}');
	let formError: string | null = $state(null);
	let formSubmitting = $state(false);

	onMount(async () => {
		try {
			sources = await api.sources.list();
		} catch (e) {
			error = String(e);
		} finally {
			loading = false;
		}
	});

	async function createSource() {
		formError = null;
		let parsed: Record<string, string>;
		try {
			parsed = JSON.parse(formFieldMap);
		} catch {
			formError = 'field_map must be valid JSON';
			return;
		}

		formSubmitting = true;
		try {
			const src = await api.sources.create({
				name:        formName,
				description: formDescription || undefined,
				field_map:   parsed,
			});
			sources = [...sources, src];
			formName = '';
			formDescription = '';
			formFieldMap = '{}';
		} catch (e) {
			formError = String(e);
		} finally {
			formSubmitting = false;
		}
	}
</script>

<div class="page-header">
	<h1>Sources</h1>
</div>

<div style="display: grid; grid-template-columns: 1fr 360px; gap: 1.5rem; align-items: start;">

	<!-- Source list -->
	<div class="card" style="padding: 0;">
		{#if loading}
			<p class="muted" style="padding: 1rem;">Loading…</p>
		{:else if error}
			<p class="error" style="padding: 1rem;">{error}</p>
		{:else if sources.length === 0}
			<p class="muted" style="padding: 1rem;">No sources registered yet.</p>
		{:else}
			<table>
				<thead>
					<tr>
						<th>Name</th>
						<th>Description</th>
						<th>Fields mapped</th>
						<th>Created</th>
					</tr>
				</thead>
				<tbody>
					{#each sources as src}
						<tr>
							<td><span class="badge">{src.name}</span></td>
							<td class="muted">{src.description ?? '—'}</td>
							<td>{Object.keys(src.field_map).length}</td>
							<td class="muted">{new Date(src.created_at).toLocaleDateString()}</td>
						</tr>
					{/each}
				</tbody>
			</table>
		{/if}
	</div>

	<!-- Register form -->
	<div class="card">
		<h2 style="margin-bottom: 1rem;">Register Source</h2>

		<div style="display: flex; flex-direction: column; gap: 0.75rem;">
			<label>
				<div class="muted" style="margin-bottom: 0.25rem; font-size: 0.8rem;">Name *</div>
				<input style="width: 100%;" placeholder="bls-unemployment" bind:value={formName} />
			</label>

			<label>
				<div class="muted" style="margin-bottom: 0.25rem; font-size: 0.8rem;">Description</div>
				<input style="width: 100%;" placeholder="BLS unemployment by state" bind:value={formDescription} />
			</label>

			<label>
				<div class="muted" style="margin-bottom: 0.25rem; font-size: 0.8rem;">
					Field map (JSON) *
					<span title="Maps canonical name → CSV column. E.g. state_code→STATE, year→YEAR">ⓘ</span>
				</div>
				<textarea
					style="width: 100%; min-height: 120px; background: var(--surface2); border: 1px solid var(--border); border-radius: 6px; color: var(--text); padding: 0.5rem; font-family: monospace; font-size: 0.8rem; resize: vertical;"
					bind:value={formFieldMap}
				></textarea>
			</label>

			{#if formError}
				<p class="error">{formError}</p>
			{/if}

			<button onclick={createSource} disabled={formSubmitting || !formName}>
				{formSubmitting ? 'Saving…' : 'Register'}
			</button>
		</div>
	</div>
</div>
