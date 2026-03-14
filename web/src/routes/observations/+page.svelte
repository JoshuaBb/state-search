<script lang="ts">
	import { onMount } from 'svelte';
	import { api, type Observation } from '$lib/api';
	import ObservationsTable from '$lib/components/ObservationsTable.svelte';

	let observations: Observation[] = $state([]);
	let loading = $state(false);
	let error: string | null = $state(null);

	// Filters
	let metricFilter = $state('');
	let sourceFilter = $state('');
	let limit = $state(100);
	let offset = $state(0);

	async function fetchData() {
		loading = true;
		error = null;
		try {
			observations = await api.observations.query({
				metric_name: metricFilter || undefined,
				source_name: sourceFilter || undefined,
				limit,
				offset,
			});
		} catch (e) {
			error = String(e);
		} finally {
			loading = false;
		}
	}

	onMount(fetchData);

	function prev() { if (offset >= limit) { offset -= limit; fetchData(); } }
	function next() { offset += limit; fetchData(); }
	function search() { offset = 0; fetchData(); }
</script>

<div class="page-header">
	<h1>Observations</h1>
</div>

<div class="filters">
	<input
		placeholder="Metric name…"
		bind:value={metricFilter}
		onkeydown={(e) => e.key === 'Enter' && search()}
	/>
	<input
		placeholder="Source name…"
		bind:value={sourceFilter}
		onkeydown={(e) => e.key === 'Enter' && search()}
	/>
	<select bind:value={limit} onchange={search}>
		<option value={50}>50 rows</option>
		<option value={100}>100 rows</option>
		<option value={250}>250 rows</option>
	</select>
	<button onclick={search}>Search</button>
</div>

{#if error}
	<p class="error">{error}</p>
{:else}
	<div class="card" style="padding: 0;">
		<ObservationsTable {observations} {loading} />
	</div>

	<div class="pagination">
		<button class="secondary" onclick={prev} disabled={offset === 0}>← Prev</button>
		<span>rows {offset + 1}–{offset + observations.length}</span>
		<button class="secondary" onclick={next} disabled={observations.length < limit}>Next →</button>
	</div>
{/if}
