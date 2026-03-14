<script lang="ts">
	import { onMount } from 'svelte';
	import { api, type ImportSource, type Observation } from '$lib/api';
	import StateMap from '$lib/components/StateMap.svelte';
	import MetricChart from '$lib/components/MetricChart.svelte';

	let sources: ImportSource[] = $state([]);
	let observations: Observation[] = $state([]);
	let selectedMetric: string = $state('');
	let loading = $state(true);
	let error: string | null = $state(null);

	// Derived stats
	let metrics = $derived([...new Set(observations.map(o => o.metric_name))].sort());
	let locationCount = $derived(new Set(observations.map(o => o.location_id).filter(Boolean)).size);

	onMount(async () => {
		try {
			[sources, observations] = await Promise.all([
				api.sources.list(),
				api.observations.query({ limit: 1000 }),
			]);
			if (metrics.length > 0) selectedMetric = metrics[0];
		} catch (e) {
			error = String(e);
		} finally {
			loading = false;
		}
	});

	let filteredObs = $derived(
		selectedMetric
			? observations.filter(o => o.metric_name === selectedMetric)
			: observations
	);
</script>

<div class="page-header">
	<h1>Dashboard</h1>
</div>

{#if loading}
	<p class="muted">Loading…</p>
{:else if error}
	<p class="error">{error}</p>
{:else}
	<div class="card-grid">
		<div class="card stat-card">
			<div class="label">Total Observations</div>
			<div class="value">{observations.length.toLocaleString()}</div>
		</div>
		<div class="card stat-card">
			<div class="label">Sources</div>
			<div class="value">{sources.length}</div>
		</div>
		<div class="card stat-card">
			<div class="label">Locations</div>
			<div class="value">{locationCount}</div>
		</div>
		<div class="card stat-card">
			<div class="label">Metrics</div>
			<div class="value">{metrics.length}</div>
		</div>
	</div>

	{#if metrics.length > 0}
		<div class="filters mt-2">
			<select bind:value={selectedMetric}>
				{#each metrics as m}
					<option value={m}>{m}</option>
				{/each}
			</select>
		</div>

		<div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; margin-top: 1rem;">
			<div class="card">
				<h2 style="margin-bottom: 1rem;">By State — {selectedMetric}</h2>
				<StateMap observations={filteredObs} />
			</div>
			<div class="card">
				<h2 style="margin-bottom: 1rem;">Over Time — {selectedMetric}</h2>
				<MetricChart observations={filteredObs} />
			</div>
		</div>
	{:else}
		<div class="card">
			<p class="muted">No data yet. Use the ingest CLI to import a CSV file.</p>
		</div>
	{/if}
{/if}
