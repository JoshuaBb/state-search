<script lang="ts">
	import type { Observation } from '$lib/api';

	let { observations, loading }: { observations: Observation[]; loading: boolean } = $props();

	const cols = [
		{ key: 'id',           label: 'ID'          },
		{ key: 'source_name',  label: 'Source'      },
		{ key: 'metric_name',  label: 'Metric'      },
		{ key: 'metric_value', label: 'Value'       },
		{ key: 'location_id',  label: 'Location ID' },
		{ key: 'time_id',      label: 'Time ID'     },
	] as const;

	function fmt(obs: Observation, key: typeof cols[number]['key']): string {
		const v = obs[key];
		if (v === null || v === undefined) return '—';
		if (key === 'metric_value') return Number(v).toLocaleString(undefined, { maximumFractionDigits: 4 });
		return String(v);
	}
</script>

{#if loading}
	<p class="muted" style="padding: 1rem;">Loading…</p>
{:else if observations.length === 0}
	<p class="muted" style="padding: 1rem;">No observations found.</p>
{:else}
	<div style="overflow-x: auto;">
		<table>
			<thead>
				<tr>
					{#each cols as col}
						<th>{col.label}</th>
					{/each}
				</tr>
			</thead>
			<tbody>
				{#each observations as obs}
					<tr>
						{#each cols as col}
							<td>{fmt(obs, col.key)}</td>
						{/each}
					</tr>
				{/each}
			</tbody>
		</table>
	</div>
{/if}
