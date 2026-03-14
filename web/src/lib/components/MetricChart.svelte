<script lang="ts">
	import { onMount, onDestroy } from 'svelte';
	import type { Observation } from '$lib/api';
	import { Chart, BarElement, CategoryScale, LinearScale, Tooltip, Legend, BarController } from 'chart.js';

	Chart.register(BarElement, CategoryScale, LinearScale, Tooltip, Legend, BarController);

	let { observations }: { observations: Observation[] } = $props();

	let canvas: HTMLCanvasElement;
	let chart: Chart | null = null;

	function buildChartData(obs: Observation[]) {
		// Group by time_id, average metric_value
		const byTime = new Map<number, number[]>();
		for (const o of obs) {
			if (o.time_id == null || o.metric_value == null) continue;
			const arr = byTime.get(o.time_id) ?? [];
			arr.push(o.metric_value);
			byTime.set(o.time_id, arr);
		}
		const sorted = [...byTime.entries()].sort(([a], [b]) => a - b);
		return {
			labels: sorted.map(([t]) => `Period ${t}`),
			data:   sorted.map(([, vals]) => vals.reduce((a, b) => a + b, 0) / vals.length),
		};
	}

	function renderChart() {
		if (!canvas) return;
		const { labels, data } = buildChartData(observations);
		if (chart) {
			chart.data.labels = labels;
			(chart.data.datasets[0] as any).data = data;
			chart.update();
			return;
		}
		chart = new Chart(canvas, {
			type: 'bar',
			data: {
				labels,
				datasets: [{
					label:           'Avg value',
					data,
					backgroundColor: 'rgba(99, 102, 241, 0.7)',
					borderColor:     'rgba(99, 102, 241, 1)',
					borderWidth:     1,
				}],
			},
			options: {
				responsive: true,
				plugins: { legend: { display: false } },
				scales: {
					x: { ticks: { color: '#6b7094' }, grid: { color: '#2e3148' } },
					y: { ticks: { color: '#6b7094' }, grid: { color: '#2e3148' } },
				},
			},
		});
	}

	onMount(renderChart);
	onDestroy(() => chart?.destroy());

	$effect(() => {
		observations; // reactive on prop change
		renderChart();
	});
</script>

<canvas bind:this={canvas} height="240"></canvas>
