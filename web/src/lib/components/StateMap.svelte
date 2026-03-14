<script lang="ts">
	import { onMount } from 'svelte';
	import type { Observation } from '$lib/api';

	let { observations }: { observations: Observation[] } = $props();

	let svg: SVGSVGElement;
	let tooltip = $state<{ x: number; y: number; text: string } | null>(null);

	// State code → average metric value
	let stateValues = $derived(() => {
		const map = new Map<string, number[]>();
		for (const o of observations) {
			// attributes may carry state_code if location isn't resolved
			const code = (o.attributes as any)?.state_code as string | undefined;
			if (!code || o.metric_value == null) continue;
			const arr = map.get(code.toUpperCase()) ?? [];
			arr.push(o.metric_value);
			map.set(code.toUpperCase(), arr);
		}
		return new Map([...map.entries()].map(([k, v]) => [k, v.reduce((a, b) => a + b, 0) / v.length]));
	});

	onMount(async () => {
		// Lazy-load d3 + topojson only when this component mounts
		const [d3, topojson] = await Promise.all([
			import('d3'),
			import('topojson-client'),
		]);

		const us = await fetch('https://cdn.jsdelivr.net/npm/us-atlas@3/states-10m.json').then(r => r.json());
		const states = topojson.feature(us, us.objects.states);
		const stateMesh = topojson.mesh(us, us.objects.states, (a: any, b: any) => a !== b);

		const width  = svg.clientWidth || 600;
		const height = Math.round(width * 0.62);
		svg.setAttribute('viewBox', `0 0 ${width} ${height}`);

		const projection = d3.geoAlbersUsa().fitSize([width, height], states);
		const path = d3.geoPath().projection(projection);

		const vals = stateValues();
		const allVals = [...vals.values()];
		const colorScale = d3.scaleSequential(d3.interpolateBlues)
			.domain([Math.min(...allVals), Math.max(...allVals)]);

		const g = d3.select(svg).append('g');

		// FIPS code → state abbreviation lookup (abbreviated list)
		const fipsToCode: Record<string, string> = {
			'01':'AL','02':'AK','04':'AZ','05':'AR','06':'CA','08':'CO','09':'CT',
			'10':'DE','12':'FL','13':'GA','15':'HI','16':'ID','17':'IL','18':'IN',
			'19':'IA','20':'KS','21':'KY','22':'LA','23':'ME','24':'MD','25':'MA',
			'26':'MI','27':'MN','28':'MS','29':'MO','30':'MT','31':'NE','32':'NV',
			'33':'NH','34':'NJ','35':'NM','36':'NY','37':'NC','38':'ND','39':'OH',
			'40':'OK','41':'OR','42':'PA','44':'RI','45':'SC','46':'SD','47':'TN',
			'48':'TX','49':'UT','50':'VT','51':'VA','53':'WA','54':'WV','55':'WI',
			'56':'WY','11':'DC',
		};

		g.selectAll('path')
			.data((states as any).features)
			.join('path')
			.attr('d', path as any)
			.attr('fill', (d: any) => {
				const code = fipsToCode[String(d.id).padStart(2, '0')];
				const val  = code ? vals.get(code) : undefined;
				return val !== undefined ? colorScale(val) : '#2e3148';
			})
			.attr('stroke', '#0f1117')
			.attr('stroke-width', 0.5)
			.on('mousemove', (event: MouseEvent, d: any) => {
				const code = fipsToCode[String(d.id).padStart(2, '0')] ?? '?';
				const val  = vals.get(code);
				const rect = svg.getBoundingClientRect();
				tooltip = {
					x:    event.clientX - rect.left + 8,
					y:    event.clientY - rect.top  - 28,
					text: val !== undefined
						? `${code}: ${val.toLocaleString(undefined, { maximumFractionDigits: 2 })}`
						: `${code}: no data`,
				};
			})
			.on('mouseleave', () => { tooltip = null; });

		// State borders
		g.append('path')
			.datum(stateMesh)
			.attr('d', path as any)
			.attr('fill', 'none')
			.attr('stroke', '#0f1117')
			.attr('stroke-width', 0.5);
	});
</script>

<div style="position: relative;">
	<svg bind:this={svg} style="width: 100%; height: auto; display: block;"></svg>

	{#if tooltip}
		<div style="
			position: absolute;
			left: {tooltip.x}px;
			top:  {tooltip.y}px;
			background: var(--surface2);
			border: 1px solid var(--border);
			border-radius: 4px;
			padding: 0.3rem 0.6rem;
			font-size: 0.8rem;
			pointer-events: none;
		">{tooltip.text}</div>
	{/if}
</div>
