# Web Frontend (SvelteKit)

SvelteKit SPA served statically from `web/dist/` by the Rust API server.

## Commands

```bash
npm install        # install dependencies
npm run dev        # dev server (vite, proxies /api to localhost:3000)
npm run build      # build to web/dist/ (static adapter, SPA fallback)
npm run check      # svelte-check type checking
```

## Structure

```
web/src/
  app.html                         # HTML template
  app.css                          # Global styles
  app.d.ts                         # Ambient type declarations
  lib/
    api.ts                         # Typed API client + TypeScript interfaces
    index.ts                       # Re-exports
    components/
      StateMap.svelte              # D3 choropleth (US states, colors by metric)
      MetricChart.svelte           # Chart.js bar/line chart over time
      ObservationsTable.svelte     # Filterable data table
  routes/
    +layout.svelte                 # Root layout (nav)
    +layout.ts                     # Shared data loading logic
    +page.svelte                   # Dashboard (map + chart + summary cards)
    sources/+page.svelte           # Source registry list + create form
    observations/+page.svelte      # Filterable observations table
```

## API Client (`src/lib/api.ts`)

All API calls go through the typed `api` object:

```ts
api.health()
api.sources.list()
api.sources.get(name)
api.sources.create(body)
api.locations.list({ limit, offset })
api.observations.query({ metric_name, source_name, location_id, limit, offset })
```

TypeScript interfaces mirror Rust models:
- `ImportSource` — id, name, description, field_map, created_at
- `Location` — id, state_code, state_name, country, zip_code, fips_code, latitude, longitude
- `Observation` — id, metric_name, metric_value, source_name, location_id, time_id, attributes, raw_import_id

## Key Libraries

| Library | Use |
|---------|-----|
| `d3` v7 | SVG choropleth map (US states) |
| `chart.js` v4 | Bar/line charts over time |
| `topojson-client` | Converts TopoJSON US atlas to GeoJSON for D3 |
| `@sveltejs/adapter-static` | Static output with SPA fallback (index.html) |

## SvelteKit Config Notes

- Adapter: `adapter-static` with `fallback: 'index.html'` (SPA mode — all unmatched routes serve index.html, handled by Axum's fallback service)
- No SSR — this is a pure client-side SPA
- Base path: none (served from `/`)
- API requests in dev proxy to `http://localhost:3000` via Vite config

## Component Notes

**StateMap.svelte**: Draws a US choropleth from `fact_observations`. Color scale is built from min/max of the selected metric across states. Requires `observations` prop (array of `Observation`) and `metric_name` prop.

**MetricChart.svelte**: Time-series bar/line chart. Groups observations by `time_id` year. Requires `observations` and `metric_name` props.

**ObservationsTable.svelte**: Sortable/paginated table of raw observations. Accepts `observations` array and filter state from parent.
