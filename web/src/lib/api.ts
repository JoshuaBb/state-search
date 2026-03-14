const BASE = '/api';

// ── Types matching Rust models ───────────────────────────────────────────────

export interface ImportSource {
	id:          number;
	name:        string;
	description: string | null;
	field_map:   Record<string, string>;
	created_at:  string;
}

export interface Location {
	id:         number;
	state_code: string | null;
	state_name: string | null;
	country:    string | null;
	zip_code:   string | null;
	fips_code:  string | null;
	latitude:   number | null;
	longitude:  number | null;
}

export interface Observation {
	id:            number;
	raw_import_id: number | null;
	location_id:   number | null;
	time_id:       number | null;
	source_name:   string | null;
	metric_name:   string;
	metric_value:  number | null;
	attributes:    Record<string, unknown> | null;
}

// ── Helpers ──────────────────────────────────────────────────────────────────

async function get<T>(path: string, params?: Record<string, string | number | undefined>): Promise<T> {
	const url = new URL(BASE + path, window.location.origin);
	if (params) {
		for (const [k, v] of Object.entries(params)) {
			if (v !== undefined) url.searchParams.set(k, String(v));
		}
	}
	const res = await fetch(url.toString());
	if (!res.ok) throw new Error(`GET ${path} → ${res.status}`);
	return res.json();
}

async function post<T>(path: string, body: unknown): Promise<T> {
	const res = await fetch(BASE + path, {
		method:  'POST',
		headers: { 'Content-Type': 'application/json' },
		body:    JSON.stringify(body),
	});
	if (!res.ok) throw new Error(`POST ${path} → ${res.status}`);
	return res.json();
}

// ── API surface ──────────────────────────────────────────────────────────────

export const api = {
	health: () => get<{ status: string }>('/health'),

	sources: {
		list:   ()                     => get<ImportSource[]>('/sources'),
		get:    (name: string)         => get<ImportSource>(`/sources/${name}`),
		create: (body: {
			name:        string;
			description?: string;
			field_map:   Record<string, string>;
		}) => post<ImportSource>('/sources', body),
	},

	locations: {
		list: (params?: { limit?: number; offset?: number }) =>
			get<Location[]>('/locations', params),
	},

	observations: {
		query: (params: {
			metric_name?: string;
			source_name?: string;
			location_id?: number;
			limit?:       number;
			offset?:      number;
		}) => get<Observation[]>('/observations', params),
	},
};
