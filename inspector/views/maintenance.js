import { dataTable, number } from "../common.js";

export const queries = {
	maintenance: `SELECT 'Fork intent' AS kind, edge_id, parent_path, params_key AS detail FROM fork_intents UNION ALL SELECT 'GC release', edge_id, parent_path, source_gen AS detail FROM gc_releases ORDER BY edge_id LIMIT 100`,
};

export function render({ maintenance }) {
	const content = dataTable([
		{ label: "Kind", key: "kind" },
		{ label: "Edge", key: "edge_id", className: "mono" },
		{ label: "Parent", key: "parent_path", className: "mono" },
		{ label: "Detail", key: "detail", className: "mono muted" },
	], maintenance, "There is no pending maintenance work.");
	return { summary: maintenance.length ? `${number(maintenance.length)} pending` : "All caught up", content };
}
