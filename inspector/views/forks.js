import { dataTable, el, empty, field, number, section } from "../common.js";

export const queries = {
	origin: `SELECT forked_from, fork_offset, fork_sub_offset, fork_edge_id, fork_source_gen FROM meta WHERE id = 1 AND forked_from IS NOT NULL LIMIT 1`,
	edges: `SELECT edge_id, fork_offset FROM fork_edges ORDER BY edge_id LIMIT 100`,
};

export function render({ origin, edges }) {
	if (!origin.length && !edges.length) {
		return { summary: "No relationships", content: empty("This stream has no fork relationships.") };
	}
	const content = document.createDocumentFragment();
	if (origin.length) {
		const row = origin[0];
		const details = el("div", "details");
		details.append(
			field("Parent", row.forked_from),
			field("Fork offset", row.fork_offset),
			field("Sub-offset", row.fork_sub_offset),
			field("Edge", row.fork_edge_id),
			field("Source generation", row.fork_source_gen),
		);
		content.append(section("Forked from", "Parent stream", details));
	}
	if (edges.length) {
		content.append(section("Downstream forks", `${number(edges.length)} children`, dataTable([
			{ label: "Edge", key: "edge_id", className: "mono" },
			{ label: "Fork offset", key: "fork_offset", className: "mono" },
		], edges, "No downstream forks.")));
	}
	return { summary: `${number(edges.length)} downstream${origin.length ? " · forked stream" : ""}`, content };
}
