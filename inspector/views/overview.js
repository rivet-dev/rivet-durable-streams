import { bytes, el, empty, field, number, section, time } from "../common.js";

export const queries = {
	overview: `SELECT gen AS generation, content_type, ttl_seconds, expires_at, closed, current_offset, last_seq, created_at, last_accessed_at, soft_deleted, (SELECT COUNT(*) FROM messages) AS message_count, (SELECT COALESCE(SUM(length(data)), 0) FROM messages) AS stored_bytes, (SELECT COUNT(*) FROM producers) AS producer_count, (SELECT COUNT(*) FROM fork_edges) AS fork_count FROM meta WHERE id = 1 LIMIT 1`,
};

export function render({ overview }) {
	if (!overview.length) return { summary: "Not created", content: empty("This stream has not been created yet.") };
	const meta = overview[0];
	const status = Number(meta.soft_deleted) ? "Deleted" : Number(meta.closed) ? "Closed" : "Open";
	const stats = el("div", "stats");
	for (const [label, value] of [
		["Current offset", meta.current_offset],
		["Messages", number(meta.message_count)],
		["Stored", bytes(meta.stored_bytes)],
		["Status", status],
	]) {
		const stat = el("div", "stat");
		stat.append(el("div", "stat-label", label));
		if (label === "Status") {
			const statusNode = el("div", "stat-value status");
			const dot = el("span", "status-dot");
			dot.dataset.state = status.toLowerCase();
			statusNode.append(dot, document.createTextNode(status));
			stat.append(statusNode);
		} else {
			stat.append(el("div", "stat-value", value));
		}
		stats.append(stat);
	}

	const details = el("div", "details");
	details.append(
		field("Content type", meta.content_type),
		field("Generation", meta.generation),
		field("Last sequence", meta.last_seq),
		field("Created", time(meta.created_at)),
		field("Last accessed", time(meta.last_accessed_at)),
		field("Expires", meta.expires_at ? time(meta.expires_at) : meta.ttl_seconds == null ? "Never" : `${meta.ttl_seconds}s after access`),
	);

	const limits = el("div", "details");
	limits.append(
		field("Maximum write", "1 MB"),
		field("Maximum read", "4 MiB"),
		field("Rows per read", "128"),
		field("Live readers", "64"),
		field("Producers", number(meta.producer_count)),
		field("Downstream forks", number(meta.fork_count)),
	);

	const content = document.createDocumentFragment();
	content.append(stats, section("Stream", "Read-only", details), section("Protocol limits", "Per stream", limits));
	return { summary: `${status} · ${number(meta.message_count)} messages`, content };
}
