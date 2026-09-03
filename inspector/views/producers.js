import { dataTable, number, producerId, shortTime } from "../common.js";

export const queries = {
	producers: `SELECT producer_id, epoch, last_seq, last_updated FROM producers ORDER BY last_updated DESC, producer_id LIMIT 100`,
};

export function render({ producers }) {
	const content = dataTable([
		{ label: "Producer", key: "producer_id", className: "mono", render: producerId },
		{ label: "Epoch", key: "epoch", className: "mono" },
		{ label: "Last sequence", key: "last_seq", className: "mono" },
		{ label: "Last active", key: "last_updated", className: "muted", render: shortTime },
	], producers, "No idempotent producers have written to this stream.");
	return { summary: producers.length ? `${number(producers.length)} active` : "No producers", content };
}
