import { bytes, dataTable, el, number, preview, shortTime } from "../common.js";

export const queries = {
	messages: `SELECT msg_offset, length(data) AS bytes, hex(substr(data, 1, 256)) AS preview_hex, ts FROM messages ORDER BY msg_offset DESC LIMIT 100`,
};

export function render({ messages }) {
	const content = dataTable([
		{ label: "Offset", key: "msg_offset", className: "mono" },
		{ label: "Size", key: "bytes", className: "mono muted", render: bytes },
		{
			label: "Preview",
			key: "preview_hex",
			render: (value) => {
				const node = el("div", "preview", preview(value));
				node.title = node.textContent;
				return node;
			},
		},
		{ label: "Written", key: "ts", className: "muted", render: shortTime },
	], messages, "No messages have been appended yet.");
	return { summary: messages.length ? `${number(messages.length)} most recent` : "No messages", content };
}
