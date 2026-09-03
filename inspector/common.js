export function el(tag, className, text) {
	const node = document.createElement(tag);
	if (className) node.className = className;
	if (text != null) node.textContent = text;
	return node;
}

export function empty(text) {
	return el("div", "empty", text);
}

export function number(value) {
	return new Intl.NumberFormat().format(Number(value ?? 0));
}

export function bytes(value) {
	const size = Number(value ?? 0);
	if (size < 1024) return `${size} B`;
	if (size < 1024 ** 2) return `${(size / 1024).toFixed(size < 10240 ? 1 : 0)} KiB`;
	return `${(size / 1024 ** 2).toFixed(1)} MiB`;
}

export function time(value) {
	if (value == null || value === "") return "—";
	const date = new Date(Number(value));
	return Number.isNaN(date.valueOf())
		? String(value)
		: date.toLocaleString([], { dateStyle: "medium", timeStyle: "short" });
}

export function shortTime(value) {
	if (value == null || value === "") return "—";
	const date = new Date(Number(value));
	if (Number.isNaN(date.valueOf())) return String(value);
	const today = new Date();
	return date.toDateString() === today.toDateString()
		? date.toLocaleTimeString([], { hour: "numeric", minute: "2-digit" })
		: date.toLocaleDateString([], { month: "short", day: "numeric" });
}

export function producerId(value) {
	try {
		const raw = String(value).replace(/-/g, "+").replace(/_/g, "/");
		const normalized = raw.padEnd(Math.ceil(raw.length / 4) * 4, "=");
		const values = Uint8Array.from(atob(normalized), (char) => char.charCodeAt(0));
		return new TextDecoder("utf-8", { fatal: true }).decode(values);
	} catch {
		return value == null || value === "" ? "—" : String(value);
	}
}

export function preview(hex) {
	if (!hex) return "Empty message";
	const values = String(hex).match(/.{1,2}/g)?.map((pair) => Number.parseInt(pair, 16)) ?? [];
	return new TextDecoder("utf-8", { fatal: false })
		.decode(new Uint8Array(values))
		.replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/g, "·");
}

export function field(label, value, title = value) {
	const node = el("div", "field");
	node.append(el("div", "field-label", label));
	const content = el("div", "field-value", value == null || value === "" ? "—" : String(value));
	content.title = title == null ? "" : String(title);
	node.append(content);
	return node;
}

export function section(title, note, content) {
	const node = el("section", "section");
	const header = el("div", "section-header");
	header.append(el("span", "section-title", title));
	if (note) header.append(el("span", "section-note", note));
	node.append(header, content);
	return node;
}

export function dataTable(columns, rows, emptyText) {
	if (!rows.length) return empty(emptyText);
	const node = el("table");
	const head = node.createTHead().insertRow();
	for (const column of columns) head.append(el("th", "", column.label));
	const body = node.createTBody();
	for (const row of rows) {
		const tr = body.insertRow();
		for (const column of columns) {
			const td = tr.insertCell();
			if (column.className) td.className = column.className;
			const value = row[column.key];
			const rendered = column.render
				? column.render(value, row)
				: value == null || value === "" ? "—" : String(value);
			if (rendered instanceof Node) td.append(rendered);
			else td.textContent = rendered;
		}
	}
	return node;
}
