const VIEWS = {
	"stream-overview": () => import("./views/overview.js"),
	"stream-messages": () => import("./views/messages.js"),
	"stream-producers": () => import("./views/producers.js"),
	"stream-forks": () => import("./views/forks.js"),
	"stream-maintenance": () => import("./views/maintenance.js"),
};

function trustedShellOrigin() {
	const raw = new URLSearchParams(location.search).get("shellOrigin");
	try {
		return raw ? new URL(raw).origin : location.origin;
	} catch {
		return location.origin;
	}
}

function tabIdFromUrl() {
	return location.pathname.match(/\/custom-tabs\/([^/]+)/)?.[1] ?? "stream-overview";
}

const SHELL_ORIGIN = trustedShellOrigin();
const initialTheme = new URLSearchParams(location.search).get("theme") ?? "dark";
const app = document.getElementById("app");
const loader = VIEWS[tabIdFromUrl()];
let authToken = null;
let refreshing = false;
let view = null;

document.documentElement.classList.toggle("dark", initialTheme === "dark");

function shell() {
	app.replaceChildren();
	const toolbar = document.createElement("div");
	toolbar.className = "toolbar";
	const summary = document.createElement("span");
	summary.id = "summary";
	summary.className = "toolbar-summary";
	summary.textContent = "Loading…";
	const spacer = document.createElement("span");
	spacer.className = "spacer";
	const updated = document.createElement("span");
	updated.id = "updated";
	updated.className = "updated";
	const refresh = document.createElement("button");
	refresh.id = "refresh";
	refresh.className = "icon-button";
	refresh.type = "button";
	refresh.title = "Refresh";
	refresh.ariaLabel = "Refresh";
	refresh.innerHTML = `<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M13.5 8a5.5 5.5 0 1 1-1.6-3.9"/><path d="M13.5 1.5v3h-3"/></svg>`;
	refresh.addEventListener("click", refreshView);
	toolbar.append(summary, spacer, updated, refresh);

	const error = document.createElement("div");
	error.id = "error";
	error.className = "rivet-error error";
	error.hidden = true;
	const viewport = document.createElement("main");
	viewport.id = "viewport";
	viewport.className = "viewport";
	app.append(toolbar, error, viewport);
}

async function query(sql) {
	const response = await fetch("../../database/execute", {
		method: "POST",
		headers: { Authorization: `Bearer ${authToken}`, "Content-Type": "application/json" },
		body: JSON.stringify({ sql }),
	});
	if (response.status === 401) {
		parent.postMessage({ type: "token-refresh-needed", v: 1 }, SHELL_ORIGIN);
		return null;
	}
	if (!response.ok) throw new Error(`Inspector query failed: ${response.status}`);
	return (await response.json()).rows ?? [];
}

async function refreshView() {
	if (!authToken || refreshing || !view) return;
	refreshing = true;
	const refresh = document.getElementById("refresh");
	const error = document.getElementById("error");
	refresh.classList.add("loading");
	refresh.disabled = true;
	try {
		const entries = await Promise.all(
			Object.entries(view.queries).map(async ([name, sql]) => [name, await query(sql)]),
		);
		if (entries.some(([, rows]) => rows === null)) return;
		const result = view.render(Object.fromEntries(entries));
		document.getElementById("summary").textContent = result.summary;
		document.getElementById("viewport").replaceChildren(result.content);
		document.getElementById("updated").textContent = `Updated ${new Date().toLocaleTimeString([], { hour: "numeric", minute: "2-digit" })}`;
		error.hidden = true;
	} catch (cause) {
		error.textContent = cause?.message ?? String(cause);
		error.hidden = false;
	} finally {
		refreshing = false;
		refresh.classList.remove("loading");
		refresh.disabled = false;
	}
}

if (!loader) {
	const unknown = document.createElement("div");
	unknown.className = "empty";
	unknown.textContent = "Unknown inspector tab.";
	app.replaceChildren(unknown);
} else {
	view = await loader();
	shell();
	window.addEventListener("message", (event) => {
		if (event.origin !== SHELL_ORIGIN || event.data?.type !== "init" || event.data?.v !== 1) return;
		authToken = event.data.authToken;
		document.documentElement.classList.toggle("dark", (event.data.theme ?? initialTheme) === "dark");
		refreshView();
	});
	parent.postMessage({ type: "ready", v: 1 }, SHELL_ORIGIN);
}
