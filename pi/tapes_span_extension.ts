/**
 * Tapes span-context extension for Pi.
 *
 * Proof-of-concept end-to-end trace/span alignment:
 * - Pi creates stable trace/root span ids at the user-turn boundary.
 * - Each provider request gets a fresh LLM span id.
 * - The extension registers a local OpenAI-compatible `tapes` provider and
 *   selects it by default when TAPES_PROXY_URL is set, so Pi traffic actually
 *   traverses the Tapes proxy instead of the user's default provider.
 * - The extension patches fetch() to attach those ids as X-Pi-* headers
 *   when the request is going to the configured Tapes/Paper proxy.
 * - The Tapes proxy strips the headers before forwarding upstream and stores
 *   the resulting turn using the same ids in span_turns/spans.
 *
 * Configure:
 *   TAPES_PROXY_URL=http://localhost:8080 pi -e ./pi/tapes_span_extension.ts
 *
 * If your Pi provider transport does not expose a URL that matches
 * TAPES_PROXY_URL, set TAPES_SPAN_INJECT_ALL=1 for the POC.
 */

import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

const TRACE_ID_HEADER = "X-Pi-Trace-Id";
const TURN_ID_HEADER = "X-Pi-Turn-Id";
const ROOT_SPAN_ID_HEADER = "X-Pi-Root-Span-Id";
const LLM_SPAN_ID_HEADER = "X-Pi-Llm-Span-Id";
const PARENT_SPAN_ID_HEADER = "X-Pi-Parent-Span-Id";

function compactId(prefix: string): string {
	return `${prefix}_${crypto.randomUUID().replaceAll("-", "")}`;
}

function configuredProxyPrefixes(): string[] {
	return [
		process.env.TAPES_PROXY_URL,
		process.env.PAPER_PROXY_URL,
		process.env.AI_GATEWAY_URL,
	]
		.filter((v): v is string => Boolean(v && v.trim()))
		.map((v) => v.replace(/\/+$/, ""));
}

function requestURL(input: RequestInfo | URL): string {
	if (typeof input === "string") return input;
	if (input instanceof URL) return input.toString();
	return input.url;
}

function shouldInject(input: RequestInfo | URL): boolean {
	if (process.env.TAPES_SPAN_INJECT_ALL === "1") return true;
	const url = requestURL(input);
	return configuredProxyPrefixes().some((prefix) => url.startsWith(prefix));
}

type HeaderBag = Record<string, string>;

let activeHeaders: HeaderBag | undefined;
let patched = false;

function patchFetchOnce(): void {
	if (patched) return;
	patched = true;
	const originalFetch = globalThis.fetch.bind(globalThis);
	globalThis.fetch = async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
		const headersToInject = activeHeaders;
		if (!headersToInject || !shouldInject(input)) {
			return originalFetch(input, init);
		}

		const nextInit: RequestInit = { ...(init ?? {}) };
		const headers = new Headers(nextInit.headers ?? (input instanceof Request ? input.headers : undefined));
		for (const [key, value] of Object.entries(headersToInject)) {
			if (value) headers.set(key, value);
		}
		nextInit.headers = headers;
		return originalFetch(input, nextInit);
	};
}

function routeProviderBase(provider: string): string | undefined {
	const proxy = process.env.TAPES_PROXY_URL?.replace(/\/+$/, "");
	if (!proxy) return undefined;
	return `${proxy}/agents/pi/providers/${provider}`;
}

function proxyModelID(): string {
	return process.env.TAPES_MODEL || process.env.OLLAMA_MODEL || "qwen3-coder:30b";
}

function registerProxyRoutes(pi: ExtensionAPI): void {
	const openAIBaseURL = routeProviderBase("openai");
	if (!openAIBaseURL) return;

	// This provider is the reliable local POC path. Pi's default provider is often
	// Google, and current OpenAI built-ins use the Responses API. Tapes' proxy POC
	// captures OpenAI Chat Completions and maps them to Ollama's /v1-compatible
	// endpoint, so expose an explicit OpenAI-completions provider and switch to it
	// on session_start below.
	pi.registerProvider("tapes", {
		name: "Tapes Local Proxy",
		baseUrl: openAIBaseURL,
		apiKey: "TAPES_PROXY_API_KEY",
		api: "openai-completions",
		models: [
			{
				id: proxyModelID(),
				name: `Tapes Local (${proxyModelID()})`,
				reasoning: false,
				input: ["text"],
				cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
				contextWindow: 128000,
				maxTokens: 8192,
			},
		],
	});

}

export default function tapesSpanExtension(pi: ExtensionAPI): void {
	patchFetchOnce();
	registerProxyRoutes(pi);

	let sessionId = "";
	let currentTraceId = "";
	let currentTurnId = "";
	let currentRootSpanId = "";
	let userTurnPending = false;

	function resetTurnIDs(): void {
		currentTraceId = compactId("trc");
		currentTurnId = compactId("turn");
		currentRootSpanId = compactId("agent");
	}

	function ensureTurnIDs(): void {
		if (!currentTraceId || !currentTurnId || !currentRootSpanId) {
			resetTurnIDs();
		}
	}

	function buildProviderHeaders(): HeaderBag {
		ensureTurnIDs();
		const llmSpanId = compactId("llm");
		return {
			[TRACE_ID_HEADER]: currentTraceId,
			[TURN_ID_HEADER]: currentTurnId,
			[ROOT_SPAN_ID_HEADER]: currentRootSpanId,
			[LLM_SPAN_ID_HEADER]: llmSpanId,
			[PARENT_SPAN_ID_HEADER]: currentRootSpanId,
			// The session id is not consumed by the proxy POC yet; it is useful in
			// packet captures and future header->session-envelope bridges.
			"X-Tapes-Pi-Session-Id": sessionId,
		};
	}

	pi.on("session_start", async (_event, ctx) => {
		sessionId = ctx.sessionManager.getSessionId();
		if (process.env.TAPES_PROXY_PRESERVE_MODEL === "1") return;

		const modelID = proxyModelID();
		const model = ctx.modelRegistry.find("tapes", modelID);
		if (!model) {
			ctx.ui.notify(`Tapes proxy model ${modelID} was not registered`, "error");
			return;
		}
		const selected = await pi.setModel(model);
		if (selected) {
			ctx.ui.setStatus("tapes", `proxy ${modelID}`);
		} else {
			ctx.ui.notify(`Could not select Tapes proxy model ${modelID}`, "error");
		}
	});

	pi.on("before_agent_start", () => {
		userTurnPending = true;
	});

	pi.on("agent_start", (_event, ctx) => {
		const isContinuation = !userTurnPending;
		if (!isContinuation || !currentTraceId || !currentRootSpanId) {
			resetTurnIDs();
		}
		userTurnPending = false;
		ctx.ui.setStatus("tapes", `trace ${currentTraceId.slice(0, 12)}…`);
	});

	pi.on("before_provider_request", () => {
		// Pi emits before_provider_request before message_start, so create the LLM
		// span here. The fetch patch reads activeHeaders synchronously when the
		// provider SDK sends the HTTP request.
		activeHeaders = buildProviderHeaders();
		return undefined;
	});

	pi.on("after_provider_response", () => {
		activeHeaders = undefined;
	});

	pi.on("agent_end", () => {
		activeHeaders = undefined;
	});
}
