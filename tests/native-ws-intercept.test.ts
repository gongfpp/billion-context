import test from "node:test";
import { AsyncLocalStorage } from "node:async_hooks";
import assert from "node:assert/strict";
import { liveStateSlotEffect, type NativeInterceptState } from "../src/agent/native-intercept.ts";
import { _resetForTest, codexResponsesWsTarget, installNativeWebSocketIntercept, nativeWebSocketInterceptInstalled, rewriteCodexResponsesWsUrl } from "../src/agent/native-ws-intercept.ts";

// Same @types/node quirk as in src: the listener callback type is not global.
type EventListener = (event: Event) => void;

// ——— Fakes ————————————————————————————————————————————————————————————————

class FakeWs {
    static CONNECTING = 0;
    static OPEN = 1;
    static CLOSING = 2;
    static CLOSED = 3;
    static instances: FakeWs[] = [];
    /** When set, every construction throws — seam for the total-failure path. */
    static ctorFail = false;
    readonly ctorArgs: unknown[];
    url: string;
    readyState = 0;
    bufferedAmount = 0;
    protocol = "";
    extensions = "";
    binaryType = "blob";
    onopen: ((e: unknown) => void) | null = null;
    onerror: ((e: unknown) => void) | null = null;
    onclose: ((e: unknown) => void) | null = null;
    onmessage: ((e: unknown) => void) | null = null;
    sent: string[] = [];
    closed = false;
    private listeners = new Map<string, Array<{ fn: EventListener; once: boolean }>>();

    constructor(...ctorArgs: unknown[]) {
        if (FakeWs.ctorFail) throw new TypeError("FakeWs construction disabled (test seam)");
        this.ctorArgs = ctorArgs;
        this.url = String(ctorArgs[0] ?? "");
        FakeWs.instances.push(this);
    }

    addEventListener(type: string, listener: EventListener | null, options?: { once?: boolean } | boolean): void {
        if (listener === null) return;
        const once = typeof options === "object" && options !== null ? options.once === true : options === true;
        const list = this.listeners.get(type) ?? [];
        list.push({ fn: listener, once });
        this.listeners.set(type, list);
    }

    removeEventListener(type: string, listener: EventListener | null): void {
        const list = this.listeners.get(type);
        if (list !== undefined) {
            const at = list.findIndex((entry) => entry.fn === listener);
            if (at >= 0) list.splice(at, 1);
        }
    }

    dispatchEvent(event: Event): boolean {
        for (const entry of [...(this.listeners.get(event.type) ?? [])]) entry.fn.call(this, event);
        return true;
    }

    send(data: unknown): void {
        this.sent.push(String(data));
    }

    close(): void {
        this.closed = true;
        this.readyState = FakeWs.CLOSING;
    }

    emit(type: "open" | "error" | "close" | "message"): void {
        const event = new Event(type);
        this.dispatchEvent(event);
        const handler = this[`on${type}`] as ((e: unknown) => void) | null | undefined;
        handler?.(event);
    }
}

type GlobalRecord = Record<PropertyKey, unknown>;

function savedDescriptor(): PropertyDescriptor | undefined {
    return Object.getOwnPropertyDescriptor(globalThis, "WebSocket");
}

function setGlobal(value: unknown): void {
    (globalThis as GlobalRecord).WebSocket = value;
}

function WsCtor<T>(): new (...args: unknown[]) => T {
    return (globalThis as GlobalRecord).WebSocket as unknown as new (...args: unknown[]) => T;
}

async function withPatchedGlobal<T>(fn: () => Promise<T>): Promise<T> {
    const saved = savedDescriptor();
    _resetForTest();
    if (saved !== undefined) Object.defineProperty(globalThis, "WebSocket", saved);
    FakeWs.instances.length = 0;
    setGlobal(FakeWs);
    try {
        return await fn();
    } finally {
        _resetForTest();
        if (saved !== undefined) Object.defineProperty(globalThis, "WebSocket", saved);
    }
}

function pendingState(): NativeInterceptState & { readyResolve: (o: string | undefined) => void } {
    let readyResolve!: (o: string | undefined) => void;
    const ready = new Promise<string | undefined>((resolve) => {
        readyResolve = resolve;
    });
    return { origin: undefined, ready, readyResolve };
}

const ORIGIN = "http://127.0.0.1:8787";
const UPSTREAM_WS = "wss://chatgpt.com/backend-api/codex/responses";
const REWRITTEN = `ws://127.0.0.1:8787/bili/https://chatgpt.com/backend-api/codex/responses`;
const tick = (): Promise<void> => new Promise((resolve) => setImmediate(resolve));

// ——— Claim predicate / rewrite (pure) ————————————————————————————————————

test("codexResponsesWsTarget: claims the Codex Responses shape, normalized to http(s)", () => {
    assert.equal(codexResponsesWsTarget("wss://chatgpt.com/backend-api/codex/responses"), "https://chatgpt.com/backend-api/codex/responses");
    assert.equal(codexResponsesWsTarget("ws://127.0.0.1:9999/backend-api/codex/responses"), "http://127.0.0.1:9999/backend-api/codex/responses");
    assert.equal(codexResponsesWsTarget("wss://chatgpt.com/backend-api/codex/responses?beta=1"), "https://chatgpt.com/backend-api/codex/responses?beta=1");
    assert.equal(codexResponsesWsTarget("wss://relay.example.com/v1/responses/"), "https://relay.example.com/v1/responses/");
    assert.equal(codexResponsesWsTarget(new URL("wss://chatgpt.com/backend-api/codex/responses")), "https://chatgpt.com/backend-api/codex/responses");
});

test("codexResponsesWsTarget: rejects non-model, already-routed, control-plane and non-ws URLs", () => {
    assert.equal(codexResponsesWsTarget("wss://example.com/socket.io?transport=websocket"), undefined);
    assert.equal(codexResponsesWsTarget("wss://api.anthropic.com/v1/messages"), undefined);
    assert.equal(codexResponsesWsTarget("wss://api.openai.com/v1/chat/completions"), undefined);
    assert.equal(codexResponsesWsTarget("wss://bedrock-runtime.us-east-1.amazonaws.com/converse-stream"), undefined);
    assert.equal(codexResponsesWsTarget("wss://127.0.0.1:8787/bili/https://chatgpt.com/backend-api/codex/responses"), undefined);
    assert.equal(codexResponsesWsTarget("wss://127.0.0.1:8787/__acp/status"), undefined);
    assert.equal(codexResponsesWsTarget("wss://127.0.0.1:8787/__bili/plugin/manifest"), undefined);
    assert.equal(codexResponsesWsTarget("https://chatgpt.com/backend-api/codex/responses"), undefined);
    assert.equal(codexResponsesWsTarget("not a url"), undefined);
    assert.equal(codexResponsesWsTarget("wss://bad host/x/responses"), undefined);
});

test("rewriteCodexResponsesWsUrl: ws(s) proxy side, http(s) embedded upstream", () => {
    assert.equal(rewriteCodexResponsesWsUrl("https://chatgpt.com/backend-api/codex/responses", ORIGIN), REWRITTEN);
    assert.equal(rewriteCodexResponsesWsUrl("https://chatgpt.com/backend-api/codex/responses", `${ORIGIN}/`), REWRITTEN);
    assert.equal(rewriteCodexResponsesWsUrl("https://h.example/v1/responses", "https://proxy.example"), "wss://proxy.example/bili/https://h.example/v1/responses");
});

// ——— Install mechanics —————————————————————————————————————————————————————

test("install: idempotent, flagged, stable top across repeated loads", async () => {
    await withPatchedGlobal(async () => {
        const state: NativeInterceptState = { origin: ORIGIN, ready: Promise.resolve(ORIGIN) };
        assert.equal(nativeWebSocketInterceptInstalled(), false);
        assert.equal(installNativeWebSocketIntercept(state), true);
        assert.equal(nativeWebSocketInterceptInstalled(), true);
        const first = (globalThis as GlobalRecord).WebSocket;
        assert.equal(installNativeWebSocketIntercept(state), false);
        assert.equal((globalThis as GlobalRecord).WebSocket, first);
    });
});

test("install: refuses when no global WebSocket exists", async () => {
    const saved = savedDescriptor();
    _resetForTest();
    Object.defineProperty(globalThis, "WebSocket", { value: undefined, configurable: true, writable: true });
    try {
        const state: NativeInterceptState = { origin: ORIGIN, ready: Promise.resolve(ORIGIN) };
        assert.equal(installNativeWebSocketIntercept(state), false);
        assert.equal(nativeWebSocketInterceptInstalled(), false);
    } finally {
        _resetForTest();
        if (saved !== undefined) Object.defineProperty(globalThis, "WebSocket", saved);
    }
});

test("statics and name are carried onto the wrapper", async () => {
    await withPatchedGlobal(async () => {
        const state: NativeInterceptState = { origin: ORIGIN, ready: Promise.resolve(ORIGIN) };
        installNativeWebSocketIntercept(state);
        const top = (globalThis as GlobalRecord).WebSocket as unknown as { CONNECTING: number; OPEN: number; CLOSING: number; CLOSED: number; name: string };
        assert.deepEqual([top.CONNECTING, top.OPEN, top.CLOSING, top.CLOSED], [0, 1, 2, 3]);
        assert.equal(top.name, "WebSocket");
    });
});

// ——— Routing decisions —————————————————————————————————————————————————————

test("passthrough: non-model WS keeps URL and arguments verbatim", async () => {
    await withPatchedGlobal(async () => {
        const state: NativeInterceptState = { origin: ORIGIN, ready: Promise.resolve(ORIGIN) };
        installNativeWebSocketIntercept(state);
        const init = { headers: { "sec-x": "1" }, protocols: ["a"] };
        const ws = new (WsCtor<FakeWs>())("wss://example.com/devtools", init);
        assert.ok(ws instanceof FakeWs);
        assert.equal(ws.url, "wss://example.com/devtools");
        assert.equal(ws.ctorArgs[1], init);
    });
});

test("immediate rewrite: origin known → routed URL, args forwarded verbatim, evidence recorded", async () => {
    await withPatchedGlobal(async () => {
        const dispatched: Array<[string, string]> = [];
        const state: NativeInterceptState = {
            origin: ORIGIN,
            ready: Promise.resolve(ORIGIN),
            onDispatch: (url, action) => dispatched.push([url, action]),
        };
        installNativeWebSocketIntercept(state);
        const init = { headers: { "session-id": "sess-1", "x-bili-plugin": "pi" } };
        const ws = new (WsCtor<FakeWs>())(UPSTREAM_WS, init);
        assert.ok(ws instanceof FakeWs);
        assert.equal(ws.url, REWRITTEN);
        assert.equal(ws.ctorArgs[1], init);
        assert.deepEqual(dispatched, [[REWRITTEN, "rewrite"]]);
        assert.equal(state.routedOrigin, ORIGIN);
    });
});

test("already-routed /bili/ WS is never double-prefixed", async () => {
    await withPatchedGlobal(async () => {
        const state: NativeInterceptState = { origin: ORIGIN, ready: Promise.resolve(ORIGIN) };
        installNativeWebSocketIntercept(state);
        const ws = new (WsCtor<FakeWs>())(REWRITTEN, {});
        assert.equal(ws.url, REWRITTEN);
    });
});

test("takeoverGate refusal: unattributable connection goes direct", async () => {
    await withPatchedGlobal(async () => {
        const dispatched: Array<[string, string]> = [];
        const state: NativeInterceptState = {
            origin: ORIGIN,
            ready: Promise.resolve(ORIGIN),
            takeoverGate: () => false,
            onDispatch: (url, action) => dispatched.push([url, action]),
        };
        installNativeWebSocketIntercept(state);
        const ws = new (WsCtor<FakeWs>())(UPSTREAM_WS, {});
        assert.equal(ws.url, UPSTREAM_WS);
        assert.deepEqual(dispatched, [["https://chatgpt.com/backend-api/codex/responses", "refused"]]);
    });
});

test("instanceof holds against both the wrapper and the underlying constructor", async () => {
    await withPatchedGlobal(async () => {
        const state: NativeInterceptState = { origin: ORIGIN, ready: Promise.resolve(ORIGIN) };
        installNativeWebSocketIntercept(state);
        const top = (globalThis as GlobalRecord).WebSocket as unknown as new (...args: unknown[]) => unknown;
        const ws = new top(UPSTREAM_WS, {});
        assert.ok(ws instanceof FakeWs);
        assert.ok(ws instanceof top);
    });
});

// ——— Live-state routing across hot-reimport (#3558) ————————————————

test("install: a hot-reimport rebinds WS routing to the new instance's state (#3558)", async () => {
    await withPatchedGlobal(async () => {
        // Two distinct module instances: A booted first, B is the FRESH state a
        // host hot-reimport re-applies in the SAME process with no reset between.
        const dispatchedA: Array<[string, string]> = [];
        const dispatchedB: Array<[string, string]> = [];
        const stateA: NativeInterceptState = {
            origin: "http://127.0.0.1:40901",
            ready: Promise.resolve("http://127.0.0.1:40901"),
            onDispatch: (url, action) => dispatchedA.push([url, action]),
        };
        const stateB: NativeInterceptState = {
            origin: "http://127.0.0.1:40902",
            ready: Promise.resolve("http://127.0.0.1:40902"),
            onDispatch: (url, action) => dispatchedB.push([url, action]),
        };
        assert.equal(installNativeWebSocketIntercept(stateA), true);
        // The hot-reimport path: install again with B's state. One wrapper per
        // process (returns false) but the shared live slot must rebind to B.
        assert.equal(installNativeWebSocketIntercept(stateB), false);
        const ws = new (WsCtor<FakeWs>())(UPSTREAM_WS, {});
        assert.ok(ws instanceof FakeWs);
        // Routing must follow B — the newest applied instance: B's origin rewrite
        // AND B's dispatch evidence. Pre-#3558 the wrapper closed over A → 40901.
        assert.equal(ws.url, "ws://127.0.0.1:40902/bili/https://chatgpt.com/backend-api/codex/responses");
        assert.deepEqual(dispatchedB, [["ws://127.0.0.1:40902/bili/https://chatgpt.com/backend-api/codex/responses", "rewrite"]]);
        assert.deepEqual(dispatchedA, []);
    });
});

test("install: a same-instance re-activation outranks an older activation's late dispose (#3558)", async () => {
    await withPatchedGlobal(async () => {
        // Deactivate/reactivate (#2082/#2101) re-applies the SAME module-level
        // state object under a fresh context. If activation 1's effect cleanup
        // fires LATE — after activation 2's apply() already rebound the slot —
        // object identity cannot tell the activations apart; only the install
        // generation can. A wrong clear silently drops WS routing to direct.
        const state: NativeInterceptState = { origin: ORIGIN, ready: Promise.resolve(ORIGIN) };
        assert.equal(installNativeWebSocketIntercept(state), true);
        // Activation 1's effect runs post-commit: capture its disposer.
        const disposeActivation1 = liveStateSlotEffect(state)();
        // Re-activation: same state object, fresh context, no reset in between.
        assert.equal(installNativeWebSocketIntercept(state), false);
        disposeActivation1();
        const ws = new (WsCtor<FakeWs>())(UPSTREAM_WS, {});
        // The late dispose must be a no-op: routing still rides the rebound slot.
        assert.equal(ws.url, REWRITTEN);
    });
});

test("install: the latest activation's own teardown clears the slot to clean direct (#3558)", async () => {
    await withPatchedGlobal(async () => {
        const state: NativeInterceptState = { origin: ORIGIN, ready: Promise.resolve(ORIGIN) };
        assert.equal(installNativeWebSocketIntercept(state), true);
        // The activation's own teardown fires: generation + identity both match
        // the current slot, so the clear lands.
        liveStateSlotEffect(state)()();
        const ws = new (WsCtor<FakeWs>())(UPSTREAM_WS, {});
        // Empty slot → the model WS shape passes through direct, unrewritten.
        assert.equal(ws.url, UPSTREAM_WS);
    });
});

// ——— Deferred shell (origin still settling) ——————————————————————————————————

test("deferred: window mirrors the WebSocket API, then attaches to the routed connection", async () => {
    await withPatchedGlobal(async () => {
        const state = pendingState();
        installNativeWebSocketIntercept(state);
        const top = (globalThis as GlobalRecord).WebSocket as unknown as new (...args: unknown[]) => FakeWs;
        const init = { headers: { "session-id": "sess-deferred" } };
        const ws = new top(UPSTREAM_WS, init) as unknown as {
            readyState: number;
            url: string;
            send: (d: unknown) => void;
            close: () => void;
            addEventListener: (t: string, f: EventListener) => void;
            onopen: ((e: unknown) => void) | null;
            binaryType: string;
        };
        assert.ok(!(ws instanceof FakeWs), "shell stands in before the origin settles");
        assert.equal(ws.readyState, 0);
        assert.equal(ws.url, UPSTREAM_WS);
        assert.throws(() => ws.send("early"), (err: Error) => err.name === "InvalidStateError");
        const opens: Array<"event" | "onopen"> = [];
        ws.addEventListener("open", (() => { opens.push("event"); }) as EventListener);
        ws.onopen = () => { opens.push("onopen"); };
        ws.binaryType = "arraybuffer";
        state.readyResolve(ORIGIN);
        await tick();
        await tick();
        const [real] = FakeWs.instances;
        assert.equal(real.url, REWRITTEN);
        assert.equal(real.ctorArgs[1], init);
        assert.equal(ws.readyState, 0);
        assert.equal(real.binaryType, "arraybuffer");
        real.emit("open");
        assert.deepEqual(opens, ["event", "onopen"]);
    });
});

test("deferred: close() during the window cancels and emits an abnormal close frame", async () => {
    await withPatchedGlobal(async () => {
        const state = pendingState();
        installNativeWebSocketIntercept(state);
        const top = (globalThis as GlobalRecord).WebSocket as unknown as new (...args: unknown[]) => {
            readyState: number;
            close: () => void;
            addEventListener: (t: string, f: EventListener) => void;
        };
        const ws = new top(UPSTREAM_WS, {});
        const closes: Array<number> = [];
        ws.addEventListener("close", ((e: { code?: number }) => { closes.push(e.code ?? -1); }) as EventListener);
        ws.close();
        assert.equal(ws.readyState, 2, "CLOSING while cancelled");
        state.readyResolve(ORIGIN);
        await tick();
        await tick();
        assert.equal(FakeWs.instances.length, 0, "no connection was ever opened");
        assert.deepEqual(closes, [1006]);
    });
});

test("deferred: close() during the window reaches onclose property handlers", async () => {
    await withPatchedGlobal(async () => {
        const state = pendingState();
        installNativeWebSocketIntercept(state);
        const top = (globalThis as GlobalRecord).WebSocket as unknown as new (...args: unknown[]) => {
            readyState: number;
            close: () => void;
            onclose: EventListener | null;
        };
        const ws = new top(UPSTREAM_WS, {});
        const closes: Array<number> = [];
        ws.onclose = ((e: { code?: number }) => { closes.push(e.code ?? -1); }) as EventListener;
        ws.close();
        assert.equal(ws.readyState, 2, "CLOSING while cancelled");
        state.readyResolve(ORIGIN);
        await tick();
        await tick();
        assert.equal(FakeWs.instances.length, 0, "no connection was ever opened");
        assert.deepEqual(closes, [1006], "the on* property handler received the abnormal close frame");
    });
});

test("deferred: total construct failure surfaces error + close instead of an unhandled rejection", async () => {
    await withPatchedGlobal(async () => {
        const state = pendingState();
        installNativeWebSocketIntercept(state);
        const top = (globalThis as GlobalRecord).WebSocket as unknown as new (...args: unknown[]) => {
            readyState: number;
            addEventListener: (t: string, f: EventListener) => void;
        };
        const ws = new top(UPSTREAM_WS, {});
        const errors: Array<unknown> = [];
        const closes: Array<number> = [];
        ws.addEventListener("error", ((e: { error?: unknown }) => { errors.push(e.error); }) as EventListener);
        ws.addEventListener("close", ((e: { code?: number }) => { closes.push(e.code ?? -1); }) as EventListener);
        FakeWs.ctorFail = true;
        try {
            state.readyResolve(ORIGIN);
            await tick();
            await tick();
            assert.equal(FakeWs.instances.length, 0, "no connection was constructed");
            assert.ok(errors.length === 1 && errors[0] instanceof TypeError, "error event carries the constructor failure");
            assert.deepEqual(closes, [1006], "abnormal close follows the error");
            assert.equal(ws.readyState, FakeWs.CLOSED, "failed preparation/connection must not remain CONNECTING");
        } finally {
            FakeWs.ctorFail = false;
        }
    });
});

test("deferred: a permanently absent proxy degrades DIRECT with the original URL", async () => {
    await withPatchedGlobal(async () => {
        const state = pendingState();
        installNativeWebSocketIntercept(state);
        const top = (globalThis as GlobalRecord).WebSocket as unknown as new (...args: unknown[]) => FakeWs;
        const ws = new top(UPSTREAM_WS, {});
        state.readyResolve(undefined);
        await tick();
        await tick();
        const [real] = FakeWs.instances;
        assert.equal(real.url, UPSTREAM_WS);
        assert.equal(ws.url, UPSTREAM_WS);
    });
});

// ——— Third-party adoption —————————————————————————————————————————————————————

class FakeWs2 extends FakeWs {}

test("adoption: a third-party constructor takes over as downstream, routing survives", async () => {
    await withPatchedGlobal(async () => {
        const state: NativeInterceptState = { origin: ORIGIN, ready: Promise.resolve(ORIGIN) };
        installNativeWebSocketIntercept(state);
        setGlobal(FakeWs2);
        const ws = new (WsCtor<FakeWs2>())(UPSTREAM_WS, {});
        assert.ok(ws instanceof FakeWs2);
        assert.equal(ws.url, REWRITTEN);
    });
});

test("adoption: non-constructor replacements are refused, previous constructor kept", async () => {
    await withPatchedGlobal(async () => {
        const state: NativeInterceptState = { origin: ORIGIN, ready: Promise.resolve(ORIGIN) };
        installNativeWebSocketIntercept(state);
        const kept = (globalThis as GlobalRecord).WebSocket;
        setGlobal("junk" as unknown);
        assert.equal((globalThis as GlobalRecord).WebSocket, kept);
        const ws = new (kept as new (...args: unknown[]) => FakeWs)(UPSTREAM_WS, {});
        assert.equal(ws.url, REWRITTEN);
    });
});

// ——— DSH request lifecycle (#5838) ———————————————————————————————————————————

for (const verdict of [false, "refuse-oversized", "refuse-oversized-unattributed"] as const) {
    test(`async takeover verdict is honored: ${verdict}`, async () => {
        await withPatchedGlobal(async () => {
            const state: NativeInterceptState = { origin: ORIGIN, ready: Promise.resolve(ORIGIN), takeoverGate: async () => verdict };
            installNativeWebSocketIntercept(state);
            const ws = new (WsCtor<FakeWs>())(UPSTREAM_WS, {});
            let errors = 0;
            let closes = 0;
            ws.addEventListener("error", () => errors++);
            ws.addEventListener("close", () => closes++);
            await tick();
            await tick();
            if (verdict === false) {
                assert.equal(FakeWs.instances[0]?.url, UPSTREAM_WS);
                assert.equal(errors, 0);
            } else {
                assert.equal(FakeWs.instances.length, 0, "refusal must not construct a direct socket");
                assert.equal(errors, 1);
                assert.equal(closes, 1);
                assert.equal(ws.readyState, FakeWs.CLOSED);
            }
        });
    });
}

test("DSH waits for tool registration and merges authoritative upgrade headers without mutating arguments", async () => {
    await withPatchedGlobal(async () => {
        let ready = false;
        let release!: () => void;
        const toolsReady = new Promise<void>(resolve => { release = () => { ready = true; resolve(); }; });
        const state: NativeInterceptState = {
            origin: ORIGIN, ready: Promise.resolve(ORIGIN), toolsReady,
            headersFor: () => ready ? { "x-bili-plugin": "dsh", "x-bili-plugin-conversation": "test-session" } : undefined,
        };
        installNativeWebSocketIntercept(state);
        const options = { headers: { Authorization: "test-only", "x-bili-plugin-conversation": "stale" } };
        new (WsCtor<FakeWs>())(UPSTREAM_WS, ["responses"], options);
        await tick();
        assert.equal(FakeWs.instances.length, 0);
        release();
        await tick();
        const [real] = FakeWs.instances;
        assert.equal(real.url, REWRITTEN);
        assert.deepEqual(real.ctorArgs[1], ["responses"]);
        const headers = new Headers((real.ctorArgs[2] as { headers: Record<string, string> }).headers);
        assert.equal(headers.get("authorization"), "test-only");
        assert.equal(headers.get("x-bili-plugin"), "dsh");
        assert.equal(headers.get("x-bili-plugin-conversation"), "test-session");
        assert.equal(options.headers["x-bili-plugin-conversation"], "stale");
    });
});

test("DSH prepares every response.create in sender scope, preserves continuation, and refuses model changes", async () => {
    await withPatchedGlobal(async () => {
        const scope = new AsyncLocalStorage<string>();
        const seen: string[] = [];
        let model = "model-a";
        let release!: () => void;
        const adoption = new Promise<void>(resolve => { release = resolve; });
        const state: NativeInterceptState = {
            origin: ORIGIN, ready: Promise.resolve(ORIGIN), takeoverGate: async () => true,
            headersFor: () => ({ "x-bili-plugin": "dsh", "x-bili-model": model }),
            beforeSend: async (_url, init) => {
                seen.push(scope.getStore() ?? "missing");
                assert.equal(JSON.parse(String(init?.body)).type, "response.create");
                await adoption;
            },
        };
        installNativeWebSocketIntercept(state);
        const ws = new (WsCtor<FakeWs>())(UPSTREAM_WS, {});
        await tick();
        const [real] = FakeWs.instances;
        real.readyState = FakeWs.OPEN;
        const first = JSON.stringify({ type: "response.create", model, input: [] });
        scope.run("first-dispatch", () => ws.send(first));
        await tick();
        assert.equal(real.sent.length, 0, "first frame must await fork preparation");
        release();
        await tick();
        const next = JSON.stringify({ type: "response.create", model, previous_response_id: "test-response", input: [] });
        scope.run("second-dispatch", () => ws.send(next));
        await tick();
        assert.deepEqual(seen, ["first-dispatch", "second-dispatch"]);
        assert.deepEqual(real.sent, [first, next], "do not expand or strip cached continuation on the client");
        model = "model-b";
        ws.send(next);
        await tick();
        assert.equal(real.sent.length, 2);
        assert.equal(real.closed, true, "immutable upgrade metadata requires reconnect");
    });
});

for (const change of ["state", "origin", "headers", "gate"] as const) {
    test(`DSH cached connection refuses changed ${change}`, async () => {
        await withPatchedGlobal(async () => {
            let refuse = false;
            let mutate = false;
            const state: NativeInterceptState = {
                origin: ORIGIN, ready: Promise.resolve(ORIGIN),
                headersFor: () => ({ "x-bili-plugin": "dsh" }),
                takeoverGate: async () => !refuse,
                beforeSend: async (_url, init) => { if (mutate && init) init.headers = { "x-bili-plugin": "dsh", "x-bili-plugin-conversation": "changed" }; },
            };
            installNativeWebSocketIntercept(state);
            const ws = new (WsCtor<FakeWs>())(UPSTREAM_WS, {});
            await tick();
            const [real] = FakeWs.instances;
            real.readyState = FakeWs.OPEN;
            if (change === "state") installNativeWebSocketIntercept({ ...state });
            if (change === "origin") state.origin = "http://127.0.0.1:9999";
            if (change === "headers") mutate = true;
            if (change === "gate") refuse = true;
            ws.send(JSON.stringify({ type: "response.create", input: [] }));
            await tick();
            assert.equal(real.sent.length, 0);
            assert.equal(real.closed, true);
        });
    });
}

for (const change of ["cached headers", "in-place preparation"] as const) {
    test(`upgrade identity is immutable against ${change}`, async () => {
        await withPatchedGlobal(async () => {
            const identity = { "x-bili-plugin": "dsh", "x-bili-model": "model-a" };
            const state: NativeInterceptState = {
                origin: ORIGIN, ready: Promise.resolve(ORIGIN), headersFor: () => identity,
                beforeSend: async (_url, init) => {
                    if (change === "in-place preparation" && init?.headers) {
                        const headers = init.headers as Record<string, string>;
                        headers["x-bili-model"] = "model-b";
                    }
                },
            };
            installNativeWebSocketIntercept(state);
            const ws = new (WsCtor<FakeWs>())(UPSTREAM_WS, {});
            await tick();
            const [real] = FakeWs.instances;
            real.readyState = FakeWs.OPEN;
            if (change === "cached headers") identity["x-bili-model"] = "model-b";
            ws.send(JSON.stringify({ type: "response.create", input: [] }));
            await tick();
            assert.equal(real.sent.length, 0);
            assert.equal(real.closed, true);
            if (change === "in-place preparation") assert.equal(identity["x-bili-model"], "model-a");
        });
    });
}

test("pending upgrade cannot adopt a later activation of the same state", async () => {
    await withPatchedGlobal(async () => {
        let release!: () => void;
        const toolsReady = new Promise<void>(resolve => { release = resolve; });
        const state: NativeInterceptState = { origin: ORIGIN, ready: Promise.resolve(ORIGIN), toolsReady, headersFor: () => ({ "x-bili-plugin": "dsh" }), beforeSend: async () => undefined };
        installNativeWebSocketIntercept(state);
        const ws = new (WsCtor<FakeWs>())(UPSTREAM_WS, {});
        let closes = 0;
        ws.addEventListener("close", () => closes++);
        await tick();
        installNativeWebSocketIntercept(state);
        release();
        await tick();
        assert.equal(FakeWs.instances.length, 0);
        assert.equal(closes, 1);
    });
});

test("origin changes during asynchronous frame preparation refuse the held frame", async () => {
    await withPatchedGlobal(async () => {
        let release!: () => void;
        const hold = new Promise<void>(resolve => { release = resolve; });
        const state: NativeInterceptState = { origin: ORIGIN, ready: Promise.resolve(ORIGIN), beforeSend: async () => hold };
        installNativeWebSocketIntercept(state);
        const ws = new (WsCtor<FakeWs>())(UPSTREAM_WS, {});
        await tick();
        const [real] = FakeWs.instances;
        real.readyState = FakeWs.OPEN;
        ws.send(JSON.stringify({ type: "response.create", input: [] }));
        await tick();
        state.origin = "http://127.0.0.1:9999";
        release();
        await tick();
        assert.equal(real.sent.length, 0);
        assert.equal(real.closed, true);
    });
});

test("rejected asynchronous gate does not construct a direct fallback", async () => {
    await withPatchedGlobal(async () => {
        installNativeWebSocketIntercept({ origin: ORIGIN, ready: Promise.resolve(ORIGIN), takeoverGate: async () => { throw new Error("test gate failure"); } });
        const ws = new (WsCtor<FakeWs>())(UPSTREAM_WS, {});
        let errors = 0;
        ws.addEventListener("error", () => errors++);
        await tick();
        assert.equal(FakeWs.instances.length, 0);
        assert.equal(errors, 1);
        assert.equal(ws.readyState, FakeWs.CLOSED);
    });
});

test("rejected asynchronous preparation fails closed and cancelled preparation emits no duplicate close", async () => {
    await withPatchedGlobal(async () => {
        let reject!: (reason: Error) => void;
        const gate = new Promise<boolean>((_resolve, no) => { reject = no; });
        installNativeWebSocketIntercept({ origin: ORIGIN, ready: Promise.resolve(ORIGIN), takeoverGate: () => gate });
        const ws = new (WsCtor<FakeWs>())(UPSTREAM_WS, {});
        let errors = 0;
        let closes = 0;
        ws.addEventListener("error", () => errors++);
        ws.addEventListener("close", () => closes++);
        ws.close();
        reject(new Error("test-only gate rejection"));
        await tick();
        assert.equal(FakeWs.instances.length, 0);
        assert.equal(errors, 0);
        assert.equal(closes, 1);
    });
});

// ——— Reset —————————————————————————————————————————————————————————————————————

test("_resetForTest restores the pre-install global and clears the flag", async () => {
    const saved = savedDescriptor();
    _resetForTest();
    if (saved !== undefined) Object.defineProperty(globalThis, "WebSocket", saved);
    setGlobal(FakeWs);
    const state: NativeInterceptState = { origin: ORIGIN, ready: Promise.resolve(ORIGIN) };
    installNativeWebSocketIntercept(state);
    _resetForTest();
    assert.equal(nativeWebSocketInterceptInstalled(), false);
    assert.equal((globalThis as GlobalRecord).WebSocket, FakeWs);
    if (saved !== undefined) Object.defineProperty(globalThis, "WebSocket", saved);
});
