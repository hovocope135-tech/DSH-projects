window.__ModuleLoader__.load({
	id: "dsh-mira-projects",
	factory: (require) => {
		var module = { exports: {} };
		var exports = module.exports;
		Object.defineProperty(exports, Symbol.toStringTag, { value: "Module" });
		const React = require("react");
		const P = require("@deepseek-ai/dsh-client-ui-primitives");
		const h = React.createElement;
		const { useState, useEffect, useLayoutEffect, useSyncExternalStore, useRef, useMemo, useCallback } = React;

		// ------------------------------------------------------------------ copy
		// Status colors follow the Raft design system: cyan = in progress,
		// pink = needs you, lavender = in review, lime = done, orange = warning,
		// stone = closed. They never change meaning and never blink.

		const NS = "miraProjects";
		const PANEL_ID = "mira-projects";
		const HOME_ID = "mira-personal";
		const zh = { panel: "项目" };
		const en = { panel: "Projects" };
		const STATE = {
			working: { label: "进行中", tone: "cyan" },
			blocked: { label: "等你", tone: "pink" },
			review_ready: { label: "待验收", tone: "lav" },
			done: { label: "完成", tone: "lime" },
			failed: { label: "失败", tone: "orange" },
			archived: { label: "已归档", tone: "stone" },
		};
		const KIND = {
			next_step: { label: "下一步", tone: "pink" },
			reminder: { label: "提醒", tone: "orange" },
			open_loop: { label: "没收尾", tone: "orange" },
			recap: { label: "回顾", tone: "cyan" },
			capture: { label: "记一下", tone: "lav" },
			morning: { label: "早安回顾", tone: "yellow" },
		};
		const MEMORY = [
			["now", "现在在忙什么"],
			["decisions", "做过的决定"],
			["openLoops", "没收尾的事"],
			["facts", "关于你和项目"],
		];
		const ACTIVITY = {
			inbox_read: "在读收件箱", post_message: "在写回复", update_card: "在更新卡片", request_decision: "在准备拍板卡",
			open_thread: "在开线程", list_threads: "在看线程", read_thread: "在看线程", message_thread: "在转达",
			message_coordinator: "在转达给项目助理", react: "在回应", memory_read: "在读记忆", memory_write: "在写记忆",
			card_propose: "在准备卡片", card_withdraw: "在撤卡", withdraw_decision: "在作废拍板", no_reply: "刚结束一轮",
		};
		const CARD_STATE = { open: "待处理", acted: "已处理", dismissed: "已忽略", superseded: "已替换", withdrawn: "已撤回", expired: "已过期", stale: "已过时" };
		// The daily recap is "morning" only in the morning.
		const recapLabel = (at) => { const hour = new Date(at || Date.now()).getHours(); return hour < 11 ? "早安回顾" : hour < 18 ? "今日回顾" : "晚间回顾"; };

		// ------------------------------------------------------------------ transport

		const STATE_URL = "api/mira-projects/state";
		const ACTION_URL = "api/mira-projects/action";
		const DOCK_URL = "api/mira-projects/dock";
		const FILE_URL = "api/mira-projects/file";
		const fileURL = (projectId, fileId, revision) => FILE_URL + "?" + new URLSearchParams({ projectId, fileId, ...(revision ? { revision: String(revision) } : {}) });

		async function act(op, body) {
			const res = await fetch(ACTION_URL, {
				method: "POST",
				credentials: "same-origin",
				headers: { "content-type": "application/json" },
				body: JSON.stringify({ op, ...body }),
			});
			const data = await res.json().catch(() => ({}));
			if (!res.ok || data.error) throw new Error(data.error || `HTTP ${res.status}`);
			return data;
		}

		/** One revision stream per origin, shared without sending source content between tabs. */
		function createRevisionFeed({ fetcher, locks, Channel, channelName = "mira-projects:revision", fallbackMs = 3000, retryMs = 2000 }) {
			let state = { rev: -1, error: null }, active = null;
			const listeners = new Set();
			const update = (rev, error = null) => { if (state.rev !== rev || state.error !== error) { state = { rev, error };for (const listener of listeners) listener(); } };
			const delay = (ms, signal) => new Promise(resolve => {
				const done = () => { clearTimeout(timer);signal.removeEventListener("abort", done);resolve(); }, timer = setTimeout(done, ms);
				signal.addEventListener("abort", done, { once: true });if (signal.aborted) done();
			});
			const stop = () => { const current = active;active = null;if (current) { current.abort.abort();current.channel?.close(); } };
			const start = () => {
				const current = { abort: new AbortController(), leader: false, coordinated: false, channel: null };active = current;
				const signal = current.abort.signal;
				try {
					if (locks?.request && Channel) {
						current.channel = new Channel(channelName);current.coordinated = true;
						current.channel.onmessage = event => {
							const revision = event.data?.rev;
							if (active === current && Number.isSafeInteger(revision) && revision >= 0) update(revision);
						};
					}
				} catch { current.channel?.close();current.channel = null; }
				const read = async wait => {
					const response = await fetcher(`api/mira-projects/revision?since=${state.rev}&wait=${wait ? 1 : 0}`, { credentials: "same-origin", cache: "no-store", signal });
					if (!response.ok) throw Error(`HTTP ${response.status}`);
					const body = await response.json();if (!Number.isSafeInteger(body.rev) || body.rev < 0) throw Error("更新通知格式无效");
					if (active !== current || signal.aborted) return;
					update(body.rev);
					try { current.channel?.postMessage({ rev: body.rev }); } catch { current.channel?.close();current.channel = null;current.coordinated = false; }
				};
				const loop = async longPoll => {
					while (active === current && !signal.aborted) {
						try {
							if (longPoll || !current.leader) await read(longPoll && state.rev >= 0);
							await delay(longPoll ? 50 : current.coordinated ? fallbackMs : 1000, signal);
						} catch (error) {
							if (signal.aborted || active !== current) return;
							update(state.rev, error.message);await delay(retryMs, signal);
						}
					}
				};
				if (current.coordinated) {
					try {
						Promise.resolve(locks.request(channelName, { mode: "exclusive", signal }, async () => {
							if (active !== current || signal.aborted) return;
							current.leader = true;try { await loop(true); } finally { current.leader = false; }
						})).catch(() => { if (active === current && !signal.aborted) current.coordinated = false; });
					} catch { current.coordinated = false; }
				}
				// Followers also do a short read periodically: a suspended leader or
				// unavailable browser coordination must never freeze the other tabs.
				void loop(false);
			};
			return {
				getSnapshot: () => state,
				subscribe: listener => { listeners.add(listener);if (!active) start();return () => { listeners.delete(listener);if (!listeners.size) stop(); }; },
				dispose: () => { stop();listeners.clear(); },
			};
		}
		let revisionFeed;
		function revisions() {
			return revisionFeed ||= createRevisionFeed({ fetcher: (...args) => fetch(...args), locks: window.navigator?.locks, Channel: window.BroadcastChannel,
				channelName: "mira-projects:revision:" + new URL("api/mira-projects/revision", document.baseURI).pathname });
		}
		function useServerSnapshot(url, initial) {
			const [state, setState] = useState(initial), feed = revisions(), revision = useSyncExternalStore(feed.subscribe, feed.getSnapshot, feed.getSnapshot);
			useEffect(() => {
				const abort = new AbortController();let alive = true;
				const pause = ms => new Promise(resolve => {
					const done = () => { clearTimeout(timer);abort.signal.removeEventListener("abort", done);resolve(); }, timer = setTimeout(done, ms);
					abort.signal.addEventListener("abort", done, { once: true });if (abort.signal.aborted) done();
				});
				(async () => { while (alive) {
					try {
						const response = await fetch(url, { credentials: "same-origin", cache: "no-store", signal: abort.signal }), body = await response.json();
						if (!response.ok || body.error) throw Error(body.error || `HTTP ${response.status}`);
						if (alive) setState({ ...body, loaded: true, error: null });
						// Child execution and device presence can change without a
						// domain event. Preserve periodic refresh using short reads.
						await pause(20000);
					} catch (error) { if (!alive) return;setState(old => ({ ...old, loaded: true, error: error.message }));await pause(2000); }
				} })();
				return () => { alive = false;abort.abort(); };
			}, [url, revision.rev]);
			return revision.error && !state.error ? { ...state, error: revision.error } : state;
		}
		/**
		 * One polling loop per URL for the whole page, however many components read it. v4 renders the same task in
		 * several places at once (left column, right-panel tabs, the goal strip, cards inside the conversation).
		 */
		const sharedFeeds = new Map();
		function sharedFeed(url, initial) {
			let feed = sharedFeeds.get(url);
			if (feed) return feed;
			const listeners = new Set(), revision = revisions();
			let state = initial, timer = null, abort = null, users = 0, seenRev = null, offRevision = null;
			const emit = () => { for (const listener of listeners) listener(); };
			const load = async () => {
				abort?.abort(); const controller = abort = new AbortController();
				try {
					const response = await fetch(url, { credentials: "same-origin", cache: "no-store", signal: controller.signal }), body = await response.json();
					if (!response.ok || body.error) throw Error(body.error || `HTTP ${response.status}`);
					state = { ...body, loaded: true, error: null };
				} catch (error) { if (controller.signal.aborted) return; state = { ...state, loaded: true, error: error.message }; }
				emit();
			};
			feed = {
				subscribe(listener) {
					listeners.add(listener);
					if (users++ === 0) {
						load(); timer = setInterval(load, 20000);
						offRevision = revision.subscribe(() => { const rev = revision.getSnapshot().rev; if (rev !== seenRev) { seenRev = rev; load(); } });
					}
					return () => {
						listeners.delete(listener);
						if (--users === 0) { clearInterval(timer); offRevision?.(); abort?.abort(); if (sharedFeeds.get(url) === feed) sharedFeeds.delete(url); }
					};
				},
				getSnapshot: () => state,
			};
			sharedFeeds.set(url, feed);
			return feed;
		}
		function useShared(url, initial) {
			const feed = sharedFeed(url, initial);
			return useSyncExternalStore(feed.subscribe, feed.getSnapshot, feed.getSnapshot);
		}
		const EMPTY_STATE = { rev: -1, projects: [], project: null, dock: null, loaded: false, error: null };
		function useSnapshot(projectId) {
			return useShared(STATE_URL + "?" + new URLSearchParams({ project: projectId || "" }), EMPTY_STATE);
		}

		function readPref(key) {
			try { return window.localStorage.getItem(`mira-projects:${key}`) || undefined; } catch (_e) { return undefined; }
		}
		function writePref(key, value) {
			try { window.localStorage.setItem(`mira-projects:${key}`, value); } catch (_e) { /* storage unavailable */ }
		}

		/** Hidden panes keep their drafts and reading position, including while messages arrive. */
		function useReadingPosition(visible, identity, revision, startAtBottom = true) {
			const ref = useRef(null), position = useRef(0), stick = useRef(startAtBottom);
			useLayoutEffect(() => { position.current = 0; stick.current = startAtBottom; }, [identity]);
			useLayoutEffect(() => {
				const el = ref.current;
				if (visible && el) el.scrollTop = stick.current ? el.scrollHeight : position.current;
			}, [visible, identity, revision]);
			const onScroll = e => {
				const el = e.currentTarget;
				if (!visible || el.clientHeight === 0) return;
				position.current = el.scrollTop;
				stick.current = el.scrollHeight - el.scrollTop - el.clientHeight < 80;
			};
			return { ref, onScroll };
		}

		function useNarrow(px) {
			const query = `(max-width: ${px}px)`;
			const [narrow, setNarrow] = useState(() => (window.matchMedia ? window.matchMedia(query).matches : false));
			useEffect(() => {
				if (!window.matchMedia) return undefined;
				const mq = window.matchMedia(query);
				const on = (e) => setNarrow(e.matches);
				mq.addEventListener ? mq.addEventListener("change", on) : mq.addListener(on);
				return () => { mq.removeEventListener ? mq.removeEventListener("change", on) : mq.removeListener(on); };
			}, [query]);
			return narrow;
		}

		// ------------------------------------------------------------------ time

		function clock(at) {
			const d = new Date(at);
			return `${String(d.getHours()).padStart(2, "0")}:${String(d.getMinutes()).padStart(2, "0")}`;
		}
		function sameDay(a, b) { return new Date(a).toDateString() === new Date(b).toDateString(); }
		function dayLabel(at) {
			const now = Date.now();
			if (sameDay(at, now)) return "今天";
			if (sameDay(at, now - 86400e3)) return "昨天";
			const d = new Date(at);
			return `${d.getMonth() + 1}月${d.getDate()}日`;
		}
		function ago(at) {
			if (!at) return "";
			const diff = Date.now() - at;
			if (diff < 45e3) return "刚刚";
			if (diff < 3600e3) return `${Math.max(1, Math.round(diff / 60e3))} 分钟前`;
			if (sameDay(at, Date.now())) return clock(at);
			if (sameDay(at, Date.now() - 86400e3)) return `昨天 ${clock(at)}`;
			return dayLabel(at);
		}

		/** Presence light: busy while a turn runs, orange after a failed wake, lime when the session exists, stone otherwise. */
		function presenceOf(status, lastTurn, sessionId) {
			if (status === "running" || status === "queued") return "busy";
			if (lastTurn && lastTurn.failed) return "error";
			return sessionId ? "online" : "offline";
		}
		function activityText(status, activity, lastTurn) {
			if (status === "queued") return "排队中";
			if (status === "running") return (activity && ACTIVITY[activity.tool]) || "在想";
			if (lastTurn && lastTurn.failed) return "上次唤醒失败";
			if (!lastTurn) return "";
			const when = ago(lastTurn.at);
			return /^\d/.test(when) || /^昨天/.test(when) ? `${when} 更新` : `${when}更新`;
		}


		// ------------------------------------------------------------------ identity
		// An agent is drawn as the model it runs on: one 14 × 14 pixel slime per model
		// family, the family picks the colour (DeepSeek blue, Claude terracotta, Kimi
		// near-black...). Mira alone looks different: yellow, eyes closed, cheeks and a
		// sprout. People keep their own avatar.

		const FAMILIES = [
			{ id: "claude", label: "Claude", test: /claude|anthropic|opus|sonnet|haiku/i, fill: "#E07A55" },
			{ id: "kimi", label: "Kimi", test: /kimi|moonshot/i, fill: "#2E2E36", light: true },
			{ id: "deepseek", label: "DeepSeek", test: /deepseek/i, fill: "#4D6BFE", light: true },
			{ id: "gpt", label: "GPT", test: /gpt|openai|(^|[/:-])o[134](-|$)/i, fill: "#10A37F" },
			{ id: "gemini", label: "Gemini", test: /gemini|gemma/i, fill: "#9B7BF2" },
			{ id: "qwen", label: "Qwen", test: /qwen|tongyi/i, fill: "#EC5FA5" },
			{ id: "glm", label: "GLM", test: /glm|zhipu|chatglm/i, fill: "#F5C342" },
			{ id: "doubao", label: "豆包", test: /doubao|seed|bytedance/i, fill: "#F0564A" },
			{ id: "minimax", label: "MiniMax", test: /minimax|abab/i, fill: "#F08A3C" },
			{ id: "llama", label: "Llama", test: /llama|meta-/i, fill: "#3D7BF2", light: true },
			{ id: "mistral", label: "Mistral", test: /mistral|mixtral/i, fill: "#FA8A2E" },
			{ id: "scripted", label: "脚本模型", test: /scripted/i, fill: "#A8A29E" },
		];
		const UNKNOWN_FAMILY = { id: "default", label: "默认模型", fill: "#B9B2AA" };

		/** The family and display text of a model id; unknown ids keep a neutral stone circle. */
		function modelInfo(model) {
			const id = typeof model === "string" ? model : model && (model.model || model.id);
			if (!id) return { ...UNKNOWN_FAMILY, model: null, short: "默认模型" };
			const fam = FAMILIES.find((f) => f.test.test(id)) || { ...UNKNOWN_FAMILY, label: id };
			const short = id.split("/").pop();
			return { ...fam, model: id, short };
		}

		const PX_BODY = [
			"..............",
			".....XXXX.....",
			"...XXXXXXXX...",
			"..XXXXXXXXXX..",
			".XXXXXXXXXXXX.",
			".XXXXXXXXXXXX.",
			"XXXXXXXXXXXXXX",
			"XXXXXXXXXXXXXX",
			"XXXXXXXXXXXXXX",
			"XXXXXXXXXXXXXX",
			"XXXXXXXXXXXXXX",
			"XXXXXXXXXXXXXX",
			".XXXXXXXXXXXX.",
			"..............",
		];
		const PX_INK = "#202020";
		/** A colour moved toward black (k < 0) or white (k > 0). */
		function pxShade(hex, k) {
			const n = parseInt(hex.slice(1), 16), t = k < 0 ? 0 : 255, a = Math.abs(k);
			return "#" + [n >> 16, (n >> 8) & 255, n & 255].map(v => Math.round(v + (t - v) * a).toString(16).padStart(2, "0")).join("");
		}

		const AV_SIZE = { "2xs": 19, xs: 19, sm: 27, md: 35, lg: 51 };
		const PRESENCE_LABEL = { online: "在线", busy: "在工作", error: "出错了", offline: "离线", external: "外部" };

		/**
		 * One avatar per member, everywhere. `author` is a member or a message author;
		 * `project` lets it find the member's role and model.
		 */
		function Avatar({ author, size = "md", presence, project, title }) {
			const px = AV_SIZE[size] || size;
			const member = project && author ? project.members.find((m) => m.id === author.id) : null;
			const human = author && author.kind === "human";
			const light = presence ? h("i", { className: `mp-light mp-light-${presence}`, role: "img", "aria-label": PRESENCE_LABEL[presence] }) : null;
			if (human) {
				return h("span", { className: "mp-av mp-av-human", "data-size": size, style: { width: px, height: px, fontSize: px >= 35 ? 14 : 11 }, "aria-hidden": !presence, title },
					h("svg", { viewBox: "0 0 24 24", width: Math.round(px * 0.5), height: Math.round(px * 0.5), fill: "none", stroke: "currentColor", strokeWidth: 2, strokeLinecap: "round", strokeLinejoin: "round", "aria-hidden": true },
						h("path", { d: "M18 20a6 6 0 0 0-12 0" }), h("circle", { cx: 12, cy: 10, r: 4 })), light);
			}
			const role = (member && member.role) || (author && author.role) || (author && author.id === "mira" ? "memory" : "worker");
			const faceRole = role === "worker" ? (author && author.id) : role;
			const info = modelInfo((member && member.model) || (author && author.model)), mira = faceRole === "memory";
			return h("span", { className: "mp-av", "data-size": size, style: { width: px, height: px }, title: title || `${(author && author.name) || ""}，${info.model || "默认模型"}` },
				h(LetterAvatar, { name: mira ? "记" : (member && member.name) || (author && author.name), tint: mira ? "#8e4ec6" : info.fill, size: px }),
				light);
		}

		/** The model after the agent's name, as the product writes "Claude Opus": caption, hint color. */
		function ModelTag({ member, mono }) {
			if (!member || member.kind !== "assistant") return null;
			const info = modelInfo(member.model);
			const source = member.model?.source === "default" ? "（DSH 默认模型，还没跑过）" : member.model?.source === "member" ? "（成员配置，下一次唤醒生效）" : "（最近请求实测）";
			const configured = member.modelSelection && member.modelSelection.model !== member.model?.model ? `；下一次唤醒配置：${member.modelSelection.provider} / ${member.modelSelection.model}` : "";
			return h("span", { className: "mp-model" + (mono ? " mp-mono" : ""), title: info.model ? `${member.model.provider ? `${member.model.provider} / ` : ""}${info.model}${source}${configured}` : "跟随 DSH 的默认模型" }, info.model ? info.short : "默认模型");
		}

		// ------------------------------------------------------------------ atoms

		// Interface icons in the host's line style (24 × 24, 2 px stroke), so ours sit beside DSH's without a seam.
		const ICONS = {
			close: "M6 6l12 12M18 6L6 18",
			menu: "M4 5h16v14H4zM9 5v14",
			plus: "M12 5v14M5 12h14",
			popout: "M14 4h6v6M20 4l-9 9M19 14v5a1 1 0 0 1-1 1H5a1 1 0 0 1-1-1V6a1 1 0 0 1 1-1h5",
			session: "M4 6h16M4 12h10M4 18h7",
			check: "M5 12l4 4L19 6",
			chevron: "M9 6l6 6-6 6",
			spark: "M12 3l1.8 5.2L19 10l-5.2 1.8L12 17l-1.8-5.2L5 10l5.2-1.8z",
			back: "M15 6l-6 6 6 6",
			dock: "M4 5h16v14H4zM4 15h16",
			hash: "M9 4L7 20M17 4l-2 16M4 9h16M3 15h16",
			send: "M5 12h13M13 6l6 6-6 6",
			reply: "M21 15a4 4 0 0 1-4 4H8l-5 3V7a4 4 0 0 1 4-4h10a4 4 0 0 1 4 4z",
			decide: "M9 11l3 3 8-8M20 12v6a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h9",
			tasks: "M9 6h11M9 12h11M9 18h11M3 6l1 1 2-2M3 12l1 1 2-2M3 18l1 1 2-2",
			panel: "M4 5h16v14H4zM15 5v14",
			widen: "M4 12h16M8 8l-4 4 4 4M16 8l4 4-4 4",
			more: "M5 12h.01M12 12h.01M19 12h.01",
			note: "M6 3h9l3 3v15H6zM9 10h6M9 14h6M9 18h4",
			inbox: "M4 13l3-8h10l3 8v6H4zM4 13h5l1 2h4l1-2h5",
			clock: "M12 21a9 9 0 1 0 0-18 9 9 0 0 0 0 18zM12 7v5l3 2",
			tree: "M6 4v16M6 8h6M6 14h6M12 8h6M12 14h6",
			diff: "M12 4v8M8 8h8M8 18h8",
			agents: "M8 11a3 3 0 1 0 0-6 3 3 0 0 0 0 6zM16 11a3 3 0 1 0 0-6 3 3 0 0 0 0 6zM3 20c0-3 2.5-5 5-5s5 2 5 5M13 15.5c.9-.3 1.9-.5 3-.5 2.5 0 5 2 5 5",
			quote: "M7 7h4v4c0 3-1.5 5-4 6M14 7h4v4c0 3-1.5 5-4 6",
			folder: "M3 6h6l2 2h10v11H3z",
			download: "M12 4v11M7 10l5 5 5-5M5 20h14",
			sliders: "M21 4h-7M10 4H3M21 12h-9M8 12H3M21 20h-5M12 20H3M14 2v4M8 10v4M16 18v4",
			moon: "M12 3a6 6 0 0 0 9 9 9 9 0 1 1-9-9Z",
			enter: "M20 4v7a4 4 0 0 1-4 4H4M9 10l-5 5 5 5",
			monitor: "M3 4h18v12H3zM8 20h8M12 16v4",
		};
		function Icon({ name, size = 16 }) {
			return h("svg", { width: size, height: size, viewBox: "0 0 24 24", fill: "none", stroke: "currentColor", strokeWidth: 2, strokeLinecap: "round", strokeLinejoin: "round", "aria-hidden": true },
				h("path", { d: ICONS[name] || "" }));
		}
		function IconButton({ name, label, onClick, active }) {
			return h("button", { type: "button", className: "mp-ib" + (active ? " mp-ib-on" : ""), title: label, "aria-label": label, onClick }, h(Icon, { name }));
		}
		/** Raft Button: outline by default, ghost when `quiet`. `strong` marks the recommended choice; the one pink button is Send. */
		function Btn({ children, onClick, strong, quiet, disabled, title, className, ...attributes }) {
			return h("button", { ...attributes, type: "button", className: "mp-btn" + (strong ? " mp-btn-strong" : "") + (quiet ? " mp-btn-quiet" : "") + (className ? ` ${className}` : ""), onClick, disabled, title }, children);
		}

		/** Raft Status: a still square light with an ink edge. */
		function Light({ tone, size }) {
			return h("i", { className: `mp-status mp-status-${tone || "stone"}` + (size ? ` mp-status-${size}` : ""), "aria-hidden": true });
		}
		/** Raft TaskCardStatus: soft tint chip, square light, label. */
		function Chip({ tone, children, dot = true }) {
			return h("span", { className: `mp-chip mp-tone-${tone || "stone"}` }, dot ? h(Light, { tone }) : null, children);
		}
		function StateChip({ state, label }) {
			const s = STATE[state] || { label: state, tone: "stone" };
			return h(Chip, { tone: s.tone }, label || s.label);
		}

		const MARKDOWN_LABELS = { code: { copyLabel: "复制", copiedLabel: "已复制", toolbarLabels: { codeLabel: "代码", wrapLabel: "自动换行", unwrapLabel: "取消换行" } }, footnotes: "脚注" };
		let mentionHighlightId = 0;
		/** Use DSH's own parser, code toolbar and link policy. Highlight mentions without changing its DOM. */
		function RichText({ text, members }) {
			const root = useRef(null);
			const namesKey = JSON.stringify((members || []).filter(m => m.kind === "assistant").map(m => m.name).concat(["我", "用户"]).sort((a, b) => b.length - a.length));
			const highlightId = useRef(null);
			if (!highlightId.current) highlightId.current = `mp-mention-${++mentionHighlightId}`;
			useEffect(() => {
				if (!window.CSS?.highlights || !window.Highlight || !root.current) return;
				const names = JSON.parse(namesKey);
				const pattern = new RegExp(`@(?:${names.map(n => n.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")).join("|")})`, "g");
				const walker = document.createTreeWalker(root.current, NodeFilter.SHOW_TEXT);
				const ranges = [];
				for (let node; (node = walker.nextNode());) {
					if (node.parentElement.closest("pre,code,a")) continue;
					for (const match of node.textContent.matchAll(pattern)) {
						const range = new Range(); range.setStart(node, match.index); range.setEnd(node, match.index + match[0].length); ranges.push(range);
					}
				}
				const id = highlightId.current;
				window.CSS.highlights.set(id, new window.Highlight(...ranges));
				const style = document.createElement("style"); style.textContent = `::highlight(${id}){color:var(--dsw-alias-state-business-primary,#4176e6)}`; document.head.appendChild(style);
				return () => { window.CSS.highlights.delete(id); style.remove(); };
			}, [text, namesKey]);
			return h("div", { className: "mp-markdown", ref: root }, h(P.MarkdownText, { text: String(text || ""), labels: MARKDOWN_LABELS }));
		}

		function Progress({ checklist }) {
			const total = checklist.length;
			const done = checklist.filter((c) => c.done).length;
			if (!total) return null;
			return h("span", { className: "mp-mono mp-hint", title: `清单 ${done}/${total}` }, `${done}/${total}`);
		}

		function Faces({ people, project, size = "xs" }) {
			const shown = people.slice(0, 3);
			return h("span", { className: "mp-faces" },
				shown.map((p, i) => h(Avatar, { key: `${p.id}-${i}`, author: p, project, size })),
				people.length > 3 ? h("span", { className: "mp-faces-more" }, `+${people.length - 3}`) : null);
		}

		// ------------------------------------------------------------------ threads in the chat

		/** The task chip under a message, as the product shows "#2 @Koda": status light, number, assignee, on the status tint. */
		function TaskChip({ thread, project, onOpen }) {
			const s = STATE[thread.state === "done" ? "done" : thread.resolved ? "archived" : thread.state] || { label: thread.state, tone: "stone" };
			const index = project.threads.findIndex((t) => t.id === thread.id) + 1;
			return h("button", { type: "button", className: `mp-task-chip mp-tone-${s.tone}`, onClick: () => onOpen(thread.id), title: `${s.label}，${thread.title}` },
				thread.experience?.execution.active ? h("span", { className: "mp-spin mp-spin-sm", "aria-hidden": true }) : h(Light, { tone: s.tone, size: "sm" }), h("span", null, `#${index}`), h("span", null, executorOf(project, thread).name || thread.assigneeName));
		}

		/** Raft MessageReplies: "N replies" and the latest three, each with a small face, name, text and time. */
		function RepliesBlock({ thread, project, onOpen, active }) {
			const replies = thread.timeline.filter((r) => (r.kind || "message") === "message");
			if (!replies.length) return null;
			return h("button", { type: "button", className: "mp-replies" + (active ? " mp-replies-on" : ""), onClick: () => onOpen(thread.id), "aria-label": `打开线程：${thread.title}` },
				h("span", { className: "mp-replies-count" }, `${replies.length} 条回复`),
				replies.slice(-3).map((r) => h("span", { key: r.id, className: "mp-replies-row" },
					h(Avatar, { author: r.author, project, size: "2xs" }),
					h("b", null, shownName(project.members.find(m => m.id === r.author.id), r.author.name)),
					h("span", { className: "mp-replies-text" }, P.extractMarkdownPlainText(r.text)),
					h("time", { className: "mp-mono" }, clock(r.at)))));
		}

		/** Raft MessageActionCard: icon tile, the question, a status on the right, outline choices. */
		function DecisionCard({ decision, onAnswer, threadTitle, onOpenThread }) {
			const [busy, setBusy] = useState(false);
			const [error, setError] = useState(null);
			if (!decision) return null;
			const answered = decision.chosen !== null && decision.chosen !== undefined;
			const withdrawn = decision.withdrawn === true;
			const closed = answered || withdrawn;
			const pick = async (index) => {
				setBusy(true); setError(null);
				try { await onAnswer(decision.id, index); } catch (e) { setError(e.message); } finally { setBusy(false); }
			};
			return h("div", { className: "mp-action" + (closed ? " mp-action-closed" : "") },
				h("div", { className: "mp-action-head" },
					h("span", { className: "mp-action-icon" }, h(Icon, { name: "decide", size: 14 })),
					h("span", { className: "mp-action-title" },
						decision.question,
						threadTitle ? h("button", { type: "button", className: "mp-action-desc mp-linkish", onClick: onOpenThread }, `线程，${threadTitle}`) : null),
					h("span", { className: "mp-action-status" }, h(Light, { tone: closed ? "stone" : "pink" }), withdrawn ? "已作废" : answered ? "已拍板" : "等你拍板")),
				withdrawn ? h("div", { className: "mp-action-note" }, `不用选了：${decision.withdrawReason || "问题已经不成立"}`) : null,
				h("div", { className: "mp-action-body" }, decision.options.map((option, i) => {
					const chosen = answered && decision.chosen === i;
					if (closed) {
						return h("div", { key: i, className: "mp-opt-done" + (chosen ? " mp-opt-chosen" : "") },
							h("span", { className: "mp-opt-mark" }, chosen ? h(Icon, { name: "check", size: 14 }) : null),
							h("span", null, option.label),
							option.consequence ? h("span", { className: "mp-opt-note" }, option.consequence) : null);
					}
					return h("div", { key: i, className: "mp-opt" },
						h(Btn, { strong: i === decision.recommended, disabled: busy, onClick: () => pick(i) }, option.label, i === decision.recommended ? h("em", null, "推荐") : null),
						option.consequence ? h("span", { className: "mp-opt-note" }, option.consequence) : null);
				})),
				error ? h("div", { className: "mp-error" }, error) : null);
		}

		/** One of Mira's cards, in the same action-card shape: kind on the right, one sentence, outline actions, evidence in mono. */
		function MiraCard({ card, onJump, compact, showProject, project }) {
			const [busy, setBusy] = useState(false);
			const [error, setError] = useState(null);
			const kind = card.kind === "morning" ? { label: recapLabel(card.at), tone: "yellow" } : (KIND[card.kind] || { label: card.kind, tone: "stone" });
			const run = async (action) => {
				if (busy) return;
				setBusy(true); setError(null);
				try {
					if (action.id === "copy_text" && navigator.clipboard) await navigator.clipboard.writeText(action.text || card.body || card.title).catch(() => {});
					await act("cardAction", { projectId: card.projectId, cardId: card.id, actionId: action.id });
					if (action.id === "open_thread") onJump({ projectId: card.projectId, threadId: card.threadId });
					if (action.id === "reply") onJump({ projectId: card.projectId, threadId: card.threadClosed ? undefined : card.threadId, prefill: action.text || "" });
				} catch (e) { setError(e.message); } finally { setBusy(false); }
			};
			const open = card.state === "open";
			const where = [showProject && card.projectName ? card.projectName : null, card.threadTitle].filter(Boolean).join("，");
			const titleIsKind = card.title === kind.label;
			const mira = { kind: "assistant", id: "mira", name: "记忆助理", role: "memory" };
			return h("div", { className: "mp-action mp-mcard" + (compact ? " mp-action-compact" : "") + (open ? "" : " mp-action-closed") },
				h("div", { className: "mp-action-head" },
					h("span", { className: "mp-action-icon mp-action-icon-av" }, h(Avatar, { author: mira, project, size: "xs" })),
					h("span", { className: "mp-action-title" },
						titleIsKind ? kind.label : card.title,
						where ? h("span", { className: "mp-action-desc" }, where) : null),
					h("span", { className: "mp-action-status", title: card.closedNote || undefined },
						h(Light, { tone: open ? kind.tone : "stone" }), open ? kind.label : (CARD_STATE[card.state] || card.state))),
				card.body && (!compact || titleIsKind) ? h("div", { className: "mp-action-text" + (compact ? " mp-clamp" : "") }, card.body) : null,
				open ? h("div", { className: "mp-action-acts" },
					(card.actions || []).map((a) => h(Btn, { key: a.id, strong: a.id === "open_thread", disabled: busy, onClick: () => run(a), title: a.tier === "local-write" ? "会写进项目记忆" : undefined }, a.label)),
					h(Btn, { quiet: true, disabled: busy, onClick: () => run({ id: "dismiss" }) }, "忽略"),
					h("span", { className: "mp-mono mp-hint mp-right" }, ago(card.at))) : null,
				!compact && (card.evidence || []).length ? h("div", { className: "mp-action-why mp-mono" }, `依据 ${card.evidence.map((s) => `#${s}`).join(" ")}${card.confidence != null ? `，把握 ${Math.round(card.confidence * 100)}%` : ""}${!open && card.closedNote ? `，${card.closedNote}` : ""}`) : null,
				error ? h("div", { className: "mp-error" }, error) : null);
		}

		// ------------------------------------------------------------------ timeline

		/** Raft system message: centered Space Mono between two hairlines. */
		function SystemLine({ children, title }) {
			return h("div", { className: "mp-sys", title }, h("span", null, children));
		}
		function eventText(row) {
			if (row.kind === "relay") return row.toThread ? `${row.from} 转达：${row.text}` : `${row.from} → 项目助理：${row.text}`;
			return row.text;
		}
		function span(rows) {
			const ms = rows[rows.length - 1].at - rows[0].at;
			if (ms < 1000) return "";
			return ms < 60e3 ? `，用时 ${Math.round(ms / 1000)} 秒` : `，用时 ${Math.round(ms / 60e3)} 分钟`;
		}
		/** Two or more event lines in a row fold into one line that opens, as a coding agent folds its steps. */
		function EventFold({ rows }) {
			const [open, setOpen] = useState(false);
			if (rows.length === 1) return h(SystemLine, { title: eventText(rows[0]) }, `${eventText(rows[0])}，${clock(rows[0].at)}`);
			return h("div", { className: "mp-sys-group" + (open ? " mp-sys-open" : "") },
				h("div", { className: "mp-sys" }, h("button", { type: "button", className: "mp-sys-toggle", onClick: () => setOpen(!open), "aria-expanded": open },
					`执行了 ${rows.length} 步${span(rows)}，${clock(rows[rows.length - 1].at)}`, h("span", { className: "mp-sys-chev" }, h(Icon, { name: "chevron", size: 12 })))),
				open ? rows.map((r) => h(SystemLine, { key: r.id, title: eventText(r) }, `${eventText(r)}，${clock(r.at)}`)) : null);
		}

		/** Same sender, same day, under ten minutes: one header. A message carrying a thread or a decision starts its own row. */
		function groupRows(rows) {
			const out = [];
			let lastDay = null;
			let group = null;
			let events = null;
			for (const row of rows) {
				const day = new Date(row.at).toDateString();
				if (day !== lastDay) { out.push({ kind: "day", key: `day-${row.seq}`, label: dayLabel(row.at) }); lastDay = day; group = null; events = null; }
				const kind = row.kind || "message";
				if (kind === "message") {
					events = null;
					const carries = row.card && (row.card.kind === "thread" || row.card.kind === "decision");
					const same = group && !group.carries && !carries && group.author.kind === row.author.kind && group.author.id === row.author.id && row.at - group.last < 10 * 60e3;
					if (same) { group.messages.push(row); group.last = row.at; }
					else { group = { kind: "group", key: `g-${row.seq}`, author: row.author, at: row.at, last: row.at, messages: [row], carries }; out.push(group); }
				} else {
					group = null;
					if (events) events.rows.push(row);
					else { events = { kind: "events", key: `ev-${row.seq}`, rows: [row] }; out.push(events); }
				}
			}
			return out;
		}

		function FileLinks({ project, files }) {
			return files?.length ? h("div", { className: "mp-file-links" }, files.map(file => h("a", { key: file.fileId, href: fileURL(project.id, file.fileId), download: true }, file.path, `，v${file.latestRevision ?? file.revision}`))) : null;
		}

		function MessageBody({ message, project, onOpenThread, activeThreadId, onAnswer, showThreadLabel }) {
			const card = message.card;
			let body = null;
			if (card && card.kind === "thread") {
				const thread = project.threads.find((t) => t.id === card.threadId);
				body = thread ? h(React.Fragment, null,
					h("div", { className: "mp-chips" }, h(TaskChip, { thread, project, onOpen: onOpenThread })),
					h(RepliesBlock, { thread, project, onOpen: onOpenThread, active: activeThreadId === thread.id }),
					h(FileLinks, { project, files: thread.attachments })) : null;
			} else if (card && card.kind === "decision") {
				const decision = project.decisions[card.decisionId];
				const thread = decision ? project.threads.find((t) => t.id === decision.threadId) : undefined;
				body = h(DecisionCard, {
					decision, onAnswer,
					threadTitle: showThreadLabel && thread ? thread.title : null,
					onOpenThread: () => thread && onOpenThread(thread.id),
				});
			}
			const hideText = card && card.kind === "decision";
			return h(React.Fragment, null,
				hideText ? null : h("div", { className: "mp-msg-text" }, h(RichText, { text: message.text, members: project.members })),
				message.handoff && message.handoff.note ? h("div", { className: "mp-hint" }, `助理补充：${message.handoff.note}`) : null,
				message.handoff ? h("button", { type: "button", className: "mp-source-link", onClick: () => navigateHome(message.handoff.sourceRef) }, "来自个人助理，查看原话") : null,
				body,
				h(FileLinks, { project, files: message.attachments }),
				Object.keys(message.reactions || {}).length ? h("div", { className: "mp-reacts" }, Object.entries(message.reactions).map(([emoji, n]) => h("span", { key: emoji }, emoji, h("b", { className: "mp-mono" }, n)))) : null);
		}

		/** A message: sender in bold, model, time; no avatar. Follow-ups from the same sender show the body only. */
		function MessageGroup({ group, project, ...rest }) {
			const author = group.author;
			const member = project.members.find((m) => m.id === author.id);
			return h(React.Fragment, null, group.messages.map((m, i) => i === 0
				? h("div", { key: m.id, className: "mp-msg" },
					h("div", { className: "mp-msg-main" },
						h("div", { className: "mp-msg-head" },
							h("b", null, author.kind === "human" ? author.name : shownName(member, author.name)),
							author.kind === "human" ? h("span", { className: "mp-model" }, "你") : member && member.role === "worker" ? null : h(ModelTag, { member }),
							h("time", { className: "mp-mono" }, clock(m.at))),
						h(MessageBody, { message: m, project, ...rest })))
				: h("div", { key: m.id, className: "mp-msg mp-msg-compact", title: clock(m.at) },
					h("div", { className: "mp-msg-main" }, h(MessageBody, { message: m, project, ...rest })))));
		}

		function Timeline({ rows, project, onOpenThread, activeThreadId, onAnswer, showThreadLabel }) {
			const items = useMemo(() => groupRows(rows), [rows]);
			return h("div", { className: "mp-timeline" }, items.map((item) => {
				if (item.kind === "day") return h(SystemLine, { key: item.key }, item.label);
				if (item.kind === "group") return h("div", { key: item.key, className: "mp-turn" }, h(MessageGroup, { group: item, project, onOpenThread, activeThreadId, onAnswer, showThreadLabel }));
				if (item.kind === "events") return h(EventFold, { key: item.key, rows: item.rows });
				return null;
			}));
		}

		// ------------------------------------------------------------------ composer

		/** Raft Composer: one bordered field, the place in the placeholder, the one pink button is Send. */
		function Composer({ members, project, placeholder, onSend, autoFocus, prefill, draftKey, hint }) {
			const [text, setText] = useState(() => { try { return draftKey ? window.sessionStorage.getItem(`mira-projects:draft:${draftKey}`) || "" : ""; } catch (_) { return ""; } });
			const [busy, setBusy] = useState(false);
			const [error, setError] = useState(null);
			const [menu, setMenu] = useState(null);
			const ref = useRef(null);
			// The project assistant reads every chat message; it is not offered as someone to @.
			const assistants = members.filter((m) => m.kind === "assistant" && m.role !== "coordinator");
			const appliedPrefill = useRef(null);
			useEffect(() => {
				if (!draftKey) return;
				try { if (text) window.sessionStorage.setItem(`mira-projects:draft:${draftKey}`, text); else window.sessionStorage.removeItem(`mira-projects:draft:${draftKey}`); } catch (_) { /* storage unavailable */ }
			}, [draftKey, text]);
			useLayoutEffect(() => {
				if (!prefill || !prefill.nonce || appliedPrefill.current === prefill.nonce) return;
				appliedPrefill.current = prefill.nonce;
				setText(prefill.text || "");
				requestAnimationFrame(() => ref.current && ref.current.focus());
				prefill.consume?.();
			}, [prefill && prefill.nonce]);

			const refreshMenu = (value, caret) => {
				const head = value.slice(0, caret);
				const match = /@([^\s@]*)$/.exec(head);
				if (!match) { setMenu(null); return; }
				const query = match[1];
				const items = assistants.filter((m) => m.role !== "worker" && (m.name.startsWith(query) || m.id.startsWith(query)));
				setMenu(items.length ? { start: caret - match[0].length, caret, items, index: 0 } : null);
			};
			const pickMention = (member) => {
				if (!menu) return;
				const next = `${text.slice(0, menu.start)}@${member.name} ${text.slice(menu.caret)}`;
				setText(next);
				setMenu(null);
				requestAnimationFrame(() => {
					const el = ref.current;
					if (!el) return;
					const pos = menu.start + member.name.length + 2;
					el.focus();
					el.setSelectionRange(pos, pos);
				});
			};
			const send = async () => {
				const body = text.trim();
				if (!body || busy) return;
				setBusy(true); setError(null);
				try { await onSend(body); setText(""); setMenu(null); } catch (e) { setError(e.message); } finally { setBusy(false); }
			};
			const onKeyDown = (event) => {
				if (event.nativeEvent && event.nativeEvent.isComposing) return;
				if (menu) {
					if (event.key === "ArrowDown") { event.preventDefault(); setMenu({ ...menu, index: (menu.index + 1) % menu.items.length }); return; }
					if (event.key === "ArrowUp") { event.preventDefault(); setMenu({ ...menu, index: (menu.index - 1 + menu.items.length) % menu.items.length }); return; }
					if (event.key === "Enter" || event.key === "Tab") { event.preventDefault(); pickMention(menu.items[menu.index]); return; }
					if (event.key === "Escape") { event.preventDefault(); setMenu(null); return; }
				}
				if (event.key === "Enter" && !event.shiftKey) { event.preventDefault(); send(); }
			};
			useEffect(() => {
				const el = ref.current;
				if (!el) return;
				el.style.height = "auto";
				el.style.height = `${Math.min(el.scrollHeight, 128)}px`;
			}, [text]);
			return h("div", { className: "mp-composer" },
				menu ? h("div", { className: "mp-menu", role: "listbox" },
					h("div", { className: "mp-menu-label" }, "在这个项目里"),
					menu.items.map((m, i) => h("button", {
						key: m.id, type: "button", role: "option", "aria-selected": i === menu.index,
						className: "mp-menu-item" + (i === menu.index ? " mp-menu-on" : ""),
						onMouseDown: (e) => { e.preventDefault(); pickMention(m); },
					}, h(Avatar, { author: m, project, size: "xs" }), h("b", null, m.name), h(ModelTag, { member: m }), h("span", null, m.persona)))) : null,
				h("textarea", {
					ref, rows: 1, value: text, placeholder, autoFocus,
					onChange: (e) => { setText(e.target.value); refreshMenu(e.target.value, e.target.selectionStart); },
					onKeyDown,
					onClick: (e) => refreshMenu(e.target.value, e.target.selectionStart),
					onBlur: () => setTimeout(() => setMenu(null), 120),
				}),
				h("div", { className: "mp-composer-bar" },
					h("span", { className: "mp-composer-hint" }, error ? h("span", { className: "mp-error" }, error) : hint || "@ 点名，Shift+Enter 换行"),
					h("button", { type: "button", className: "mp-send", disabled: busy || !text.trim(), onClick: send, "aria-label": busy ? "发送中" : "发送", title: "发送（Enter）" }, h(Icon, { name: "send", size: 16 }))));
		}

		// ------------------------------------------------------------------ project list + dock

		function CreateProject({ onCreated, onCancel }) {
			const [name, setName] = useState("");
			const [instructions, setInstructions] = useState("");
			const [cwd, setCwd] = useState("");
			const [isolated, setIsolated] = useState(true);
			const [gitlabEnabled, setGitlabEnabled] = useState(false);
			const [gitlabUrl, setGitlabUrl] = useState("");
			const [gitlabProject, setGitlabProject] = useState("");
			const [targetBranch, setTargetBranch] = useState("main");
			const [tokenEnv, setTokenEnv] = useState("GITLAB_TOKEN");
			const [busy, setBusy] = useState(false);
			const [error, setError] = useState(null);
			const submit = async (event) => {
				event.preventDefault();
				if (!name.trim()) return;
				setBusy(true); setError(null);
				try { const res = await act("createProject", { name: name.trim(), instructions, cwd, workspaceMode: gitlabEnabled ? "worktree" : isolated ? "auto" : "shared", ...(gitlabEnabled ? { gitlab: { url: gitlabUrl, project: gitlabProject, targetBranch, tokenEnv } } : {}) }); onCreated(res.projectId); }
				catch (e) { setError(e.message); }
				finally { setBusy(false); }
			};
			return h("form", { className: "mp-create", onSubmit: submit, style: { maxHeight: "78vh", overflowY: "auto" } },
				h("label", null, "项目名", h("input", { value: name, autoFocus: true, maxLength: 80, onChange: (e) => setName(e.target.value), placeholder: "例如：官网改版" })),
				h("label", null, "项目说明（可选）", h("textarea", { value: instructions, rows: 3, onChange: (e) => setInstructions(e.target.value), placeholder: "所有助理都会读到：目标、约束、代码在哪" })),
				h("label", null, "项目目录（可选）", h("input", { value: cwd, onChange: (e) => setCwd(e.target.value), placeholder: "执行者在这个文件夹里干活，例如 ~/Projects/官网", spellCheck: false })),
				h("label", { className: "mp-create-check" }, h("input", { type: "checkbox", checked: isolated, disabled: gitlabEnabled, onChange: e => setIsolated(e.target.checked) }), "每个任务用自己的工作副本（Git 项目）"),
				h("label", { className: "mp-create-check" }, h("input", { type: "checkbox", checked: gitlabEnabled, onChange: e => { setGitlabEnabled(e.target.checked); if (e.target.checked) setIsolated(true); } }), "交付时在 GitLab 提合并请求，验收后合并"),
				gitlabEnabled ? h("div", { className: "mp-gitlab-setup" },
					h("label", null, "GitLab 地址", h("input", { type: "url", required: true, value: gitlabUrl, onChange: e => setGitlabUrl(e.target.value), placeholder: "https://gitlab.example.com" })),
					h("label", null, "GitLab 项目", h("input", { required: true, value: gitlabProject, onChange: e => setGitlabProject(e.target.value), placeholder: "group/project 或数字项目 id" })),
					h("label", null, "目标分支", h("input", { required: true, value: targetBranch, onChange: e => setTargetBranch(e.target.value) })),
					h("label", null, "服务端凭据变量名", h("input", { required: true, value: tokenEnv, onChange: e => setTokenEnv(e.target.value), spellCheck: false })),
					h("p", { className: "mp-create-note" }, "填服务端环境变量的名字，密钥本身不进页面。交付时提交并推送这个任务的分支，验收认的是那一次提交。")) : null,
				h("p", { className: "mp-create-note" }, "建好后，在项目里说一句要做什么：项目助理先问清楚，再开任务交给执行者。执行用的模型可以在任务里换。不填目录时，会新建一个同名工作区。"),
				error ? h("div", { className: "mp-error" }, error) : null,
				h("div", { className: "mp-create-bar" },
					onCancel ? h(Btn, { quiet: true, onClick: onCancel }, "取消") : null,
					h("button", { type: "submit", className: "mp-btn mp-btn-primary", disabled: busy || !name.trim() }, busy ? "创建中" : "创建项目")));
		}

		function ProjectSettings({ project, onClose }) {
			const [name, setName] = useState(project.name);
			const [instructions, setInstructions] = useState(project.instructions || "");
			const [revision] = useState(project.configRevision || 0);
			const [members, setMembers] = useState(() => project.members.filter(m => m.kind === "assistant").map(m => ({ id: m.id, name: m.name, role: m.role, persona: m.persona || "", modelSelection: m.modelSelection || null, runtimeSelection: m.runtimeSelection || null })));
			const [catalog, setCatalog] = useState([]);
			const [error, setError] = useState(null);
			const [busy, setBusy] = useState(false);
			useEffect(() => {
				let alive = true;
				fetch("api/mira-projects/models", { credentials: "same-origin" }).then(async r => { const data = await r.json(); if (!r.ok) throw Error(data.error || "模型列表暂不可用"); if (alive) setCatalog(data.groups || []); }).catch(e => { if (alive) setError(e.message); });
				return () => { alive = false; };
			}, []);
			const update = (index, patch) => setMembers(list => list.map((m, i) => i === index ? { ...m, ...patch } : m));
			const runtimes = useSnapshot().runtimes || {};
			const models = catalog.flatMap(group => group.models.map(model => ({ value: JSON.stringify({ provider: group.id, model: model.id }), label: `${group.name || group.id} / ${model.name || model.id}` })));
			const save = async event => {
				event.preventDefault(); setBusy(true); setError(null);
				try { await act("configureProject", { projectId: project.id, expectedRevision: revision, name, instructions, members }); onClose(); }
				catch (e) { setError(e.message); }
				finally { setBusy(false); }
			};
			return h("form", { className: "mp-create mp-project-settings", onSubmit: save, "aria-label": "项目设置" },
				h("h2", null, "项目设置"),
				h("p", { className: "mp-create-note" }, "改动在下一次唤醒时生效。"),
				h(ProjectIconPicker, { project }),
				h("label", null, "项目名", h("input", { "aria-label": "项目名", value: name, maxLength: 80, required: true, onChange: e => setName(e.target.value) })),
				h("label", null, "项目说明", h("textarea", { "aria-label": "项目说明", value: instructions, rows: 8, maxLength: 20000, onChange: e => setInstructions(e.target.value) })),
				// The project assistant is kept in what is saved but not listed: it is not a member the owner manages. The
				// per-task roles the project assistant wrote sit under a fold: the owner sees tasks and models, not roles.
				...(() => { const field = (member, index) => {
					const value = member.modelSelection ? JSON.stringify(member.modelSelection) : "";
					const runner = member.runtimeSelection?.provider || "dsh";
					return h("fieldset", { key: member.id || `new-${index}`, className: "mp-member-settings", "data-member-id": member.id || "new" },
						h("legend", null, member.role === "memory" ? "记忆助理" : isProjectExecutor(member) ? "执行者" : member.name),
						h("label", null, member.role === "memory" ? "成员名" : "名称", h("input", { value: member.name, maxLength: 40, required: true, readOnly: member.id === "mira" || isProjectExecutor(member), onChange: e => update(index, { name: e.target.value }) })),
						h("label", null, "职责说明", h("textarea", { value: member.persona, rows: 2, maxLength: 4000, onChange: e => update(index, { persona: e.target.value }) })),
						member.role === "worker" ? h("label", null, "由谁执行", h("select", { value: runner, "aria-label": `${member.name}由谁执行`, onChange: e => update(index, { runtimeSelection: e.target.value === "dsh" ? null : { provider: e.target.value, deviceId: "local" } }) },
							h("option", { value: "dsh" }, "DSH（默认）"),
							Object.keys(RUNNER_NAMES).map(id => h("option", { key: id, value: id, disabled: !runtimes[id] && runner !== id }, RUNNER_NAMES[id] + (runtimes[id] ? "" : "（这台机器没装）"))))) : null,
						h("label", null, "使用模型", h("select", { value, disabled: runner !== "dsh", "aria-label": `${member.name}使用模型`, onChange: e => update(index, { modelSelection: e.target.value ? JSON.parse(e.target.value) : null }) },
							h("option", { value: "" }, "跟随 DSH 默认模型"),
							value && !models.some(m => m.value === value) ? h("option", { value }, `${member.modelSelection.provider} / ${member.modelSelection.model}（当前配置）`) : null,
							models.map(m => h("option", { key: m.value, value: m.value }, m.label)))),
						runner !== "dsh" ? h("p", { className: "mp-create-note" }, `${RUNNER_NAMES[runner] || runner} 用它自己配置的模型，上面的「使用模型」对它不起作用。它的任务没有 DSH 对话：在项目的任务列表里点开，可以留言，也可以拿到「在终端接着做」的命令。`) : null,
						member.role === "worker" && !isProjectExecutor(member) ? h(Btn, { quiet: true, disabled: busy, onClick: () => setMembers(list => list.filter((_, i) => i !== index)) }, "移除") : null);
				}; const workers = members.filter(m => m.role === "worker" && !isProjectExecutor(m)).length; return [
					...members.map((member, index) => member.role === "memory" || isProjectExecutor(member) ? field(member, index) : null),
					h("details", { key: "workers", className: "mp-workers-fold" },
						h("summary", null, `做事说明 ${workers}`),
						members.map((member, index) => member.role === "worker" && !isProjectExecutor(member) ? field(member, index) : null),
						h(Btn, { quiet: true, disabled: busy || members.length >= 12, onClick: () => setMembers(list => [...list, { name: `新说明${list.length - 1}`, role: "worker", persona: "", modelSelection: null }]) }, "+ 添加做事说明"))];
				})(),
				error ? h("div", { className: "mp-error", role: "alert" }, error) : null,
				revision !== (project.configRevision || 0) ? h("p", { className: "mp-error" }, "其他页面已更新设置；请关闭后重新打开。") : null,
				h("div", { className: "mp-create-bar" }, h(Btn, { quiet: true, onClick: onClose, disabled: busy }, "关闭设置"), h("button", { className: "mp-btn mp-btn-primary", type: "submit", disabled: busy || revision !== (project.configRevision || 0) }, busy ? "保存中" : "保存项目设置")));
		}

		function AutomationPanel({ project, onClose }) {
			const newRequest = () => window.crypto?.randomUUID ? window.crypto.randomUUID() : Date.now().toString(36) + "-" + Math.random().toString(36).slice(2);
			const [requestId, setRequestId] = useState(newRequest);
			const [title, setTitle] = useState("");
			const [prompt, setPrompt] = useState("检查项目进展；只在有新结果、失败或需要我处理时汇报，不重复已完成的事。");
			const [target, setTarget] = useState("coordinator");
			const [kind, setKind] = useState("every");
			const [minutes, setMinutes] = useState(60);
			const [time, setTime] = useState("09:00");
			const [zone, setZone] = useState("Asia/Shanghai");
			const [busy, setBusy] = useState(false);
			const [error, setError] = useState(null);
			// As Codex's 定时任务: the form opens from 新建任务 and closes again once the task exists.
			const [creating, setCreating] = useState(false), count = (project.automations || []).length;
			useEffect(() => { setCreating(false); }, [count]);
			const run = async (op, body) => { setBusy(true);setError(null);try { return await act(op, { projectId: project.id, ...body }); } catch(e) { setError(e.message);return null; } finally { setBusy(false); } };
			const create = async event => { event.preventDefault();const rule = kind === "daily" ? { kind, time, timeZone: zone } : kind === "after" ? { kind, seconds: Number(minutes) * 60 } : { kind, minutes: Number(minutes) };if (await run("createAutomation", { title, prompt, target, rule, requestId })) { setTitle("");setRequestId(newRequest()); } };
			const toggle = task => run(task.status === "error" && !task.enabled ? task.deleteWanted ? "deleteAutomation" : "pauseAutomation" : task.enabled && task.status !== "error" ? "pauseAutomation" : "resumeAutomation", { automationId: task.id });
			const labels = { creating: "正在创建", active: "已启用", stopping: "正在停止", paused: "已暂停", complete: "已结束", missing: "原生任务已移除", error: "待重试" };
			return h("section", { className: "mp-create mp-project-settings", "aria-label": "自动任务" },
				h("h2", null, "自动任务"),
				!creating && !count ? h("div", { className: "mp4-empty" }, h(Icon, { name: "clock", size: 28 }), h("b", null, "安排任务"), h("p", null, "到点让项目助理或记忆助理去做一件事。"), h("button", { type: "button", className: "mp4-pill-strong", onClick: () => setCreating(true) }, "新建任务")) : null,
				!creating && count ? h("button", { type: "button", className: "mp4-new-row", onClick: () => setCreating(true) }, h(Icon, { name: "plus", size: 14 }), "新建任务") : null,
				(project.automations || []).map(task => h("div", { key: task.id, className: "mp-member-settings", "data-automation-id": task.id },
					h("strong", null, task.title), h("span", { className: "mp-hint" }, `，${labels[task.status] || task.status}`),
					h("p", null, task.prompt),
					task.enabled && task.schedule?.scheduledAt ? h("p", { className: "mp-create-note" }, "下次：" + new Date(task.schedule.scheduledAt).toLocaleString()) : null,
					task.lastRun ? h("p", { className: "mp-create-note" }, "最近投递：" + new Date(task.lastRun.at).toLocaleString() + (task.lastRun.coalesced ? `（合并至少 ${task.lastRun.coalesced} 期）` : "")) : null,
					task.error ? h("p", { className: "mp-error" }, task.error) : null,
					h("div", { className: "mp-create-bar" }, h(Btn, { quiet: true, disabled: busy, onClick: () => toggle(task) }, task.enabled && task.status !== "error" ? "暂停" : task.status === "error" ? task.enabled ? "重试" : "重试停止" : "再次启用"), h(Btn, { quiet: true, disabled: busy, onClick: () => run("deleteAutomation", { automationId: task.id }) }, "删除自动任务")))),
				creating ? h("form", { onSubmit: create, className: "mp-create", "aria-label": "新建自动任务" },
					h("label", null, "任务名", h("input", { "aria-label": "任务名", value: title, maxLength: 80, required: true, onChange: e => setTitle(e.target.value) })),
					h("label", null, "任务说明", h("textarea", { "aria-label": "任务说明", value: prompt, maxLength: 4000, rows: 3, required: true, onChange: e => setPrompt(e.target.value) })),
					h("label", null, "唤醒对象", h("select", { "aria-label": "唤醒对象", value: target, onChange: e => setTarget(e.target.value) }, h("option", { value: "coordinator" }, "项目助理"), h("option", { value: "mira" }, "记忆助理"))),
					h("label", null, "时间规则", h("select", { "aria-label": "时间规则", value: kind, onChange: e => setKind(e.target.value) }, h("option", { value: "every" }, "每隔几分钟"), h("option", { value: "after" }, "几分钟后一次"), h("option", { value: "daily" }, "每天"))),
					kind === "daily" ? h("div", null, h("input", { "aria-label": "每天时间", type: "time", value: time, onChange: e => setTime(e.target.value) }), h("input", { "aria-label": "时区", value: zone, onChange: e => setZone(e.target.value) })) : h("label", null, "分钟", h("input", { "aria-label": "间隔分钟", type: "number", min: 1, max: 43200, value: minutes, required: true, onChange: e => setMinutes(e.target.value) })),
					h("div", { className: "mp-create-bar" }, h("button", { className: "mp-btn mp-btn-primary", type: "submit", disabled: busy || !title.trim() }, "创建自动任务"), h(Btn, { quiet: true, onClick: () => setCreating(false) }, "取消"))) : null,
				error ? h("div", { className: "mp-error", role: "alert" }, error) : null);
		}

		function popoutDock() {
			const left = Math.max(0, (window.screen && window.screen.availWidth ? window.screen.availWidth : 1200) - 420);
			window.open(DOCK_URL, "mira-dock", `popup=yes,width=380,height=640,left=${left},top=80`);
		}

		/** Raft sidebar section header: disclosure, title, count, trailing action. */
		function SectionHead({ label, count, open, onToggle, action }) {
			return h("div", { className: "mp-sec" },
				h("button", { type: "button", className: "mp-sec-toggle", onClick: onToggle, "aria-expanded": open },
					h("span", { className: "mp-sec-chev" + (open ? " mp-sec-chev-on" : "") }, h(Icon, { name: "chevron", size: 12 })),
					h("span", null, label),
					count != null ? h("span", { className: "mp-mono mp-sec-count" }, count) : null),
				action);
		}

		/** The bottom of the sidebar: Mira's stack. Pops out into its own small window. */
		function DockStack({ dock, projectId, onJump, project }) {
			const [open, setOpen] = useState(() => readPref("dock") !== "closed");
			const toggle = () => { const next = !open; setOpen(next); writePref("dock", next ? "open" : "closed"); };
			const cards = dock ? [...dock.cards].sort((a, b) => (a.projectId === projectId ? -1 : 0) - (b.projectId === projectId ? -1 : 0) || b.at - a.at) : [];
			const waiting = dock ? dock.waiting.length + dock.review.length : 0;
			const total = cards.length + waiting;
			const first = dock ? (dock.waiting[0] || dock.review[0]) : null;
			return h("div", { className: "mp-dock" },
				h(SectionHead, { label: "个人助理", count: total || null, open, onToggle: toggle, action: h(IconButton, { name: "popout", label: "弹出成悬浮窗", onClick: popoutDock }) }),
				open ? h("div", { className: "mp-dock-body" },
					total === 0 ? h("div", { className: "mp-hint mp-dock-empty" }, "没有等你的事") : null,
					first ? h("button", { type: "button", className: "mp-row", onClick: () => onJump({ projectId: first.projectId, threadId: first.threadId || first.id }) },
						h(Light, { tone: dock.waiting.length ? "pink" : "lav" }),
						h("span", { className: "mp-row-name" }, `${dock.waiting.length ? `${dock.waiting.length} 个等你拍板` : ""}${dock.waiting.length && dock.review.length ? "，" : ""}${dock.review.length ? `${dock.review.length} 个待验收` : ""}`),
						[...dock.waiting, ...dock.review].some((x) => x.projectId !== projectId) ? h("span", { className: "mp-hint" }, "含其他项目") : null) : null,
					cards.slice(0, 2).map((c) => h(MiraCard, { key: c.id, card: c, onJump, compact: true, showProject: c.projectId !== projectId, project })),
					cards.length > 2 ? h("div", { className: "mp-hint mp-dock-more" }, `还有 ${cards.length - 2} 张，在“回顾与卡片”里`) : null) : null);
		}

		/** Raft sidebar: sections of one-line rows; the selected row is yellow with an ink edge, unread is a pink count. */
		function ProjectList({ selectedId, view, onSelect, onReview, dock, onJump, project, onCollapse }) {
			const [membersOpen, setMembersOpen] = useState(false);
			const cardCount = dock ? dock.cards.filter((c) => c.projectId === selectedId).length : 0;
			const agents = project ? project.members.filter((m) => m.kind === "assistant") : [];
			const busy = project ? workingLine(project) : [];
			return h("aside", { className: "mp-list" },
				h("div", { className: "mp-list-head" }, h("span", null, "项目导航"), h(IconButton, { name: "close", label: "收起此栏", onClick: onCollapse })),
				h("div", { className: "mp-list-items" },
					selectedId ? h("button", { type: "button", className: "mp-row" + (view === "review" ? " mp-row-on" : ""), onClick: onReview, title: "记忆助理的记忆和递给你的卡" },
						h("span", { className: "mp-glyph" }, h(Icon, { name: "spark", size: 16 })),
						h("span", { className: "mp-row-name" }, "回顾与卡片"),
						cardCount ? h("span", { className: "mp-count" }, cardCount) : null) : null,
					project ? h("button", { type: "button", className: "mp-row" + (view !== "review" ? " mp-row-on" : ""), onClick: () => onSelect(project.id), title: project.name }, h("span", { className: "mp-glyph" }, h(Icon, { name: "reply", size: 16 })), h("span", { className: "mp-row-name" }, "项目群聊")) : null,
					agents.length ? h(SectionHead, { label: "成员", count: agents.length, open: membersOpen, onToggle: () => setMembersOpen(!membersOpen) }) : null,
					membersOpen ? agents.map((m) => {
						const doing = busy.find((b) => b.who.id === m.id);
						return h("button", {
							key: m.id, type: "button", className: "mp-row mp-row-agent", title: `${m.name}，${modelInfo(m.model).model || "默认模型"}\n${m.persona}`,
							onClick: () => (m.role === "memory" ? onReview() : onSelect(selectedId)),
						},
							h(Avatar, { author: m, project, size: "xs", presence: presenceForMember(project, m), title: "" }),
							h("span", { className: "mp-row-name mp-row-name-agent" }, m.name),
							doing ? h("span", { className: "mp-row-side" }, doing.text) : h(ModelTag, { member: m, mono: true }));
					}) : null),
				h(DockStack, { dock, projectId: selectedId, onJump, project }));
		}

		// ------------------------------------------------------------------ thread panel

		/** Mira's handoff card: what is true now, what comes back, what you hand over, how completion is checked, where to stop. */
		function HandoffCard({ thread }) {
			const [open, setOpen] = useState(false);
			return h("div", { className: "mp-handoff" },
				h("button", { type: "button", className: "mp-handoff-head", onClick: () => setOpen(!open), "aria-expanded": open },
					h("span", { className: "mp-sec-chev" + (open ? " mp-sec-chev-on" : "") }, h(Icon, { name: "chevron", size: 12 })), h("span", null, "交接单")),
				open ? h(React.Fragment, null,
					h("div", { className: "mp-handoff-row" }, h("span", { className: "mp-k" }, "任务"), h("span", null, thread.brief)),
					thread.note ? h("div", { className: "mp-handoff-row" }, h("span", { className: "mp-k" }, "派单说明"), h("span", null, thread.note)) : null,
					thread.workspace ? h("div", { className: "mp-handoff-row" }, h("span", { className: "mp-k" }, "工作目录"), h("span", { style: { overflowWrap: "anywhere" } }, thread.workspace.status === "failed" ? thread.workspace.error : thread.workspace.cwd)) : null,
					thread.workspace?.branch ? h("div", { className: "mp-handoff-row" }, h("span", { className: "mp-k" }, "代码分支"), h("span", { style: { overflowWrap: "anywhere" } }, thread.workspace.branch)) : null,
					thread.cited.map((m) => h("div", { key: m.id, className: "mp-handoff-row" }, h("span", { className: "mp-k" }, "你的原话"), h("blockquote", null, m.text))),
					h("div", { className: "mp-handoff-row" }, h("span", { className: "mp-k" }, "怎么算完"), h("span", null, `${thread.assigneeName}把卡设为“待验收”并给出证据；你看过后点“验收通过”才归档。`)),
					h("div", { className: "mp-handoff-row" }, h("span", { className: "mp-k" }, "边界"), h("span", null, "删除数据、对外发送、改生产环境要先问你；需要你拍板时会发决策卡，不空转。"))) : null);
		}

		function Checklist({ items }) {
			if (!items.length) return null;
			return h("ul", { className: "mp-check" }, items.map((item, i) => h("li", { key: i, className: item.done ? "mp-done" : "" },
				h("i", null, item.done ? h(Icon, { name: "check", size: 10 }) : null), h("span", null, item.text))));
		}

		/** DSH sub-agents of this member's session: part of the member, not of the project. */
		function Children({ children }) {
			if (!children || !children.length) return null;
			return h("div", { className: "mp-children" },
				h("div", { className: "mp-k" }, "子代理", h("span", { className: "mp-hint" }, "，这位助理用 DSH 的 subagent 工具开的，不在群聊里，结果由它汇总")),
				children.map((c) => h("div", { key: c.id, className: "mp-child" },
					h(Light, { tone: c.running ? "yellow" : "stone" }),
					h("span", { className: "mp-ellipsis" }, c.label || c.id),
					h("span", { className: "mp-mono mp-hint" }, c.mode === "continuable" ? "可续" : c.mode === "one-shot" ? "一次性" : c.mode))));
		}

		/** Task details the product keeps on the task page: status, checklist, handoff, accept. Folded under the original message. */
		function TaskDetails({ project, thread }) {
			const [open, setOpen] = useState(() => thread.state === "review_ready");
			const [actionError, setActionError] = useState(null);
			const [acting, setActing] = useState(false);
			const run = async (op, body) => { setActing(true); setActionError(null); try { await act(op, body); } catch (error) { setActionError(error.message); } finally { setActing(false); } };
			const turn = thread.lastTurn;
			const assignee = project.members.find((m) => m.id === thread.assignee) || { kind: "assistant", id: thread.assignee, name: thread.assigneeName };
			const done = thread.checklist.filter((c) => c.done).length;
			return h("div", { className: "mp-details" },
				h("button", { type: "button", className: "mp-details-head", onClick: () => setOpen(!open), "aria-expanded": open },
					h("span", { className: "mp-sec-chev" + (open ? " mp-sec-chev-on" : "") }, h(Icon, { name: "chevron", size: 12 })),
					h(StateChip, { state: thread.state === "done" ? "done" : thread.resolved ? "archived" : thread.state, label: thread.experience?.reason }),
					h("span", { className: "mp-details-title" }, thread.title),
					thread.checklist.length ? h("span", { className: "mp-mono mp-hint" }, `${done}/${thread.checklist.length}`) : null),
				open ? h("div", { className: "mp-details-body" },
					h("div", { className: "mp-task-live" },
						h(Avatar, { author: assignee, project, size: "2xs", presence: presenceOf(thread.status, turn, thread.sessionId) }),
						h("span", null, `${assignee.name}，${activityText(thread.status, thread.activity, turn) || "还没开始"}，${thread.priority}`)),
					h(Checklist, { items: thread.checklist }),
					project.gitlab ? h("div", { className: "mp-notice", style: { overflowWrap: "anywhere" } },
						h("b", null, ({ publishing: "正在创建或更新 MR", ready: "MR 已就绪", publish_error: "MR 发布失败", merging: "正在合并已验收的提交", merge_error: "MR 合并失败，线程保持待验收", merged: "MR 已合并", closing: "正在关闭 MR", closed: "MR 已关闭", cancelled: "MR 发布已取消", close_error: "MR 关闭失败" })[thread.gitlab?.status] || "交付后自动创建 MR"),
						thread.gitlab?.url ? h("div", null, h("a", { href: thread.gitlab.url, target: "_blank", rel: "noopener" }, "打开 GitLab MR"), thread.gitlab.sha ? `，${thread.gitlab.sha.slice(0, 8)}` : "") : null,
						thread.gitlab?.error ? h("div", { className: "mp-error" }, thread.gitlab.error) : null,
						thread.gitlab?.status === "publish_error" ? h(Btn, { disabled: acting, onClick: () => run("publishThread", { projectId: project.id, threadId: thread.id }) }, "重试发布 MR") : null) : null,
					h(HandoffCard, { thread }),
					h(Children, { children: thread.children }),
					thread.generation > 1 ? h("div", { className: "mp-notice" }, `会话已重建（第 ${thread.generation} 代），助理按记录续上。`) : null,
					turn && turn.silent && !turn.failed ? h("div", { className: "mp-notice mp-warn" }, "上一轮助理没有通过工具发出任何东西。") : null,
					turn && turn.failed ? h("div", { className: "mp-notice mp-warn" }, `唤醒失败：${turn.error || "未知错误"}`) : null,
					h("div", { className: "mp-task-acts" },
					thread.resolved && thread.state !== "done"
							? h(Btn, { onClick: () => act("updateThread", { projectId: project.id, threadId: thread.id, resolved: false }) }, "重新打开")
							: thread.state === "review_ready"
								? h(Btn, { strong: true, disabled: acting || (project.gitlab && !["ready", "merge_error"].includes(thread.gitlab?.status)), onClick: () => run("updateThread", { projectId: project.id, threadId: thread.id, resolved: true, state: "done", expectedSha: thread.gitlab?.sha }) }, h(Icon, { name: "check", size: 14 }), project.gitlab ? (thread.gitlab?.status === "merge_error" ? "重试合并" : "验收通过并合并") : "验收通过")
								: null,
						thread.resolved ? null : h(Btn, { quiet: true, disabled: acting, onClick: () => run("updateThread", { projectId: project.id, threadId: thread.id, resolved: true }) }, "归档")), actionError ? h("div", { className: "mp-error" }, actionError) : null) : null);
		}

		function TaskSummary({ project, thread, onOpenRelated }) {
			const [open, setOpen] = useState(true);
			const info = thread.experience;
			if (!info) return null;
			const rows = [["目标", info.summary.goal], ["约束", info.summary.constraints], ["进展", info.summary.progress || "尚未发布进展摘要"], ["交付", thread.attachments?.length ? thread.attachments.map(a => `${a.path}，v${a.revision}`).join("、") : thread.gitlab?.url ? "已发布 MR，可在任务详情查看" : "尚未发布交付"], ["下一步", info.summary.next]];
			return h("section", { className: "mp-task-summary", "aria-label": "任务摘要" },
				h("button", { type: "button", className: "mp-summary-toggle", "aria-expanded": open, onClick: () => setOpen(v => !v) }, h("strong", null, "任务摘要"), h("span", { className: "mp-task-reason" }, info.reason), h(Icon, { name: "chevron", size: 12 })),
				open ? h("dl", null, rows.filter(([, value]) => value).map(([label, value]) => h(React.Fragment, { key: label }, h("dt", null, label), h("dd", null, value)))) : null,
				thread.relatedThreadId ? h("button", { type: "button", className: "mp-context-toggle", onClick: () => onOpenRelated(thread.relatedThreadId) }, "来源任务：", project.threads.find(t => t.id === thread.relatedThreadId)?.title || "查看原任务") : null);
		}

		function ExecutionOverview({ project, thread, openSession }) {
			const [open, setOpen] = useState(false);
			const execution = thread.experience?.execution;
			const sessions = thread.sessions || [], children = thread.children || [];
			return h("section", { className: "mp-execution", "aria-label": "任务执行" },
				h("div", { className: "mp-execution-head" }, h("strong", null, execution?.label || "当前无执行"), h("button", { type: "button", className: "mp-context-toggle", "aria-expanded": open, onClick: () => setOpen(v => !v) }, open ? "收起执行详情" : "执行详情")),
				thread.activity ? h("div", { className: "mp-hint" }, `最近操作：${ACTIVITY[thread.activity.tool] || thread.activity.tool}，${ago(thread.activity.at)}`) : h("div", { className: "mp-hint" }, "执行动态会显示在这里"),
				children.length ? h("div", { className: "mp-hint" }, `${children.length} 个子执行者，${execution?.activeChildren || 0} 个运行中`) : null,
				execution?.held ? h("p", { className: "mp-hint", role: "status" }, "设备停止尚未确认，系统不会重跑原操作。") : null,
				open ? h("div", { className: "mp-execution-body" },
					(execution?.devices || []).map(d => h("div", { key: d.requestId, className: "mp-execution-row" }, h("b", null, project.devices?.devices.find(item => item.id === d.deviceId)?.name || "设备操作"), h("span", { className: "mp-hint" }, d.stopped && d.outcome === "unknown" ? "已确认停止，结果待确认" : ({ queued: "排队中", waiting_device: "等待连接", starting: "准备执行", running: "正在执行", stopping: "正在停止，等待回执", unconfirmed: "等待停止确认" })[d.phase] || "等待确认"))),
					sessions.length ? sessions.map(s => h("div", { key: s.id, className: "mp-execution-row" }, h("div", null, h("b", null, s.current ? "当前会话" : "历史会话"), h("div", { className: "mp-hint" }, `第 ${s.generation} 代，${s.node}，${ago(s.boundAt)}`)), h(Btn, { quiet: true, onClick: () => openSession(s.id) }, "查看会话记录"))) : h("p", { className: "mp-hint" }, "还没有执行会话"),
					children.map(c => h("div", { key: c.id, className: "mp-execution-row" }, h("div", null, h("b", null, c.label || "子执行者"), h("div", { className: "mp-hint" }, `${c.status === "stopping" ? "正在停止" : c.running ? "正在执行" : c.status === "not_loaded" ? "当前未加载" : "当前空闲"}，${sessions.find(s => s.id === (c.rootSessionId || c.parentId))?.current === false ? "历史主会话下的执行者" : "当前任务的子执行者"}`)), h(Btn, { quiet: true, onClick: () => openSession({ parentSessionId: c.parentId, childSessionId: c.id, mode: c.mode }) }, "查看子执行记录"))),
					h("p", { className: "mp-hint" }, "在会话记录中查看已记录的输入、工具操作和结果。子执行结果由主会话汇总到本线程。")) : null);
		}

		/** Raft thread panel: "线程" with the place and reply count; the original message, "回复从这里开始", the replies, the composer. No tabs. */
		function ThreadPanel({ project, thread, onClose, onFollowUp, onOpenRelated, onAnswer, openSession, prefill, narrow, navigationToggle, projectExpanded, onToggleProject, projectToggleRef, hidden }) {
			const reading = useReadingPosition(!hidden, thread.id, thread.timeline.length, false);
			const replies = thread.timeline.filter((r) => (r.kind || "message") === "message");
			const original = project.chat.find((m) => m.card && m.card.kind === "thread" && m.card.threadId === thread.id);
			const followers = [];
			for (const r of replies) if (!followers.some((f) => f.id === r.author.id && f.kind === r.author.kind)) followers.push(r.author);
			return h("aside", { className: "mp-thread", "aria-label": `线程：${thread.title}`, hidden },
				h("header", { className: "mp-head mp-thread-head" },
					navigationToggle,
					narrow ? h(IconButton, { name: "back", label: "返回群聊", onClick: onClose }) : null,
					h("div", { className: "mp-head-text" },
						h("div", { className: "mp-head-title", title: thread.title }, thread.title),
						h("div", { className: "mp-head-meta" }, h("button", { type: "button", className: "mp-thread-project-link", "aria-label": `返回项目群聊：${project.name}`, onClick: onClose }, project.name), `，${replies.length} 条回复`)),
					h("button", { type: "button", className: "mp-context-toggle", ref: projectToggleRef, "aria-label": projectExpanded ? "收起主项目" : "展开主项目", "aria-expanded": projectExpanded, "aria-controls": "mp-project-context", onClick: onToggleProject }, h(Icon, { name: "menu", size: 14 }), projectExpanded ? "收起项目" : "展开项目"),
					h("div", { className: "mp-head-actions" },
						followers.length ? h("span", { className: "mp-followers", title: `${followers.length} 位在这个线程里` }, h(Faces, { people: followers, project }), h("b", { className: "mp-mono" }, followers.length)) : null,
						thread.sessionId ? h(IconButton, { name: "session", label: "查看执行记录（DSH）", onClick: () => openSession(thread.sessionId) }) : null,
						narrow ? null : h(IconButton, { name: "close", label: "关闭线程", onClick: onClose }))),
				h("div", { className: "mp-thread-scroll", ...reading },
					h("div", { className: "mp-thread-content" },
					h(TaskSummary, { project, thread, onOpenRelated }),
					h(ExecutionOverview, { project, thread, openSession }),
					h("div", { className: "mp-thread-original" },
						original
							? h(MessageGroup, { group: { author: original.author, at: original.at, messages: [{ ...original, card: null }] }, project, onOpenThread: () => {}, onAnswer })
							: null,
						h("div", { className: "mp-thread-original-chip" }, h(TaskChip, { thread, project, onOpen: () => {} })),
						h(TaskDetails, { project, thread })),
					h(SystemLine, null, "回复从这里开始"),
					h(Timeline, { rows: thread.timeline, project, onOpenThread: () => {}, onAnswer, showThreadLabel: false }))),
				thread.state === "done" || thread.resolved ? h("div", { className: "mp-composer-wrap mp-thread-closed" }, h("span", null, thread.state === "done" ? "这件事已验收，任务记录会保留。" : "这件事已归档，尚未标记为解决。"), h(Btn, { onClick: onFollowUp }, "到项目讨论后续")) : h("div", { className: "mp-composer-wrap" }, h(Composer, {
					members: project.members, project,
					placeholder: "继续这件事，补充要求或反馈…",
					draftKey: `${project.id}:${thread.id}`,
					onSend: (text) => act("post", { projectId: project.id, threadId: thread.id, text, typed: text }),
					prefill: prefill && prefill.target === thread.id ? prefill : null,
				})));
		}

		// ------------------------------------------------------------------ chat

		function presenceForMember(project, m) {
			const open = project.threads.filter((t) => !t.resolved);
			if (m.role === "coordinator") return presenceOf(project.coordinator.status, project.coordinator.lastTurn, project.coordinator.sessionId);
			if (m.role === "memory") return presenceOf(project.mira.status, project.mira.lastTurn, project.mira.sessionId);
			const threads = open.filter((t) => t.assignee === m.id);
			if (threads.some((t) => t.experience ? t.experience.execution.active : t.status === "running")) return "busy";
			if (threads.some((t) => t.lastTurn && t.lastTurn.failed)) return "error";
			return threads.some((t) => t.sessionId) ? "online" : "offline";
		}

		/** Who is working right now, as Raft's working line: names and what they do, no animation. */
		function workingLine(project) {
			const busy = [];
			if (project.coordinator.status === "running" || project.coordinator.status === "queued") busy.push({ who: project.members.find((m) => m.role === "coordinator") || { kind: "assistant", id: "coordinator", name: "协调员" }, text: activityText(project.coordinator.status, project.coordinator.activity, project.coordinator.lastTurn), where: null });
			for (const t of project.threads) {
				if (t.experience ? t.experience.execution.active : t.status === "running") busy.push({ name: executorOf(project, t).name || t.assigneeName, who: project.members.find((m) => m.id === t.assignee) || { kind: "assistant", id: t.assignee, name: t.assigneeName }, text: t.experience?.execution.devices?.length ? t.experience.execution.label : t.status === "running" ? activityText(t.status, t.activity, t.lastTurn) : t.experience.execution.label, where: t.title, threadId: t.id, step: t.activity && t.activity.at });
			}
			if (project.mira.status === "running") busy.push({ who: project.members.find((m) => m.id === "mira") || { kind: "assistant", id: "mira", name: "记忆助理" }, text: activityText("running", project.mira.activity, project.mira.lastTurn), where: null });
			return busy;
		}

		/** Raft PanelHeader: 27px yellow place tile, the name, the working line beneath. Members live in the sidebar. */
		function Header({ project, projects, onSelect, onCreate, narrow, onSettings, onAutomations, navigationToggle }) {
			const open = project.threads.filter(t => !t.resolved);
			const counts = [[open.filter(t => t.experience?.group === "executing").length, "执行中"], [open.filter(t => t.experience?.group === "unresolved").length, "未解决"], [project.threads.filter(t => t.state === "done").length, "已解决"]].filter(([n]) => n > 0).map(([n, label]) => `${n} ${label}`);
			const busy = workingLine(project);
			const sub = busy.length === 1 ? `${busy[0].who.name}${busy[0].text}` : busy.length > 1 ? `${busy.length} 位在工作：${busy.map(b => b.who.name).join("、")}` : null;
			return h("header", { className: "mp-head" },
				navigationToggle,
				h("span", { className: "mp-head-tile" }, h(Icon, { name: "hash", size: 14 })),
				h("div", { className: "mp-head-text" },
					narrow ? h("select", { className: "mp-switch", "aria-label": "切换项目", value: project.id, onChange: e => { if (e.target.value === "__new") onCreate(); else onSelect(e.target.value); } }, projects.map(p => h("option", { key: p.id, value: p.id }, p.name)), h("option", { value: "__new" }, "+ 新建项目")) : h("h1", { className: "mp-head-title" }, project.name),
					h("div", { className: "mp-head-meta" }, sub ? h("span", { className: "mp-head-working" }, h(Light, { tone: "yellow", size: "sm" }), sub) : null, h("span", null, (sub ? "，" : "") + (counts.length ? counts.join("，") : project.instructions || "没有进行中的线程")))),
				h(Btn, { quiet: true, onClick: onSettings }, "项目设置"), h(Btn, { quiet: true, onClick: onAutomations }, "自动任务"));
		}

		function SharedFiles({ project }) {
			const [upload, setUpload] = useState(null), [path, setPath] = useState("");
			const [editing, setEditing] = useState(null), [draft, setDraft] = useState("");
			const [busy, setBusy] = useState(false), [error, setError] = useState(null);
			const [repoKind, setRepoKind] = useState("local"), [repoSource, setRepoSource] = useState(""), [repoRef, setRepoRef] = useState("HEAD");
			// A filter over the list, as Codex puts 筛选文件 above its file tree.
			const [q, setQ] = useState(""), all = project.files || [], needle = q.trim().toLowerCase(), rows = needle ? all.filter(f => f.path.toLowerCase().includes(needle)) : all;
			const intents = useRef(new Map()), running = useRef(false);
			const command = async (slot, key, op, payload) => {
				let current = intents.current.get(slot);
				if (!current || current.key !== key) { current = { key, payload: { ...payload, requestId: crypto.randomUUID() } };intents.current.set(slot, current); }
				const result = await act(op, current.payload);if (intents.current.get(slot) === current) intents.current.delete(slot);return result;
			};
			const run = async fn => { if (running.current) return;running.current = true;setBusy(true);setError(null);try { await fn(); } catch (e) { setError(e.message); } finally { running.current = false;setBusy(false); } };
			const publish = () => run(async () => {
				if (!upload) throw Error("请先选择文件");
				if (upload.size > 64 * 1024 * 1024) throw Error("单份文件最多64MiB");
				const target = path.trim() || upload.name, expected_revision = rows.find(file => file.path === target)?.revision || 0;
				const bytes = new Uint8Array(await upload.arrayBuffer()), chunks = [];
				for (let offset = 0; offset < bytes.length; offset += 512 * 1024) {
					const part = bytes.subarray(offset, offset + 512 * 1024);let binary = "";
					for (let i = 0; i < part.length; i += 8192) binary += String.fromCharCode(...part.subarray(i, i + 8192));
					chunks.push((await act("fileChunk", { projectId: project.id, data: btoa(binary) })).sha256);
				}
				const sha256 = [...new Uint8Array(await crypto.subtle.digest("SHA-256", bytes))].map(n => n.toString(16).padStart(2, "0")).join("");
				await command("upload", JSON.stringify([target, sha256]), "fileCommit", { projectId: project.id, path: target, expected_revision, chunks, sha256, size: bytes.length });
				setUpload(null);setPath("");
			});
			const edit = file => run(async () => {
				const response = await fetch(fileURL(project.id, file.id, file.revision), { credentials: "same-origin" });
				if (!response.ok) throw Error((await response.json()).error || "文件暂不可读");
				setDraft(await response.text());setEditing(file);
			});
			return h("div", { className: "mp4-filepane" },
				all.length > 3 ? h("input", { className: "mp4-input mp4-file-filter", type: "search", value: q, placeholder: "筛选文件", "aria-label": "筛选文件", onChange: e => setQ(e.target.value) }) : null,
				rows.length ? h(CardGroup, { label: `文件 ${rows.length}` }, rows.map(file => h("div", { key: file.id, className: "mp4-file-item" },
					h(DeliveredFile, { project, file }),
					h("div", { className: "mp4-file-acts" },
						(file.mime.startsWith("text/") || file.mime === "application/json") && file.size <= 128 * 1024 ? h("button", { type: "button", className: "mp4-link-btn", disabled: busy, onClick: () => edit(file) }, "编辑") : null,
						h("button", { type: "button", className: "mp4-link-btn", disabled: busy, onClick: () => run(() => act("post", { projectId: project.id, text: `附上共享文件：${file.path}`, file_paths: [file.path] })) }, "附到项目对话"),
						file.versions.length > 1 ? h("details", { className: "mp4-file-versions" }, h("summary", null, `历史版本 ${file.versions.length}`), [...file.versions].reverse().map(revision => h("a", { key: revision, href: fileURL(project.id, file.id, revision), download: true }, `下载第 ${revision} 版`))) : null))))
				: h("p", { className: "mp4-muted mp4-note" }, all.length ? "没有匹配的文件" : "还没有文件"),
				editing ? h(Section, { label: `编辑 ${editing.path}，基于 v${editing.revision}` },
					h("textarea", { className: "mp4-editor", "aria-label": "共享文件正文", value: draft, onChange: e => setDraft(e.target.value), disabled: busy, rows: 10 }),
					h("div", { className: "mp4-form-actions" }, h(Btn, { quiet: true, disabled: busy, onClick: () => setEditing(null) }, "取消编辑"),
						h(Btn, { strong: true, disabled: busy, onClick: () => run(async () => { await command("edit", JSON.stringify([editing.id, editing.revision, draft]), "fileWrite", { projectId: project.id, path: editing.path, text: draft, expected_revision: editing.revision });setEditing(null); }) }, "保存新版本"))) : null,
				h("details", { className: "mp4-fold" }, h("summary", null, "上传文件"), h("p", { className: "mp4-muted mp4-note" }, "上传后和执行者发布的文件放在一起，可以附到项目对话里给助理看。"),
					h("div", { className: "mp4-form" },
						h("label", { className: "mp4-field" }, "选择文件", h("span", { className: "mp4-file-pick" }, h("span", { className: "mp4-btn" }, "选择文件"), h("span", { className: "mp4-muted mp4-ell" }, upload ? upload.name : "还没选"), h("input", { className: "mp4-file-input", type: "file", disabled: busy, onChange: e => { const selected = e.target.files?.[0];setUpload(selected || null);if (selected) setPath(selected.name); } }))),
						h("label", { className: "mp4-field" }, "放在哪个路径", h("input", { className: "mp4-input", value: path, disabled: busy, onChange: e => setPath(e.target.value) })),
						h("div", { className: "mp4-form-actions" }, h(Btn, { strong: true, disabled: busy || !upload, onClick: publish }, busy ? "正在保存" : "发布文件")))),
				h("details", { className: "mp4-fold" }, h("summary", null, "仓库快照"),
					project.repository ? h("p", { className: "mp4-muted mp4-note" }, `当前快照第 ${project.repository.revision} 版，提交 ${project.repository.commit.slice(0, 12)}`) : null,
					h("p", { className: "mp4-muted mp4-note" }, "导入所选提交及其历史，未提交文件不导入。新线程使用新快照，已有线程保留原目录和版本。"),
					h("div", { className: "mp4-form" },
						h("label", { className: "mp4-field" }, "仓库来源类型", h("select", { className: "mp4-input", value: repoKind, disabled: busy, onChange: e => setRepoKind(e.target.value) }, h("option", { value: "local" }, "开发机目录"), h("option", { value: "remote" }, "远程URL"))),
						h("label", { className: "mp4-field" }, repoKind === "local" ? "开发机仓库绝对目录" : "仓库 HTTPS 或 SSH URL", h("input", { className: "mp4-input", value: repoSource, disabled: busy, onChange: e => setRepoSource(e.target.value) })),
						h("label", { className: "mp4-field" }, "仓库提交或引用", h("input", { className: "mp4-input", value: repoRef, disabled: busy, onChange: e => setRepoRef(e.target.value) })),
						h("div", { className: "mp4-form-actions" }, h(Btn, { strong: true, disabled: busy || !repoSource.trim(), onClick: () => run(async () => { await command("repository", JSON.stringify([repoKind, repoSource.trim(), repoRef.trim()]), "importRepository", { projectId: project.id,
							...(repoKind === "local" ? { sourcePath: repoSource.trim() } : { sourceURL: repoSource.trim() }), ref: repoRef.trim() || "HEAD",
							expected_revision: project.repository?.revision || 0 });setRepoSource(""); }) }, "导入仓库快照")))),
				error ? h("div", { className: "mp-error", role: "alert" }, error) : null);
		}

		function DeviceAccess({ project }) {
			const state = project.devices, [deviceId, setDevice] = useState(state.devices[0]?.id || ""), [rootId, setRoot] = useState(state.devices[0]?.roots[0]?.id || "");
			const [mode, setMode] = useState("read"), [duration, setDuration] = useState("3600000"), [busy, setBusy] = useState(false), [error, setError] = useState(null);
			const intents = useRef(new Map()), running = useRef(false), device = state.devices.find(d => d.id === deviceId);
			const run = async (slot, signature, op, payload) => {
				if (running.current) return;running.current = true;setBusy(true);setError(null);
				let intent = intents.current.get(slot);
				if (!intent || intent.signature !== signature) { intent = { signature, payload: { ...payload, projectId: project.id, expectedRevision: state.revision, requestId: crypto.randomUUID() } };intents.current.set(slot, intent); }
				try { await act(op, intent.payload);if (intents.current.get(slot) === intent) intents.current.delete(slot); }
				catch (e) { setError(e.message); } finally { running.current = false;setBusy(false); }
			};
			const describe = grant => grant.kind === "exec" ? "执行命令" : grant.access === "write" ? "读写文件" : "只读文件";
			return h("section", { className: "mp-library" }, h("h2", null, "设备访问"),
				h("p", { className: "mp-form-note" }, "只访问你明确授权的目录，设备文件不会自动上传到项目文件库。目录访问和执行命令分别授权，权限会按设定时间到期。"),
				state.devices.length ? h("div", { className: "mp-library-upload" },
					h("label", null, "设备", h("select", { "aria-label": "授权设备", value: deviceId, disabled: busy, onChange: e => { setDevice(e.target.value);setRoot(state.devices.find(d => d.id === e.target.value)?.roots[0]?.id || ""); } }, state.devices.map(d => h("option", { key: d.id, value: d.id }, d.name)))),
					h("label", null, "目录", h("select", { "aria-label": "授权目录", value: rootId, disabled: busy, onChange: e => setRoot(e.target.value) }, (device?.roots || []).map(r => h("option", { key: r.id, value: r.id }, r.label)))),
					h("label", null, "访问能力", h("select", { "aria-label": "设备访问能力", value: mode, disabled: busy, onChange: e => setMode(e.target.value) }, h("option", { value: "read" }, "只读文件"), h("option", { value: "write" }, "读写文件"), h("option", { value: "exec" }, "执行命令"))),
					h("label", null, "有效期", h("select", { "aria-label": "设备授权有效期", value: duration, disabled: busy, onChange: e => setDuration(e.target.value) }, h("option", { value: "600000" }, "10分钟"), h("option", { value: "3600000" }, "1小时"), h("option", { value: "28800000" }, "8小时"))),
					h("p", { className: "mp-form-note" }, mode === "exec" ? "命令在这台设备的隔离环境中运行，只挂载所选目录，默认不联网。开启执行会撤销本项目在这台设备上的目录访问权限。" : mode === "write" ? "允许助理修改所选目录中的文件。只有执行线程可以写入；协调员和记忆助理仍只读。" : "允许助理读取所选目录的文件；不能修改或执行命令。"),
					h(Btn, { strong: true, disabled: busy || !deviceId || !rootId, onClick: () => run("grant", JSON.stringify([deviceId, rootId, mode, duration]), "grantDevice", { deviceId, rootId, kind: mode === "exec" ? "exec" : "folder", ...(mode === "exec" ? {} : { access: mode }), expiresAt: Date.now() + Number(duration) }) }, "确认授权"))
				: h("p", { className: "mp-form-note" }, "还没有配置可访问的设备目录。"),
				state.grants.length ? h("div", { "aria-label": "现有设备授权" }, state.grants.map(grant => {
					const d = state.devices.find(d => d.id === grant.deviceId), root = d?.roots.find(r => r.id === grant.rootId);
					return h("div", { className: "mp-library-row", key: grant.id, "data-grant-id": grant.id }, h("strong", null, `${d?.name || grant.deviceId}，${root?.label || grant.rootId}`),
						h("span", null, describe(grant)), h("span", { className: "mp-model" }, grant.state === "active" ? `已授权，到期 ${new Date(grant.expiresAt).toLocaleString()}` : grant.state === "expired" ? "已过期" : "已撤销"),
						grant.state === "active" ? h(Btn, { quiet: true, disabled: busy, onClick: () => run("revoke-" + grant.id, grant.id, "revokeDevice", { grantId: grant.id }) }, "撤销授权") : null);
				})) : h("p", { className: "mp-form-note" }, "本项目还没有设备访问权限。"),
				error ? h("div", { className: "mp-error", role: "alert" }, error) : null);
		}

		/** Raft channel tabs: Chat and Tasks; the active one is the yellow sticker. */
		function Tabs({ tab, setTab, count, filesAvailable, devicesAvailable }) {
			const item = (id, icon, label) => h("button", { type: "button", role: "tab", "aria-selected": tab === id, className: "mp-tab" + (tab === id ? " mp-tab-on" : ""), onClick: () => setTab(id) }, h(Icon, { name: icon, size: 15 }), label);
			return h("div", { className: "mp-tabs", role: "tablist" }, item("chat", "reply", "群聊"), item("tasks", "tasks", `任务${count ? ` ${count}` : ""}`), filesAvailable ? item("files", "tasks", "文件") : null, devicesAvailable ? item("devices", "tasks", "设备") : null);
		}

		const COLUMNS = [["executing", "执行中"], ["unresolved", "未解决"], ["resolved", "已解决"]];
		/** Raft task board: one column per status, cards with "#place #n", the title and the status chip. */
		function TaskBoard({ project, onOpenThread, activeThreadId }) {
			const boardRef = useRef(null), boardVersion = project.threads.map(t => `${t.id}:${t.state}:${t.resolved}`).join("|");
			useEffect(() => {
				const board = boardRef.current, active = board?.querySelector(".mp-tcard-on");
				if (!board || !active) return;
				const outer = board.getBoundingClientRect(), card = active.getBoundingClientRect();
				board.scrollTo({ left: board.scrollLeft + card.left - outer.left - (board.clientWidth - card.width) / 2, behavior: "auto" });
				const viewport = board.closest(".mp-chat-scroll"), bounds = viewport?.getBoundingClientRect();
				if (viewport && bounds && (card.top < bounds.top || card.bottom > bounds.bottom)) viewport.scrollTop += card.top - bounds.top - 12;
			}, [activeThreadId, project.id, boardVersion]);
			const col = t => t.experience?.group || (t.state === "done" ? "resolved" : t.resolved ? "archived" : "unresolved");
			const renderCard = t => h("button", { key: t.id, type: "button", className: "mp-tcard" + (activeThreadId === t.id ? " mp-tcard-on" : ""), onClick: () => onOpenThread(t.id) },
				h("span", { className: "mp-tcard-title" }, t.title),
				h("span", { className: "mp-task-reason" }, t.experience?.reason || STATE[t.state]?.label),
				t.summary ? h("span", { className: "mp-hint mp-task-preview" }, t.summary) : null,
				h("span", { className: "mp-tcard-foot mp-hint" }, t.experience?.execution.active ? t.experience.execution.label : ago(t.updatedAt)));
			return h("div", { className: "mp-board", ref: boardRef }, COLUMNS.map(([id, label]) => {
				const items = project.threads.filter((t) => col(t) === id);
				return h("section", { key: id, className: "mp-board-col" },
					h("h3", null, label, h("span", { className: "mp-mono mp-hint" }, items.length)),
					items.length ? items.map(renderCard) : h("span", { className: "mp-hint" }, "暂无任务"));
			}), h("details", { className: "mp-board-archive" }, h("summary", null, `已归档，${project.threads.filter(t => col(t) === "archived").length}`), h("p", { className: "mp-hint" }, "归档保留记录，不代表已经解决。"), project.threads.filter(t => col(t) === "archived").map(renderCard)));
		}

		/** An agent's pending post floats above the composer: a still yellow light and a sentence. */
		// ---------------------------------------------------------------- live replies: the project chat streams like a native conversation

		/** The top-level "text" field of tool-call JSON arguments that may still be streaming; an escape cut off mid-way waits for the next chunk. */
		function partialTextField(raw) {
			const match = /[{,]\s*"text"\s*:\s*"/.exec(raw);
			if (!match) return "";
			let out = "", i = match.index + match[0].length;
			while (i < raw.length && raw[i] !== "\"") {
				if (raw[i] !== "\\") { out += raw[i++]; continue; }
				const next = raw[i + 1];
				if (next === undefined) break;
				if (next === "u") {
					const hex = raw.slice(i + 2, i + 6);
					if (hex.length < 4) break;
					out += String.fromCharCode(parseInt(hex, 16)); i += 6; continue;
				}
				out += { n: "\n", t: "\t", r: "\r", b: "\b", f: "\f" }[next] ?? next; i += 2;
			}
			return out;
		}

		/** post_message calls an assistant is generating right now, read from its DSH session feed's live chunks: the text so far and when it started. */
		function livePosts(entries) {
			const calls = new Map();
			for (const entry of entries || []) {
				const e = entry.event, chunk = entry.type === "transient" && e?.type === "assistant/live-chunk" ? e.data?.chunk : null;
				if (chunk?.type !== "tool-call-delta") continue;
				const call = calls.get(chunk.id) || { id: chunk.id, name: null, args: "", at: e.time };
				call.name ||= chunk.name || null; call.args += chunk.argumentsDelta || "";
				calls.set(chunk.id, call);
			}
			return [...calls.values()].filter(c => c.name === "post_message").map(c => ({ id: c.id, at: c.at, text: partialTextField(c.args) })).filter(c => c.text);
		}

		// ---------------------------------------------------------------- @ in DSH's own composer: projects and agents

		/**
		 * What DSH's @ menu offers in one conversation. Mira's sees every project and executor (and Mira); a project's
		 * conversation or task sees the projects and that project's executors; any other DSH session sees nothing of ours.
		 * The project assistant and a project's memory Mira are not people to call on, so they are never offered.
		 */
		function mentionCandidates(data, sessionId, query) {
			const projects = data?.projects || [], agents = data?.agents || [];
			const home = projects.find(p => p.chatSessionId === sessionId || (p.threads || []).some(t => t.sessionId === sessionId));
			if (!(sessionId && (sessionId === data?.miraSessionId || home))) return [];
			const q = String(query || "").trim().toLowerCase(), hit = (...texts) => !q || texts.some(t => String(t || "").toLowerCase().includes(q));
			// DSH shows a candidate's name after its label when they differ, so name is the label; ids ride in value.
			const projectRows = projects.filter(p => hit(p.name)).map(p => ({ name: p.name, label: p.name, description: "项目", section: "项目", value: JSON.stringify({ k: "project", id: p.id, name: p.name }) }));
			const agentRows = agents
				.filter(a => a.projectId === null && a.member === "mira" && !home)
				.filter(a => hit(a.name, a.projectName))
				.map(a => ({ name: a.name, label: a.name, description: a.projectName || "全局", section: "助理",
					value: JSON.stringify(a.projectId ? { k: "agent", projectId: a.projectId, projectName: a.projectName, member: a.member, name: a.name } : { k: "mira", name: a.name }) }));
			return [...projectRows, ...agentRows];
		}
		const readMention = value => { try { const v = JSON.parse(value); return v && typeof v.name === "string" ? v : null; } catch (_) { return null; } };
		/** The owner sees and copies a plain @name. */
		function mentionClipboard(value) { const v = readMention(value); return v ? "@" + v.name : ""; }
		/** What is sent: DSH shows it in the owner's bubble, so names only; an agent carries its project to tell same names apart. */
		function mentionText(value) {
			const v = readMention(value);
			if (!v) return "";
			return v.k === "agent" ? `@${v.name}（${v.projectName}）` : "@" + v.name;
		}

		/**
		 * Every spelling mentionText can send, longest first. A sent mention is plain text, and a name may hold spaces, so
		 * only the known names can say where one ends.
		 */
		function mentionNames(data) {
			const out = [];
			for (const p of data?.projects || []) if (p.name) out.push({ text: p.name, label: p.name, kind: "project", projectId: p.id });
			for (const a of data?.agents || []) {
				if (!a.name) continue;
				if (a.projectId === null) { if (a.member === "mira") out.push({ text: a.name, label: a.name, kind: "mira" }); }
				else if (a.role === "worker") out.push({ text: `${a.name}（${a.projectName}）`, label: a.name, kind: "agent", projectId: a.projectId, projectName: a.projectName });
			}
			return out.sort((a, b) => b.text.length - a.text.length);
		}
		const MENTION_NODE = "mira-mention";
		/**
		 * DSH's user-text rule for our mentions. DSH's own file rule ends a reference at the first space, which cut
		 * "@Mira Context 解剖" into a file chip and loose words; this one runs before it (order 20 < 30) and takes the whole name.
		 */
		function mentionRule(names) {
			const word = /[A-Za-z0-9_]/;
			return { id: MENTION_NODE, order: 20, match(text, from) {
				for (let at = text.indexOf("@", from); at !== -1; at = text.indexOf("@", at + 1)) {
					if (at > 0 && /[A-Za-z0-9_.@]/.test(text[at - 1])) continue; // an address such as a@b.c
					const hit = names.find(n => text.startsWith(n.text, at + 1) && !(word.test(n.text.at(-1)) && word.test(text[at + 1 + n.text.length] || "")));
					if (hit) return { id: MENTION_NODE, start: at, end: at + 1 + hit.text.length, data: { kind: hit.kind, label: hit.label, projectId: hit.projectId || null, projectName: hit.projectName || null } };
				}
				return undefined;
			} };
		}
		/** A sent mention in the owner's bubble: DSH's own chip. A project opens on click; an agent is a name. */
		/** Why an assistant was woken, in the owner's words, by the wake's reason. */
		const WAKE_TITLES = { spawn: "收到新任务", message: "有新消息", decision: "拍板有了答复", handoff: "交接", relay: "收到转达", configuration: "项目设置改了", thread_state: "任务状态有变化",
			conversation: "你发来消息", morning: "早上整理", evening: "晚上回顾", owner: "你让它现在整理", attention: "有事要看", no_platform_call: "提醒它回复", unanswered: "提醒它回你" };
		const WAKE_FLAGS = { 新线程: "新任务", 线程状态: "任务状态", 用户: "你的消息", 用户交还: "你交还了", 换了执行者: "换了执行方式", 决策已答复: "拍板有答复", "有 @": "提到它" };
		const TRIGGER_TITLES = { goal: "继续执行目标", "agent-message": "收到任务消息", "team-message": "收到团队消息", "subagent-settled": "子任务有更新", webhook: "收到外部事件", schedule: "自动化任务", "tool-jobs": "后台任务有更新", "cordis-host-runner": "插件状态更新" };
		const WAKE_ICONS = { spawn: "tasks", message: "reply", conversation: "reply", unanswered: "reply", no_platform_call: "reply", decision: "decide", morning: "clock", evening: "clock" };
		function triggerText(content) {
			if (typeof content === "string") return content;
			if (Array.isArray(content)) return content.map(b => typeof b === "string" ? b : b && typeof b.text === "string" ? b.text : "").join("");
			return content && typeof content.text === "string" ? content.text : "";
		}
		/** A <mira-wake> notice read for people: a title from its reason, and what it counted (never its instructions). */
		function readWake(text) {
			const m = /^\s*<mira-wake\b([^>]*)>([\s\S]*?)(?:<\/mira-wake>|$)/.exec(text || "");
			if (!m) return null;
			const attrs = {};
			for (const a of m[1].matchAll(/([a-z_]+)="([^"]*)"/g)) attrs[a[1]] = a[2];
			const reason = attrs.reason || "", parts = [];
			for (const line of m[2].split("\n").map(l => l.trim())) {
				let x;
				if ((x = /^- (.+?)：(\d+) 条(?:（(.+)）)?$/.exec(line))) {
					const where = x[1].replace(/^线程(「.*」)$/, "$1");
					const flags = (x[3] ? x[3].split("、") : []).filter(f => !(reason === "spawn" && f === "新线程")).map(f => WAKE_FLAGS[f] || f);
					parts.push(flags.length ? `${where} ${flags.join("、")}` : reason === "spawn" ? where : `${where} ${x[2]} 条`);
				} else if ((x = /^(?:项目里有 \d+ 条新动态|新动态 \d+ 条)：(.+?)。/.exec(line))) parts.push(x[1]);
				else if ((x = /用户附言：(.+)$/.exec(line))) parts.push(`附言：${x[1]}`);
			}
			const title = attrs.request === "morning" ? "早上整理" : WAKE_TITLES[reason] || "有新动态";
			// How many of the owner's own messages it brings: "个人对话 N" for the assistant, "：N 条（…你的消息…）" in a task.
			const chats = /个人对话 (\d+)/.exec(m[2]), owned = [...m[2].matchAll(/：(\d+) 条（[^）]*用户[^）]*）/g)].reduce((n, x) => n + Number(x[1]), 0);
			return { reason, title, detail: parts.join("，"), icon: WAKE_ICONS[reason] || "inbox", project: attrs.project || "", request: attrs.request || "", owner: chats ? Number(chats[1]) : owned, said: attrs.said || "" };
		}
		/**
		 * The owner's own messages a wake brought: exactly the ones it names (`said`, message ids); a wake from before ids
		 * were recorded gets the last `count` of theirs from the ten minutes before it, else nothing. A page's prefix such as
		 * where they were looking is dropped; their own typed words are kept.
		 */
		function wakeMessages(messages, at, count, said) {
			// Words typed straight into the session are already on screen there; only what came from elsewhere is shown again.
			const human = (messages || []).filter(m => m && m.author && m.author.kind === "human" && !m.native);
			const ids = String(said || "").split(",").filter(Boolean);
			const own = ids.length ? ids.map(id => human.find(m => m.id === id)).filter(Boolean)
				: count ? human.filter(m => typeof m.at === "number" && m.at <= at + 3000 && m.at >= at - 10 * 60e3).sort((a, b) => a.at - b.at).slice(-count) : [];
			return own.map(m => ({ id: m.id, text: (m.typed || String(m.text || "").replace(/^（我正在看[^）]*）/, "")).trim(), from: m.relay ? m.from : null })).filter(m => m.text);
		}
		/** Whether a wake was brought by words the owner typed straight into the session (already on screen there). */
		function typedInSession(messages, at, said) {
			const human = (messages || []).filter(m => m && m.author && m.author.kind === "human");
			const ids = String(said || "").split(",").filter(Boolean);
			if (ids.length) { const found = ids.map(id => human.find(m => m.id === id)); return found.every(m => m && m.native); }
			return human.some(m => m.native && typeof m.at === "number" && m.at <= at + 3000 && m.at >= at - 10 * 60e3);
		}
		const RECAPS = { morning: "早上整理", evening: "晚上回顾" };
		/**
		 * In place of DSH's boxed "收到执行请求", as Codex shows a prompt that came from elsewhere: what the owner said is
		 * shown as their own message with where it came from; a scheduled recap is a quiet divider; the platform's own
		 * reminders to the assistant are not shown; anything else is one plain line. The raw prompt stays on the developer page.
		 */
		/** Hides the other rows of the turn it sits in (steps, tool calls, the 「已完成，用时」 line, the tail), in its own conversation only. */
		function QuietTurn({ keepTrigger }) {
			const [turn, setTurn] = useState("");
			const ref = useCallback(el => { const t = el && el.closest("[data-chat-turn]")?.getAttribute("data-chat-turn"); if (t && /^\d+$/.test(t)) setTurn(t); }, []);
			const rule = turn ? `[data-chat-flow]:has([data-mp4-quiet="${turn}"]) [data-chat-turn="${turn}"]${keepTrigger ? ':not([data-chat-flow-kind="turn-trigger"])' : ""}{display:none}` : "";
			return h("span", { ref, hidden: true, "data-mp4-quiet": turn }, rule ? h("style", null, rule) : null);
		}
		function MiraTrigger({ node }) {
			const d = (node && node.data) || {}, personal = usePersonal(), onScreen = useOnScreenSession(), task = useTaskOf(onScreen);
			const raw = triggerText(d.content), wake = readWake(raw), kind = d.source && d.source.kind, at = d.time ? new Date(d.time).getTime() : Date.now();
			const line = (icon, title, detail) => h("section", { className: "mp4-trig", "data-turn-trigger": true },
				h("div", { className: "mp4-trig-row" }, h(Icon, { name: icon, size: 13 }), h("span", { className: "mp4-trig-title" }, title),
					detail ? h("span", { className: "mp4-trig-detail" }, detail) : null,
					d.time ? h("time", { className: "mp4-trig-time", dateTime: new Date(d.time).toISOString() }, clock(d.time)) : null));
			if (!wake) return line("inbox", TRIGGER_TITLES[kind] || "收到执行请求");
			if (wake.reason === "no_platform_call" || wake.reason === "unanswered") return null;
			const bubble = (key, caption, text) => h("div", { key, className: "mp4-trig-said" }, caption ? h("span", { className: "mp4-trig-from" }, h(Icon, { name: "reply", size: 12 }), caption) : null, h("div", { className: "mp4-trig-bubble" }, h(RichText, { text, members: [] })));
			const recap = RECAPS[wake.request] || RECAPS[wake.reason];
			const divider = recap ? h("div", { key: "recap", className: "mp4-trig-divider" }, h("span", null, `${recap} ${d.time ? clock(d.time) : ""}`.trim())) : null;
			let said = [];
			if (wake.project === "总管") said = wakeMessages(personal.messages, at, wake.owner, wake.said).map(m => bubble(m.id, null, m.text));
			else if (task.thread && wake.reason === "spawn" && task.thread.brief) said = [bubble("brief", "项目助理派来的任务", task.thread.brief)];
			else if (task.thread && (wake.owner || wake.said)) said = wakeMessages((task.thread.timeline || []).filter(r => r.kind === "message"), at, wake.owner, wake.said).map(m => bubble(m.id, "你在项目页发的", m.text));
			// The assistant's own background turn (a recap, a look over the projects): what it did lands as cards and recaps,
			// and words it wrote there were never meant for the owner. Only the recap divider stays; the rest of the turn is hidden.
			if (wake.project === "总管" && wake.reason !== "conversation" && !said.length) return h("section", { className: "mp4-trig", "data-turn-trigger": true }, divider, h(QuietTurn, { keepTrigger: !!divider }));
			if (divider || said.length) return h("section", { className: "mp4-trig", "data-turn-trigger": true }, divider, said);
			// What the owner typed right here is already on screen as their own message: nothing to add.
			// Its turn only repeats what was answered right there (the platform stops a second answer): hide the whole turn.
			if (wake.project === "总管" && wake.reason === "conversation" && typedInSession(personal.messages, at, wake.said)) return h(QuietTurn, { keepTrigger: false });
			return line(wake.icon, wake.title, wake.detail);
		}

		function MentionChip({ node, raw, interactive }) {
			const d = node.data || {}, Chip = P.ReferenceChip;
			if (!Chip) return h("span", { className: "mp-mention" }, raw);
			if (d.kind === "project") return h(Chip, { appearance: "folder", label: d.label, title: `项目，${d.label}`, ...(interactive && d.projectId ? { onActivate: () => { openProject(d.projectId); } } : {}) });
			return h(Chip, { label: "@" + d.label, title: d.projectName ? `${d.label}，${d.projectName}` : d.label });
		}

		/** Projects, agents and Mira's session, fetched for the @ menu and kept for a few seconds. */
		let mentionCache = null, onMentionData = null;
		async function mentionData(signal) {
			if (mentionCache && Date.now() - mentionCache.at < 5000) return mentionCache.data;
			const get = path => fetch("api/mira-projects/" + path, { credentials: "same-origin", cache: "no-store", signal }).then(r => r.ok ? r.json() : null).catch(() => null);
			const [state, models, personal] = await Promise.all([get("state"), get("models"), get("personal")]);
			const data = { projects: state?.projects || [], agents: models?.agents || [], miraSessionId: personal?.sessionId || null };
			mentionCache = { at: Date.now(), data };
			if (state && models) onMentionData?.(data);
			return data;
		}

		/**
		 * E2E #6: DSH keeps one blank conversation per folder and its own @ source lists it by its raw id
		 * (「session-658967df-…」). A conversation without a title tells the owner nothing, so it is not offered.
		 * The host's source is wrapped once; its rows and everything else pass through untouched.
		 */
		let mentionTriggers = null;
		const RAW_SESSION_ID = /^session-[0-9a-f]{8}-[0-9a-f-]{20,}$/i;
		// Also left out: the conversations this plugin keeps for its own plumbing, named by the plugin itself (the memory assistant's, the timer courier's).
		const OWN_PLUMBING = /(的记忆助理| · 定时投递)$/;
		// The host writes a conversation's folder under its name. For one that works in this plugin's own storage that is a
		// data path nobody chose (the personal assistant's showed up that way in a project's @ menu): only its age is kept.
		const OWN_STORAGE = /\/(?:_hub|_folders)\/|\/p_[0-9a-f]{8,}\//;
		const withoutDataPath = r => r && r.icon === "session" && typeof r.description === "string" && OWN_STORAGE.test(r.description)
			? { ...r, description: r.description.includes(" · ") ? r.description.slice(r.description.lastIndexOf(" · ") + 3) : "" } : r;
		const titledOnly = rows => Array.isArray(rows) ? rows.filter(r => !(r && r.icon === "session" && (RAW_SESSION_ID.test(String(r.name || "")) || OWN_PLUMBING.test(String(r.name || ""))))).map(withoutDataPath) : rows;
		function hideUntitledSessions(triggers) {
			try {
				const source = triggers?.live?.sources?.find(s => s.trigger === "@" && s.name === "reference");
				if (!source || source.miraTitledOnly) return;
				const original = source.candidates;
				source.candidates = async function (session, req) { return titledOnly(await original.call(this, session, req)); };
				source.miraTitledOnly = true;
			} catch (_e) { /* the host changed shape: its list stays as it is */ }
		}
		/** DSH's input-trigger source: an "@" group of our projects and agents, inserted as DSH's own reference chips. */
		const MENTION_SOURCE = {
			trigger: "@", name: "mira-projects", order: -10, showGroupTitle: false,
			async candidates(session, req) { hideUntitledSessions(mentionTriggers); return mentionCandidates(await mentionData(req.signal), session.sessionId, req.query); },
			onPick(pick) {
				const value = pick.candidate.value, picked = readMention(value);
				return value ? { insert: { source: "mira-projects", ref: value, label: picked?.name || pick.candidate.label || "", clipboardText: mentionClipboard(value), ...(picked?.k === "project" ? { appearance: "folder" } : {}) } } : undefined;
			},
			codec: { clipboardText: mentionClipboard, async serialize(ref) { return mentionText(ref) || mentionClipboard(ref); } },
		};

		/** Codex's pacing: chunks arrive in bursts, so let out about 24 characters per frame and the text grows evenly; a hidden page shows it all at once. */
		function usePacedText(text) {
			const shown = useRef(text.length), [, redraw] = useState(0);
			useEffect(() => {
				shown.current = Math.min(shown.current, text.length);
				if (shown.current === text.length) return;
				if (document.hidden) { shown.current = text.length; redraw(n => n + 1); return; }
				let frame, last = performance.now();
				const tick = now => {
					shown.current = Math.min(text.length, shown.current + Math.max(1, Math.round(24 * Math.min(now - last, 500) / 16.7)));
					last = now; redraw(n => n + 1);
					if (shown.current < text.length) frame = requestAnimationFrame(tick);
				};
				frame = requestAnimationFrame(tick);
				return () => cancelAnimationFrame(frame);
			}, [text]);
			return text.slice(0, shown.current);
		}

		/** A DSH session's client feed (durable events plus live chunks), held only while `active`. */
		function useSessionFeed(sessionId, active) {
			const [source, setSource] = useState(null);
			useEffect(() => {
				if (!active || !sessionId || !shell.sessions) return;
				let alive = true, reference;
				try { reference = shell.sessions.retain(sessionId, { source: "sidebarView" }); } catch (_) { return; }
				reference.ready.then(binding => { if (alive) setSource(binding.eventSource); }, () => {});
				return () => { alive = false; setSource(null); reference.release(); };
			}, [sessionId, active]);
			const subscribe = useCallback(fn => source ? source.subscribe(fn) : () => {}, [source]);
			return useSyncExternalStore(subscribe, () => { try { return source ? source.getSnapshot() : null; } catch (_) { return null; } });
		}

		/** What the coordinator or the project's Mira is writing into the chat right now, until the posted message arrives. */
		function LiveReply({ project, memberId, assistant }) {
			const snapshot = useSessionFeed(assistant?.sessionId, Boolean(assistant?.status && assistant.status !== "idle"));
			const member = project.members.find(m => m.id === memberId);
			const posts = livePosts(snapshot?.entries).filter(p => !project.chat.some(m => m.author?.id === memberId && m.at >= p.at));
			if (!member || !posts.length) return null;
			return posts.map(p => h(LivePost, { key: p.id, member, text: p.text, project }));
		}

		function LivePost({ member, text, project }) {
			const shown = usePacedText(text), row = useRef(null), height = useRef(null);
			// Follow the growing text only when the reader was at the bottom before it grew.
			useLayoutEffect(() => {
				const el = row.current?.closest(".mp4-scroll");
				if (!el) return;
				const before = height.current ?? el.scrollHeight - row.current.offsetHeight;
				if (before - el.scrollTop - el.clientHeight < 80) el.scrollTop = el.scrollHeight;
				height.current = el.scrollHeight;
			});
			return h("div", { className: "mp-turn", ref: row }, h("div", { className: "mp-msg" }, h("div", { className: "mp-msg-main" },
				h("div", { className: "mp-msg-head" }, h("b", null, shownName(member)), h("span", { className: "mp-model" }, "正在输入")),
				h("div", { className: "mp-msg-text" }, h(RichText, { text: shown, members: project.members })))));
		}

		function WorkingLine({ project, onOpenThread }) {
			const busy = workingLine(project);
			if (!busy.length) return h("div", { className: "mp-pending" });
			const one = busy.length === 1 ? busy[0] : null;
			const text = one
				? `${one.name || one.who.name}${one.text}${one.where ? `，${one.where}` : ""}`
				: `${busy.map((b) => b.name || b.who.name).join("、")} 在工作`;
			const target = busy.find((b) => b.threadId);
			return h("div", { className: "mp-pending mp-pending-on", role: "status" },
				h("span", { className: "mp-spin", "aria-hidden": true }),
				h("span", { className: "mp-ellipsis" }, text),
				one && one.step ? h("span", { className: "mp-mono mp-hint mp-pending-time" }, `上一步 ${ago(one.step)}`) : null,
				target && onOpenThread ? h("button", { type: "button", className: "mp-pending-open", onClick: () => onOpenThread(target.threadId) }, "查看") : null);
		}

		function ChatPane({ project, projects, onSelect, onCreate, activeThreadId, onOpenThread, chatEpoch, navigationToggle, onAnswer, openSession, onReview, prefill, narrow, visible }) {
			const [settingsOpen, setSettingsOpen] = useState(false);
			const [automationsOpen, setAutomationsOpen] = useState(false);
			const [tab, setTab] = useState("chat");
			const reading = useReadingPosition(visible && tab === "chat", project.id, project.chat.length);
			useEffect(() => { setTab("chat"); setSettingsOpen(false); setAutomationsOpen(false); }, [chatEpoch]);
			const openCount = project.threads.filter((t) => !t.resolved).length;
			return h("section", { className: "mp-chat" },
				h(Header, { project, projects, onSelect, onCreate, narrow, navigationToggle, onSettings: () => { setSettingsOpen(open => !open);setAutomationsOpen(false); }, onAutomations: () => { setAutomationsOpen(open => !open);setSettingsOpen(false); } }),
				settingsOpen ? h(ProjectSettings, { key: project.id, project, onClose: () => setSettingsOpen(false) }) : null,
				automationsOpen ? h(AutomationPanel, { key: project.id, project, onClose: () => setAutomationsOpen(false) }) : null,
				project.workspace?.reason ? h("div", { className: "mp-notice" }, `本项目使用共享目录：${project.workspace.reason}`) : null,
				h(Tabs, { tab, setTab, count: openCount, filesAvailable: Array.isArray(project.files), devicesAvailable: Boolean(project.devices) }),
				tab === "files" ? h("div", { className: "mp-chat-scroll" }, h(SharedFiles, { key: project.id, project })) : null,
				tab === "devices" && project.devices ? h("div", { className: "mp-chat-scroll" }, h(DeviceAccess, { key: project.id, project })) : null,
				tab === "tasks" ? h("div", { className: "mp-chat-scroll" }, h(TaskBoard, { project, onOpenThread: id => onOpenThread(id, { showProject: true }), activeThreadId })) : null,
				tab !== "chat" ? null : h("div", {
					className: "mp-chat-scroll", ...reading,
				},
					h("div", { className: "mp-column" },
						h(SystemLine, null, "消息从这里开始"),
						project.chat.length === 0 ? h("div", { className: "mp-empty-chat" },
							h("b", null, project.name),
							h("p", null, "在这里讨论需求、方向和整体进展。需要动手的事会成为独立线程，后续可以在线程里持续交流。")) : null,
						h(Timeline, { rows: project.chat, project, onOpenThread, activeThreadId, onAnswer, showThreadLabel: true }))),
				tab !== "chat" ? null : h("div", { className: "mp-column mp-composer-wrap" },
					h(WorkingLine, { project, onOpenThread }),
					h(Composer, {
						members: project.members, project,
						placeholder: `发到 #${project.name}`,
						onSend: (text) => act("post", { projectId: project.id, text, typed: text }),
						prefill: prefill && prefill.projectId === project.id && prefill.target === "chat" ? prefill : null,
						draftKey: `${project.id}:chat`,
					})));
		}

		// ------------------------------------------------------------------ review (memory + cards)

		function MemorySection({ project, section, label }) {
			const items = project.memory[section] || [];
			const forget = async (index) => { try { await act("memoryForget", { projectId: project.id, section, index }); } catch (_e) { /* shown by the next snapshot */ } };
			return h("section", { className: "mp-mem" },
				h("h3", null, label, h("span", { className: "mp-mono mp-hint" }, items.length)),
				items.length === 0 ? h("p", { className: "mp-hint" }, section === "facts" ? "还没有。你在项目对话里说的约定、偏好，记忆助理会先递一张“记一下”卡让你确认。" : "还没有。") : null,
				items.map((item, i) => h("div", { key: i, className: "mp-mem-item" },
					h("span", { className: "mp-mem-text" }, item.text),
					h("span", { className: "mp-mem-ev mp-mono" }, (item.evidence || []).slice(0, 4).map((s) => `#${s}`).join(" ")),
					h("button", { type: "button", className: "mp-x", title: "忘掉这条", "aria-label": "忘掉这条", onClick: () => forget(i) }, h(Icon, { name: "close", size: 12 })))));
		}

		function ReviewView({ project, onJump, onBack, narrow, navigationToggle }) {
			const [busy, setBusy] = useState(null);
			const run = async (reason) => {
				setBusy(reason);
				try { await act("miraRun", { projectId: project.id, reason }); } catch (_e) { /* status shows it */ } finally { setTimeout(() => setBusy(null), 1500); }
			};
			const cards = project.cards.map((c) => ({ ...c, projectId: project.id, projectName: project.name }));
			const open = cards.filter((c) => c.state === "open").reverse();
			const closed = cards.filter((c) => c.state !== "open").reverse();
			const mira = project.mira;
			const miraMember = project.members.find((m) => m.id === "mira") || { kind: "assistant", id: "mira", name: "记忆助理", role: "memory" };
			const presence = presenceOf(mira.status, mira.lastTurn, mira.sessionId);
			return h("section", { className: "mp-chat mp-review" },
				h("header", { className: "mp-head" },
					navigationToggle,
					narrow ? h(IconButton, { name: "back", label: "返回群聊", onClick: onBack }) : h("span", { className: "mp-head-tile" }, h(Icon, { name: "spark", size: 14 })),
					h("div", { className: "mp-head-text" },
						h("h1", { className: "mp-head-title" }, "回顾与卡片"),
						h("div", { className: "mp-head-meta" },
							mira.status === "running" ? h("span", { className: "mp-head-working" }, h(Light, { tone: "yellow", size: "sm" }), `记忆助理${activityText("running", mira.activity, mira.lastTurn)}，`) : null,
							`${project.name}，记忆${project.memory.updatedAt ? `更新于 ${ago(project.memory.updatedAt)}` : "还没整理过"}${mira.pending ? `，${mira.pending} 条动态没看` : ""}`)),
					h("div", { className: "mp-head-actions" },
						h(Avatar, { author: miraMember, project, size: "sm", presence }),
						h(Btn, { onClick: () => run("owner"), disabled: busy !== null || mira.status === "running" }, busy === "owner" ? "已请求" : "现在整理"),
						h(Btn, { quiet: true, onClick: () => run("morning"), disabled: busy !== null || mira.status === "running", title: "平时每天第一次打开项目时自动生成" }, busy === "morning" ? "已请求" : recapLabel(Date.now())))),
				h("div", { className: "mp-chat-scroll" },
					h("div", { className: "mp-review-grid" },
						h("div", { className: "mp-review-col" },
							h("h2", null, "记忆", h("span", { className: "mp-hint" }, "，每条都标了来自哪条动态，点 × 可以忘掉")),
							MEMORY.map(([section, label]) => h(MemorySection, { key: section, project, section, label }))),
						h("div", { className: "mp-review-col" },
							h("h2", null, "递给你的卡", h("span", { className: "mp-hint" }, `，一天最多 3 张，${open.length} 张待处理`)),
							open.length === 0 ? h("p", { className: "mp-hint" }, "现在没有。线程待验收、有人等你拍板、你说了值得记的话，记忆助理会递卡。") : null,
							open.map((c) => h(MiraCard, { key: c.id, card: c, onJump, project })),
							closed.length ? h("details", { className: "mp-closed" },
								h("summary", null, `处理过的 ${closed.length} 张`),
								closed.map((c) => h(MiraCard, { key: c.id, card: c, onJump, compact: true, project }))) : null))));
		}

		// ------------------------------------------------------------------ page

		function readRoute() {
			const parts = window.location.hash.slice(1).split("/");
			if (parts[0] !== "mira-projects" || !parts[1]) return null;
			try { return { projectId: decodeURIComponent(parts[1]), threadId: parts[2] === "threads" && parts[3] ? decodeURIComponent(parts[3]) : undefined }; } catch (_) { return null; }
		}
		function navigateProject(projectId, threadId) {
			const hash = `#mira-projects/${encodeURIComponent(projectId)}` + (threadId ? `/threads/${encodeURIComponent(threadId)}` : "");
			if (window.location.hash !== hash) window.history.pushState({ ...window.history.state, miraProjectsPanel: PANEL_ID }, "", hash);
			writePref("project", projectId);
			window.dispatchEvent(new CustomEvent("mira-projects:navigate"));
		}
		const isHomeRoute = () => window.location.hash.split("?")[0] === "#mira-home";
		function navigateHome(sourceRef) {
			const hash = "#mira-home" + (sourceRef ? "?source=" + encodeURIComponent(sourceRef) : "");
			if (window.location.hash !== hash) window.history.pushState({ ...window.history.state, miraProjectsPanel: HOME_ID }, "", hash);
			window.dispatchEvent(new CustomEvent("mira-personal:navigate"));
		}

		function usePersonalSnapshot() {
			return useServerSnapshot("api/mira-projects/personal", { loaded: false, messages: [], projects: [], handoffs: [], activity: [] });
		}

		/** Persist an unresolved request identity across reloads; the server checks payload reuse. */
		const pendingPersonalCommands = new Map();
		async function personalCommand(op, payload, seat) {
			const key = `mira-projects:pending:${seat}`, body = JSON.stringify({ op, ...payload });
			let saved = pendingPersonalCommands.get(key); try { saved ||= JSON.parse(window.sessionStorage.getItem(key)); } catch (_) {}
			if (!saved || saved.body !== body) saved = { body, requestId: crypto.randomUUID() };
			pendingPersonalCommands.set(key, saved);
			try { window.sessionStorage.setItem(key, JSON.stringify(saved)); } catch (_) {}
			const result = await act(op, { ...payload, requestId: saved.requestId });
			pendingPersonalCommands.delete(key);
			try { window.sessionStorage.removeItem(key); } catch (_) {}
			return result;
		}

		function SourceViewer({ sourceRef, revision, now, onClose, onStep, neighbors = [] }) {
			const dialog = useRef(null), [source, setSource] = useState(null), [error, setError] = useState(null);
			const deadlineReached = source?.sourceState !== "expired" && Number.isFinite(source?.sourceExpiresAt) && Number.isFinite(now) && now >= source.sourceExpiresAt;
			useEffect(() => {
				const previous = document.activeElement; dialog.current?.showModal();
				return () => { if (previous?.isConnected) previous.focus(); };
			}, []);
			useEffect(() => {
				const abort = new AbortController();
				fetch("api/mira-projects/personal/source?ref=" + encodeURIComponent(sourceRef), { credentials: "same-origin", cache: "no-store", signal: abort.signal }).then(async r => { const d = await r.json(); if (!r.ok) throw Error(d.error); return d; }).then(d => { if (!abort.signal.aborted) { setSource(d);setError(null); } }, e => { if (!abort.signal.aborted) setError(e.message); });
				return () => abort.abort();
			}, [sourceRef, revision, deadlineReached]);
			return h("dialog", { ref: dialog, className: "mp-trace-dialog mp-source-dialog", "aria-label": "原始资料", onCancel: e => { e.preventDefault(); onClose(); } },
				h("header", { className: "mp-trace-header" }, h("strong", null, "原始资料"), h(IconButton, { name: "close", label: "关闭原始资料", onClick: onClose })),
				h("div", { className: "mp-source-body" }, error ? h("p", { role: "alert", className: "mp-error" }, error) : !source ? h("p", { role: "status" }, "正在读取原记录…") : h(React.Fragment, null,
					h("div", { className: "mp-source-meta" }, source.projectName || source.connectorName || source.deviceName || "个人记录", "，", deadlineReached ? "原文已到保留期" : source.title, "，", ({ observation: "设备观察", external: "外部资料", principal: "用户原话", relay: "助理记录", system: "项目事件" })[source.trust]),
					h("p", { className: "mp-hint" }, "发生于 ", new Date(source.happenedAt).toLocaleString(), source.happenedAt !== source.receivedAt ? `，收到于 ${new Date(source.receivedAt).toLocaleString()}` : ""),
					(source.sourceState === "expired" || deadlineReached) ? h("p", { className: "mp-hint", role: "status" }, (source.sourceCleanupPending || deadlineReached ? "原文已到保留期，清理尚未完成；暂不展示正文" : "原文已按保留设置清理"), source.sourceExpiredAt ? `，${new Date(source.sourceExpiredAt).toLocaleString()}` : "", "。引用编号和时间仍保留；延长保留期不会恢复已清理的文字。") : null,
					!deadlineReached && source.externalContext ? h("p", { className: "mp-hint" }, source.externalContext) : null, source.sourceState === "deleted" ? h("p", { className: "mp-hint" }, "来源后来已删除此记录；这里保留当时引用的快照。") : source.sourceState === "historical" ? h("p", { className: "mp-hint" }, "这是当时引用的版本，来源后来已有修改。") : null, h(RichText, { text: deadlineReached ? "" : source.text, members: [] }), source.sourceTruncated ? h("p", { className: "mp-hint" }, "来源正文较长，同步时保留了前20000字。") : null, source.truncated ? h("p", { className: "mp-hint" }, "资料较长，这里显示前20000字。") : null,
					source.media ? h("figure", { className: "mp-observation-picture" }, h("img", { src: source.media.url, alt: `${source.deviceName || "设备"}在${new Date(source.happenedAt).toLocaleString()}的关键画面`, loading: "lazy" }), h("figcaption", null, "保留的关键画面，期间未保存的画面为空缺")) : source.kind === "observation.accepted" ? h("p", { className: "mp-hint" }, source.mediaState === "expired" ? "这张关键图片已清理或超过保留期。" + (source.sourceState === "expired" ? "" : "活动文字仍保留。") : source.mediaState === "images_disabled" ? "当时未开启图片上传。" : "本条未保存关键图片。") : null,
					source.clockSkewMs ? h("p", { className: "mp-hint" }, "设备时钟存在偏差，所示发生时间来自设备；可对照收到时间核对。") : null,
					source.sourceRefs?.length && onStep ? h("details", { className: "mp-observation-source-refs" }, h("summary", null, `原始观察，${source.sourceRefs.length} 条`), h("div", { className: "mp-personal-message-actions" }, source.sourceRefs.map((ref, index) => h(Btn, { key: ref, onClick: () => onStep(ref) }, `观察 ${index + 1}`)))) : null,
					source.supersededBy && onStep ? h("p", { className: "mp-hint" }, "本摘要已补充迟到资料。", h(Btn, { onClick: () => onStep(source.supersededBy) }, "查看较新摘要")) : null,
					onStep && neighbors.includes(sourceRef) && neighbors.length > 1 ? h("nav", { className: "mp-source-actions", "aria-label": "关键画面切换" }, h(Btn, { disabled: neighbors.indexOf(sourceRef) <= 0, onClick: () => onStep(neighbors[neighbors.indexOf(sourceRef) - 1]) }, "较新一条"), h("span", { className: "mp-hint" }, `${neighbors.indexOf(sourceRef) + 1} / ${neighbors.length}`), h(Btn, { disabled: neighbors.indexOf(sourceRef) >= neighbors.length - 1, onClick: () => onStep(neighbors[neighbors.indexOf(sourceRef) + 1]) }, "较早一条")) : null,
					h("div", { className: "mp-source-actions" }, h("small", { className: "mp-hint" }, source.ref), source.projectId ? h(Btn, { onClick: () => navigateProject(source.projectId, source.threadId) }, source.threadId ? "进入所属线程" : "进入所属项目") : null))));
		}

		const observationSourceLabel = { screen: "屏幕关键帧", activity: "设备活动", accessibility: "应用活动", agent: "助理记录", docs: "文档记录", lark: "飞书资料", email: "邮件资料", summary: "窗口摘要" };
		const byteLabel = value => value >= 1024 * 1024 ? (value / 1024 / 1024).toFixed(1) + " MB" : (value / 1024).toFixed(1) + " KB";
		function ObservationDevice({ device, onRefresh }) {
			const policyOf = p => Object.fromEntries(Object.entries(p).filter(([key]) => key !== "revision"));
			const [policy, setPolicy] = useState(() => policyOf(device.policy)), [revision, setRevision] = useState(device.policy.revision), [dirty, setDirty] = useState(false), [busy, setBusy] = useState(false), [error, setError] = useState(null), [notice, setNotice] = useState(null);
			useEffect(() => { if (!dirty) { setPolicy(policyOf(device.policy));setRevision(device.policy.revision); } }, [device.policy.revision]);
			const change = (key, value) => { setPolicy(old => ({ ...old, [key]: value }));setDirty(true);setNotice(null); };
			const save = async event => { event.preventDefault();setBusy(true);setError(null);setNotice(null);try { const result = await personalCommand("observationConfigure", { deviceId: device.id, policy, expectedRevision: revision }, "observation-policy:" + device.id);setRevision(result.revision);setDirty(false);setNotice("同步设置已保存，客户端下次连接时生效。");onRefresh(); } catch (e) { setError(e.message); } finally { setBusy(false); } };
			const clear = async () => { setBusy(true);setError(null);setNotice(null);try { await personalCommand("observationClearMedia", { deviceId: device.id, expectedRevision: device.policy.revision, through: device.clearThrough }, "observation-clear:" + device.id + ":" + device.images);setNotice("关键图片已清理，活动文字保留。");onRefresh(); } catch (e) { setError(e.message); } finally { setBusy(false); } };
			return h("section", { className: "mp-observation-device", "aria-label": "同步设备：" + device.name },
				h("div", { className: "mp-observation-device-head" }, h("strong", null, device.name), h("span", { className: "mp-pill" }, device.connection === "recent" ? "最近有上报" : "等待设备连接")),
				h("p", { className: "mp-hint" }, "上次收到上报：", new Date(device.receivedAt).toLocaleString(), "，状态按最近上报判断"),
				h("div", { className: "mp-observation-metrics" }, h("span", null, "待发 ", device.report.queueEvents, " 条，", byteLabel(device.report.queueBytes), "，待重试 ", device.report.retryEvents || 0, " 条"), h("span", null, "关键图片 ", device.images, " 张，", byteLabel(device.retainedBytes)), h("span", null, "已选记录 ", device.report.captured, "，过滤 ", device.report.filtered), h("span", null, "客户端尝试正文 ", byteLabel(device.report.networkBytes), "（含重试）"), h("span", null, "网关收到/返回正文 ", byteLabel(device.report.gatewayReceivedBytes || 0), " / ", byteLabel(device.report.gatewaySentBytes || 0))),
				device.notificationPending ? h("p", { className: "mp-hint", role: "status" }, "重要信号已保存，整理通知等待重试。") : null,
				device.report.lastError ? h("p", { className: "mp-error", role: "status" }, device.report.lastError) : null,
				h("form", { onSubmit: save, className: "mp-observation-policy", "aria-label": "同步设置：" + device.name },
					h("label", { className: "mp-observation-check" }, h("input", { type: "checkbox", checked: policy.enabled, disabled: busy, onChange: e => change("enabled", e.target.checked) }), "同步此设备的观察资料"),
					h("label", { className: "mp-observation-check" }, h("input", { type: "checkbox", checked: policy.uploadImages, disabled: busy, onChange: e => change("uploadImages", e.target.checked) }), "上传选出的关键图片"),
					h("div", { className: "mp-observation-policy-grid" }, [["sampleSeconds", "采样间隔（秒）", 2, 3600], ["uploadSeconds", "上传间隔（秒）", 1, 3600], ["summaryMinutes", "摘要窗口（分钟）", 1, 120], ["lateMinutes", "迟到等待（分钟）", 7, 60], ["imageRetentionDays", "新收到图片保留（天）", 1, 365]].map(([key, label, min, max]) => h("label", { key }, label, h("input", { type: "number", min, max, value: policy[key], disabled: busy, onChange: e => change(key, Number(e.target.value)) }))),
						h("label", null, "重要信号整理间隔（分钟，0关闭）", h("input", { type: "number", min: 0, max: 1440, value: policy.importantWakeMinutes, disabled: busy, onChange: e => change("importantWakeMinutes", Number(e.target.value)) })),
						h("label", null, "待发正文与图片限额（MB）", h("input", { type: "number", min: 4, max: 2048, value: policy.maxQueueBytes / 1024 / 1024, disabled: busy, onChange: e => change("maxQueueBytes", Number(e.target.value) * 1024 * 1024) }))),
					h("label", { className: "mp-observation-exclude" }, "排除应用（每行一个名称）", h("textarea", { rows: 3, value: policy.excludeApps.join("\n"), disabled: busy, onChange: e => change("excludeApps", e.target.value.split("\n").filter(line => line.trim()).map(line => line.trim())) })),
					h("p", { className: "mp-hint" }, "暂停同步会保留已有待发资料；恢复时不补采暂停期间的上游记录。采样、上传、摘要分别配置；普通活动上传不唤醒模型。图片保留期从收到时计算，新设置用于之后收到的图片。应用排除生效时，无法识别应用来源的旧摘要也会过滤。"),
					revision !== device.policy.revision ? h("p", { className: "mp-hint" }, "服务器设置已变化。", h(Btn, { disabled: busy, onClick: () => { setPolicy(policyOf(device.policy));setRevision(device.policy.revision);setDirty(false);setError(null); } }, "载入最新设置")) : null,
					h("div", { className: "mp-observation-device-actions" }, h("button", { type: "submit", className: "mp-btn mp-btn-strong", disabled: busy || !dirty }, busy ? "正在保存…" : "保存同步设置"), h(Btn, { disabled: busy || !device.images, onClick: clear }, `清理已保存的 ${device.images} 张关键图片`))),
				error ? h("p", { className: "mp-error", role: "alert" }, error) : null, notice ? h("p", { className: "mp-hint", role: "status" }, notice) : null);
		}
		function ObservationWorkspace({ revision, retentionVersion, now, onClose, onSource }) {
			const generation = useRef(0), dialog = useRef(null), [tab, setTab] = useState("activity"), [data, setData] = useState({ items: [], devices: [], loaded: false }), [older, setOlder] = useState({ items: [], cursor: undefined }), [deviceId, setDevice] = useState(""), [source, setSource] = useState(""), [since, setSince] = useState(""), [until, setUntil] = useState(""), [error, setError] = useState(null), [loading, setLoading] = useState(false), [refresh, setRefresh] = useState(0);
			const overdue = item => item.sourceState !== "expired" && Number.isFinite(item.sourceExpiresAt) && Number.isFinite(now) && now >= item.sourceExpiresAt;
			const deadlineReached = [...older.items, ...data.items].some(overdue);
			const urlFor = cursor => { const q = new URLSearchParams({ limit: "30" });if (deviceId) q.set("deviceId", deviceId);if (source) q.set("source", source);if (cursor) q.set("before", cursor);if (since) q.set("since", String(new Date(since + "T00:00:00").getTime()));if (until) { const end = new Date(until + "T00:00:00");end.setDate(end.getDate() + 1);q.set("until", String(end.getTime())); }return "api/mira-projects/observations?" + q; };
			useEffect(() => { const previous = document.activeElement;dialog.current?.showModal();return () => { if (previous?.isConnected) previous.focus(); }; }, []);
			useEffect(() => { generation.current++;setOlder({ items: [], cursor: undefined }); }, [deviceId, source, since, until, retentionVersion, deadlineReached]);
			useEffect(() => { const abort = new AbortController();fetch(urlFor(), { credentials: "same-origin", cache: "no-store", signal: abort.signal }).then(async response => { const body = await response.json();if (!response.ok) throw Error(body.error);return body; }).then(body => { if (!abort.signal.aborted) { setData({ ...body, loaded: true });setError(null); } }, e => { if (!abort.signal.aborted) setError(e.message); });return () => abort.abort(); }, [revision, refresh, deviceId, source, since, until, deadlineReached]);
			const items = [...new Map([...older.items, ...data.items].map(item => [item.ref, item])).values()].sort((a, b) => b.happenedAt - a.happenedAt || Number(b.ref.split("#")[1]) - Number(a.ref.split("#")[1])).map(item => overdue(item) ? { ...item, text: "", title: "原文已到期，清理待完成", sourceState: "expired", sourceCleanupPending: true } : item);
			const cursor = older.cursor === undefined ? data.nextCursor : older.cursor;
			const more = async () => { const current = generation.current;setLoading(true);try { const response = await fetch(urlFor(cursor), { credentials: "same-origin", cache: "no-store" }), body = await response.json();if (!response.ok) throw Error(body.error);if (current === generation.current) { setOlder(old => ({ items: [...old.items, ...body.items], cursor: body.nextCursor }));setError(null); } } catch (e) { setError(e.message); } finally { setLoading(false); } };
			const frames = items.filter(item => item.media).map(item => item.ref);
			return h("dialog", { ref: dialog, className: "mp-observation-dialog", "aria-label": "活动与设备", onCancel: e => { e.preventDefault();onClose(); } },
				h("header", { className: "mp-trace-header" }, h("strong", null, "活动与设备"), h(IconButton, { name: "close", label: "关闭活动与设备", onClick: onClose })),
				h("div", { className: "mp-tabs", role: "tablist", "aria-label": "观察视图" }, [["activity", "活动回看"], ["devices", "设备与同步"]].map(([value, label]) => h("button", { type: "button", role: "tab", key: value, className: "mp-tab" + (tab === value ? " mp-tab-on" : ""), "aria-selected": tab === value, onClick: () => setTab(value) }, label))),
				h("div", { className: "mp-observation-scroll" }, error ? h("p", { role: "alert", className: "mp-error" }, error) : null,
					tab === "devices" ? h("div", null, h("p", { className: "mp-hint" }, "这里控制支持观察同步的客户端。新设备默认暂停，不会自动开始屏幕采集。"), data.devices.map(device => h(ObservationDevice, { key: device.id, device, onRefresh: () => setRefresh(n => n + 1) })), data.loaded && !data.devices.length ? h("p", { className: "mp-hint" }, "还没有连接支持观察同步的客户端。") : null) : h(React.Fragment, null,
						h("div", { className: "mp-observation-filters" }, h("label", null, "设备", h("select", { "aria-label": "设备", value: deviceId, onChange: e => setDevice(e.target.value) }, h("option", { value: "" }, "所有设备"), data.devices.map(device => h("option", { key: device.id, value: device.id }, device.name)))), h("label", null, "资料来源", h("select", { "aria-label": "资料来源", value: source, onChange: e => setSource(e.target.value) }, h("option", { value: "" }, "所有来源"), Object.entries(observationSourceLabel).map(([value, label]) => h("option", { key: value, value }, label)))), h("label", null, "开始日期", h("input", { type: "date", "aria-label": "开始日期", value: since, onChange: e => setSince(e.target.value) })), h("label", null, "截止日期", h("input", { type: "date", "aria-label": "截止日期", value: until, onChange: e => setUntil(e.target.value) }))),
						
						items.map(item => h("button", { key: item.ref, type: "button", className: "mp-observation-item", "aria-label": "查看活动：" + (item.title || item.app), onClick: () => onSource(item.ref, frames) }, item.media ? h("img", { src: item.media.url, alt: item.title || "关键画面", loading: "lazy" }) : h("span", { className: "mp-observation-no-image" }, item.sourceState === "expired" ? (item.sourceCleanupPending ? "清理待完成" : "原文已清理") : item.mediaState === "expired" ? "图片已清理" : "仅文字"), h("span", { className: "mp-observation-item-main" }, h("span", { className: "mp-hint" }, new Date(item.happenedAt).toLocaleString(), "，", item.deviceName, "，", observationSourceLabel[item.source]), h("strong", null, item.title || item.app || "设备活动"), h("span", null, item.text), item.clockSkewMs ? h("small", { className: "mp-hint" }, "设备时钟存在偏差，请对照收到时间") : null))),
						!data.loaded && !error ? h("p", { role: "status" }, "正在读取活动…") : data.loaded && !items.length ? h("p", { className: "mp-hint" }, "这个范围还没有可靠收到的活动记录。") : null,
						cursor ? h(Btn, { disabled: loading, onClick: more }, loading ? "正在加载…" : "加载更早活动") : null)));
		}

		const connectorStateLabel = { paused: "已暂停", waiting: "等待首次同步", ready: "最近同步成功", "rate-limited": "等待来源恢复", "authentication-required": "需要更新连接", "resync-required": "需要重新同步", error: "等待重试", unavailable: "账号配置不可用" };
		function ConnectorSource({ source }) {
			const [busy, setBusy] = useState(false), [error, setError] = useState(null), [notice, setNotice] = useState(null);
			const change = async enabled => { setBusy(true);setError(null);setNotice(null);try { await personalCommand("connectorConfigure", { id: source.id, enabled, expectedRevision: source.revision }, "connector:" + source.id);setNotice(enabled ? "已开启，只同步此后新资料。" : "已暂停，已经保存的资料保留。"); } catch (e) { setError(e.message); } finally { setBusy(false); } };
			return h("section", { className: "mp-connector-source", "aria-label": "连接来源：" + source.name },
				h("div", { className: "mp-connector-source-head" }, h("strong", null, source.name), h("span", { className: "mp-pill" }, connectorStateLabel[source.state] || "等待同步")),
				h("p", { className: "mp-hint" }, source.provider === "graph-mail" ? "Microsoft 365 邮箱，只读" : "飞书会话，只读", "，已保存 ", source.records, " 条", source.expiredRecords ? `，已到期 ${source.expiredRecords} 条` : ""),
				h("p", { className: "mp-hint" }, source.coverage),
				source.lastSyncAt ? h("p", { className: "mp-hint" }, "最近同步：", new Date(source.lastSyncAt).toLocaleString(), source.caughtUp ? "，已读完本轮资料" : "，仍有资料待同步") : null,
				source.nextAttemptAt && ["error", "rate-limited"].includes(source.state) ? h("p", { className: "mp-hint" }, "下次尝试：", new Date(source.nextAttemptAt).toLocaleString()) : null,
				source.error ? h("p", { className: "mp-hint", role: "status" }, source.error) : null,
				h("div", { className: "mp-source-actions" }, h(Btn, { disabled: busy || !source.configured, "aria-label": (source.enabled ? "暂停来源：" : "开启来源：") + source.name, onClick: () => change(!source.enabled) }, busy ? "正在保存…" : source.enabled ? "暂停同步" : "开启同步"), source.state === "resync-required" ? h(Btn, { disabled: busy, onClick: () => change(true) }, "从现在重新同步") : null),
				error ? h("p", { className: "mp-error", role: "alert" }, error) : null, notice ? h("p", { className: "mp-hint", role: "status" }, notice) : null);
		}
		function ConnectorsWorkspace({ data, onClose, onSource }) {
			const dialog = useRef(null);
			useEffect(() => { const previous = document.activeElement;dialog.current?.showModal();return () => { if (previous?.isConnected) previous.focus(); }; }, []);
			return h("dialog", { ref: dialog, className: "mp-trace-dialog mp-connectors-dialog", "aria-label": "邮件与飞书来源", onCancel: e => { e.preventDefault();onClose(); } },
				h("header", { className: "mp-trace-header" }, h("strong", null, "邮件与飞书来源"), h(IconButton, { name: "close", label: "关闭邮件与飞书来源", onClick: onClose })),
				h("div", { className: "mp-source-body" }, h("p", { className: "mp-hint" }, "从开启的时刻开始读取。资料里出现的请求不算你的授权。"),
					!data?.items?.length ? h("p", null, "尚未连接邮箱或飞书。当前版本需要由管理员连接只读账号，然后在这里开启同步。") : h("div", { className: "mp-connector-sources" }, data.items.map(source => h(ConnectorSource, { key: source.id, source }))),
					h("h3", { className: "mp-connector-recent-title" }, "最近资料"), data?.activity?.length ? data.activity.map(item => h("button", { key: item.ref, type: "button", className: "mp-connector-record", onClick: () => onSource(item.ref) }, h("span", { className: "mp-hint" }, item.name, "，", ago(item.happenedAt)), h("strong", null, item.title), h("span", null, item.preview))) : h("p", { className: "mp-hint" }, "暂无同步资料")));
		}

		function HandoffDialog({ request, projects, onClose }) {
			const dialog = useRef(null), [projectId, setProjectId] = useState(projects[0]?.id || ""), [text, setText] = useState(request.text || ""), [ready, setReady] = useState(request.text !== undefined), [busy, setBusy] = useState(false), [error, setError] = useState(null);
			useEffect(() => {
				const previous = document.activeElement; dialog.current?.showModal(); const abort = new AbortController();
				if (!ready && request.sourceRef) fetch("api/mira-projects/personal/source?ref=" + encodeURIComponent(request.sourceRef), { credentials: "same-origin", signal: abort.signal }).then(async r => { const d = await r.json(); if (!r.ok) throw Error(d.error); return d; }).then(d => { setText(d.text); setReady(true); }, e => { if (!abort.signal.aborted) setError(e.message); });
				return () => { abort.abort(); if (previous?.isConnected) previous.focus(); };
			}, []);
			const submit = async e => { e.preventDefault(); setBusy(true); setError(null); try { await personalCommand("personalHandoff", { projectId, messageId: request.messageId, text, refs: request.refs || [] }, "handoff:" + request.messageId); onClose(); } catch (e) { setError(e.message); } finally { setBusy(false); } };
			return h("dialog", { ref: dialog, className: "mp-handoff-dialog", "aria-label": "交给项目", onCancel: e => { e.preventDefault(); onClose(); } },
				h("header", { className: "mp-trace-header" }, h("strong", null, "交给项目"), h(IconButton, { name: "close", label: "关闭交接", onClick: onClose })),
				h("form", { onSubmit: submit, className: "mp-handoff-form" },
					h("label", null, "目标项目", h("select", { "aria-label": "目标项目", value: projectId, disabled: busy, onChange: e => setProjectId(e.target.value) }, projects.map(p => h("option", { key: p.id, value: p.id }, p.name)))),
					h("label", null, "交给项目的需求", h("textarea", { "aria-label": "交给项目的需求", value: text, disabled: busy || !ready, onChange: e => setText(e.target.value), rows: 6 })),
					h("p", { className: "mp-hint" }, `保留个人对话中的原话，附带 ${request.refs?.length || 0} 条相关资料。发送后在这里显示项目回执。`),
					error ? h("p", { className: "mp-error", role: "alert" }, error) : null,
					h("button", { type: "submit", className: "mp-btn mp-btn-strong", disabled: busy || !ready || !projectId || !text.trim() }, busy ? "正在交接…" : "发送给项目")));
		}

		function RetentionDialog({ data, onClose }) {
			const dialog = useRef(null), [policy, setPolicy] = useState(() => ({ rawDays: data.policy.rawDays, summaryDays: data.policy.summaryDays })), [revision, setRevision] = useState(data.policy.revision), [dirty, setDirty] = useState(false), [busy, setBusy] = useState(false), [error, setError] = useState(null), [notice, setNotice] = useState(null);
			useEffect(() => { const previous = document.activeElement;dialog.current?.showModal();return () => { if (previous?.isConnected) previous.focus(); }; }, []);
			const save = async event => {
				event.preventDefault();setBusy(true);setError(null);setNotice(null);
				try { const result = await personalCommand("retentionConfigure", { policy, expectedRevision: revision }, "source-retention-policy");setRevision(result.revision);setDirty(false);setNotice(result.cleanup.pending ? "设置已保存；原文清理尚未完成，可重试。" : "设置已保存，当前到期原文已清理。"); } catch (e) { setError(e.message); } finally { setBusy(false); }
			};
			const retry = async () => { setBusy(true);setError(null);setNotice(null);try { await personalCommand("retentionCollect", {}, "source-retention-collect");setNotice("当前到期原文已清理。"); } catch (e) { setError(e.message); } finally { setBusy(false); } };
			return h("dialog", { ref: dialog, className: "mp-handoff-dialog mp-retention-dialog", "aria-label": "资料保留", onCancel: e => { e.preventDefault();onClose(); } },
				h("header", { className: "mp-trace-header" }, h("strong", null, "资料保留"), h(IconButton, { name: "close", label: "关闭资料保留", onClick: onClose })),
				h("form", { className: "mp-handoff-form", onSubmit: save },
					h("p", { className: "mp-hint" }, "按收到时间计算，到期自动清理。"),
					[["rawDays", "原文保留"], ["summaryDays", "活动摘要保留"]].map(([key, label]) => h("label", { key }, label, h("select", { "aria-label": label, value: policy[key], disabled: busy, onChange: e => { setPolicy(old => ({ ...old, [key]: Number(e.target.value) }));setDirty(true);setNotice(null); } }, [...new Set([0, 1, 7, 30, 90, 365, 3650, policy[key]])].sort((a, b) => a - b).map(days => h("option", { key: days, value: days }, days ? `${days} 天` : "长期保留"))))),
					h("p", { className: "mp-hint" }, "清理原文、标题和文件路径，引用编号和时间保留；图片在「活动与设备」里单独设置。"),
					h("p", { className: "mp-hint" }, "对话、项目、记忆和回顾不受影响。"),
					(data.cleaned ?? data.expired) ? h("p", { className: "mp-hint", role: "status" }, `已清理 ${data.cleaned ?? data.expired} 条原文`, data.lastCompletedAt ? `，上次 ${new Date(data.lastCompletedAt).toLocaleString()}` : "") : null,
					data.pending ? h("p", { className: "mp-error", role: "status" }, data.error || "原文清理尚未完成，过期资料暂不用于检索。") : null,
					revision !== data.policy.revision ? h("p", { className: "mp-hint" }, "服务器设置已变化。", h(Btn, { disabled: busy, onClick: () => { setPolicy({ rawDays: data.policy.rawDays, summaryDays: data.policy.summaryDays });setRevision(data.policy.revision);setDirty(false);setError(null);setNotice(null); } }, "载入最新设置")) : null,
					error ? h("p", { className: "mp-error", role: "alert" }, error) : null, notice ? h("p", { className: "mp-hint", role: "status" }, notice) : null,
					h("div", { className: "mp-source-actions" }, h("button", { type: "submit", className: "mp-btn mp-btn-strong", disabled: busy || !dirty }, busy ? "正在处理…" : "保存"), data.pending ? h(Btn, { disabled: busy, onClick: retry }, "重试清理") : null)));
		}

		// ================================================================== v4，every level reads like a DSH session
		//
		// Mira, a project's conversation and each task share one frame: header, conversation, composer, and a right
		// panel of tabs. A task is the real DSH Session (native conversation, files, terminal, browser); the plugin adds
		// its own right-panel tabs, the goal strip above the composer, and cards for its tools inside the conversation.

		/** Host services captured at apply time; null until the DSH shell provides them. */
		const shell = { workspace: null, sidebar: null, sessions: null, showNav: null, showNative: null, expandSidebar: null, showingNav: true, navListeners: new Set() };
		function useShowingNav() {
			const subscribe = useCallback(fn => { shell.navListeners.add(fn); return () => shell.navListeners.delete(fn); }, []);
			const read = useCallback(() => shell.showingNav, []);
			return useSyncExternalStore(subscribe, read, read);
		}
		function SidebarModeSwitch({ wide }) {
			const showingNav = useShowingNav();
			const label = showingNav ? "显示工作区" : "显示项目";
			const title = showingNav ? "切换到原有工作区与对话" : "切回项目与最近任务";
			const toggle = () => {
				if (showingNav) shell.showNative?.();
				else { shell.showNav?.(); if (!wide) shell.expandSidebar?.(); }
			};
			const btn = !wide
				? h("button", { type: "button", className: "mp-sidebar-switch mp-sidebar-switch-rail", "aria-label": title, title: P.Tooltip ? undefined : title, onClick: toggle }, h(Icon, { name: showingNav ? "folder" : "tasks", size: 18 }))
				: h("button", { type: "button", className: "mp-sidebar-switch", "aria-label": title, title, onClick: toggle }, h(Icon, { name: showingNav ? "folder" : "tasks", size: 16 }), h("span", null, label));
			return !wide && P.Tooltip ? h(P.Tooltip, { label: title, side: "right", delayMs: 500, portal: true }, btn) : btn;
		}
		const TASK_TAB_ID = "mira-projects.task", TASK_TAB_KIND = "mira-projects-task";
		const CHANGES_TAB_ID = "mira-projects.changes", CHANGES_TAB_KIND = "mira-projects-changes";
		const AGENTS_TAB_ID = "mira-projects.agents", AGENTS_TAB_KIND = "mira-projects-agents";
		// Inside a project conversation (the project assistant's Session).
		const PROJECT_TASKS_KIND = "mira-projects-project-tasks";
		const PROJECT_TABS = [
			{ id: "mira-projects.project-tasks", kind: PROJECT_TASKS_KIND, title: "项目任务", order: 4, desc: "这个项目的任务：等你的、在跑的、做完的" },
			{ id: "mira-projects.project-memory", kind: "mira-projects-project-memory", title: "项目记忆", order: 8, desc: "记忆助理记下的现状、决定和没收尾的事", quiet: true },
			{ id: "mira-projects.project-files", kind: "mira-projects-project-files", title: "共享文件", order: 9, desc: "项目里共享的文件和版本" },
			{ id: "mira-projects.project-settings", kind: "mira-projects-project-settings", title: "项目设置", order: 10, desc: "项目名、说明和模型" },
			{ id: "mira-projects.project-automations", kind: "mira-projects-project-automations", title: "自动任务", order: 11, desc: "定时让项目做的事" },
		];

		const isIndexRoute = () => window.location.hash === "#mira-projects";
		function navigateIndex() {
			if (window.location.hash !== "#mira-projects") window.history.pushState({ ...window.history.state, miraProjectsPanel: PANEL_ID }, "", "#mira-projects");
			window.dispatchEvent(new CustomEvent("mira-projects:navigate"));
		}
		/** Re-renders on every in-app navigation; returns the hash. */
		function useRoute() {
			const [hash, setHash] = useState(() => window.location.hash);
			useEffect(() => {
				const sync = () => setHash(window.location.hash);
				const names = ["hashchange", "popstate", "mira-projects:navigate", "mira-personal:navigate"];
				for (const name of names) window.addEventListener(name, sync);
				return () => { for (const name of names) window.removeEventListener(name, sync); };
			}, []);
			return hash;
		}
		/** The DSH Session on screen, if any (a plugin page fills the main column otherwise). */
		function useOnScreenSession() {
			const mounted = shell.sidebar?.mounted;
			const subscribe = useCallback(listener => (mounted && typeof mounted.subscribe === "function" ? mounted.subscribe(listener) : null) || (() => {}), [mounted]);
			const read = useCallback(() => (mounted && typeof mounted.getSnapshot === "function" ? mounted.getSnapshot() : undefined), [mounted]);
			return useSyncExternalStore(subscribe, read, read);
		}
		/** The Session a sub-agent works for: DSH's parent links, followed up to the first Session that is not a sub-agent. */
		function rootSessionId(byId, sessionId) {
			const seen = new Set();
			let cursor = sessionId;
			while (cursor && !seen.has(cursor)) {
				seen.add(cursor);
				const summary = byId && byId[cursor];
				if (!summary || summary.origin !== "subagent" || !summary.parentId) break;
				cursor = summary.parentId;
			}
			return cursor;
		}
		/** `sessionId` itself, or for a sub-agent (at any depth) the Session that sent it out. */
		function useRootSession(sessionId) {
			const list = shell.sessions?.list;
			const subscribe = useCallback(listener => (list && typeof list.subscribe === "function" ? list.subscribe(listener) : null) || (() => {}), [list]);
			const read = useCallback(() => rootSessionId(list && typeof list.getSnapshot === "function" ? list.getSnapshot().byId : undefined, sessionId), [list, sessionId]);
			return useSyncExternalStore(subscribe, read, read);
		}
		/** The last two Sessions shown, so a project's list can mark the task the owner just came back from. */
		const trail = { current: undefined, previous: undefined };
		function notePreviousSession() {
			const mounted = shell.sidebar?.mounted;
			const id = mounted && typeof mounted.getSnapshot === "function" ? mounted.getSnapshot() : undefined;
			if (id && id !== trail.current) { trail.previous = trail.current; trail.current = id; }
			return trail.previous;
		}
		function usePreviousSession() {
			const mounted = shell.sidebar?.mounted;
			const subscribe = useCallback(listener => (mounted && typeof mounted.subscribe === "function" ? mounted.subscribe(listener) : null) || (() => {}), [mounted]);
			return useSyncExternalStore(subscribe, notePreviousSession, notePreviousSession);
		}
		function usePersonal() {
			return useShared("api/mira-projects/personal", { loaded: false, messages: [], projects: [], handoffs: [], activity: [], actions: [] });
		}
		function useToday() {
			return useShared("api/mira-projects/today", { loaded: false, waiting: { decisions: [], review: [], failed: [] }, timeline: [], recap: {}, projects: [] });
		}

		// ---------------------------------------------------------------- task state, in the owner's words

		const isWaiting = t => !t.resolved && (t.state === "review_ready" || t.state === "blocked" || t.state === "failed");
		const isRunning = t => !t.resolved && t.state === "working";
		function taskPill(t) {
			if (!t) return ["", "idle"];
			if (t.state === "done") return ["已完成", "done"];
			if (t.resolved) return ["已归档", "idle"];
			if (t.state === "review_ready") return ["待验收", "wait"];
			if (t.state === "blocked") return [t.experience?.reason || "等你", "wait"];
			if (t.state === "failed") return ["失败", "fail"];
			if (t.experience?.execution?.phase === "queued") return ["排队中", "run"];
			return ["进行中", "run"];
		}
		function Pill({ tone, children }) { return h("span", { className: "mp4-pill mp4-" + tone }, children); }
		/** Pieces of one line, set apart by space (no separator characters). */
		const parts = (...items) => items.filter(x => x !== null && x !== undefined && x !== "").map((x, i) => h("span", { key: i, className: "mp4-part" }, x));
		/**
		 * A task's status as Linear and Multica draw it: a ring whose fill says how far along, one colour per kind.
		 * 排队 dashed grey, 进行中 half blue, 待拍板 yellow with a pause, 待验收 three-quarters yellow, 失败 red with a bar,
		 * 已完成 solid green with a check, 已归档 grey with a cross.
		 */
		const STATUS_LOOK = { queued: ["排队中", "#8d8d8d"], working: ["进行中", "#0090ff"], decision: ["待拍板", "#eab308"], review: ["待验收", "#eab308"], failed: ["失败", "#e5484d"], done: ["已完成", "#30a46c"], archived: ["已归档", "#8d8d8d"], note: ["提醒", "#8d8d8d"] };
		function taskStatus(t) {
			if (!t) return "queued";
			if (t.state === "done") return "done";
			if (t.resolved) return "archived";
			if (t.state === "review_ready") return "review";
			if (t.state === "blocked") return "decision";
			if (t.state === "failed") return "failed";
			if (t.experience?.execution?.phase === "queued") return "queued";
			return "working";
		}
		function StatusIcon({ status, size = 14 }) {
			const [label, color] = STATUS_LOOK[status] || STATUS_LOOK.queued;
			const ring = extra => h("circle", { key: "ring", cx: 7, cy: 7, r: 5.6, fill: "none", stroke: color, strokeWidth: 1.5, ...extra });
			const inner = status === "queued" ? [ring({ strokeDasharray: "1.8 1.6" })]
				: status === "working" ? [ring(), h("path", { key: "fill", d: "M7 3.6A3.4 3.4 0 0 1 7 10.4Z", fill: color })]
				: status === "review" ? [ring(), h("path", { key: "fill", d: "M7 7V3.6A3.4 3.4 0 1 1 3.6 7Z", fill: color })]
				: status === "decision" ? [ring(), h("rect", { key: "a", x: 5, y: 4.6, width: 1.4, height: 4.8, rx: 0.4, fill: color }), h("rect", { key: "b", x: 7.6, y: 4.6, width: 1.4, height: 4.8, rx: 0.4, fill: color })]
				: status === "failed" ? [ring(), h("path", { key: "bar", d: "M3.1 10.9L10.9 3.1", stroke: color, strokeWidth: 1.5 })]
				: status === "note" ? [ring(), h("circle", { key: "dot", cx: 7, cy: 7, r: 1.8, fill: color })]
				: status === "done" ? [h("circle", { key: "disc", cx: 7, cy: 7, r: 6.3, fill: color }), h("path", { key: "tick", d: "M4.4 7.2l1.8 1.8 3.5-3.7", fill: "none", stroke: "#FFFFFF", strokeWidth: 1.6, strokeLinecap: "round", strokeLinejoin: "round" })]
				: [ring(), h("path", { key: "x", d: "M5.1 5.1l3.8 3.8M8.9 5.1l-3.8 3.8", stroke: color, strokeWidth: 1.4, strokeLinecap: "round" })];
			return h("svg", { className: "mp4-status", width: size, height: size, viewBox: "0 0 14 14", role: "img", "aria-label": label }, h("title", null, label), inner);
		}
		/**
		 * What a project wears in lists: one line icon from a fixed library, drawn in its colour. A model picks both once
		 * (project-icons.js on the server); the owner can change them in 项目设置. Until then a stable stand-in from the id.
		 * The icons are Lucide's (https://lucide.dev, ISC licence, Copyright (c) Lucide Icons and Contributors), copied as
		 * shapes so the plugin needs no package; the server keeps the same 40 names (a test checks they match).
		 */
		const PROJECT_TINTS = { gray: "#8d8d8d", red: "#e5484d", orange: "#f76b15", yellow: "#ffc53d", green: "#30a46c", teal: "#12a594", blue: "#0090ff", purple: "#8e4ec6", pink: "#d6409f" };
		const PROJECT_ICON_SHAPES = {"book-open":[["path",{"d":"M12 5v16"}],["path",{"d":"M20.001 19A2 2 0 0022 17V5a2 2 0 00-1.999-2L16 3.002A5 5 0 0012 5a5 5 0 00-4-2H4a2 2 0 00-2 2v12a2 2 0 001.999 2H8a5 5 0 014 2 5 5 0 014-2z"}]],"bot":[["path",{"d":"M12 8V4H8"}],["rect",{"width":"16","height":"12","x":"4","y":"8","rx":"2"}],["path",{"d":"M2 14h2"}],["path",{"d":"M20 14h2"}],["path",{"d":"M15 13v2"}],["path",{"d":"M9 13v2"}]],"brain":[["path",{"d":"M12 18V5"}],["path",{"d":"M15 13a4.17 4.17 0 01-3-4 4.17 4.17 0 01-3 4"}],["path",{"d":"M17.707 6.294A3 3 0 1012 5a3 3 0 10-5.707 1.294"}],["path",{"d":"M17.996 5.125a4 4 0 012.526 5.77"}],["path",{"d":"M18 18a4 4 0 002-7.465"}],["path",{"d":"M19.967 17.482a4 4 0 11-7.967.517 4 4 0 11-7.967-.517"}],["path",{"d":"M6 18a4 4 0 01-2-7.465"}],["path",{"d":"M6.004 5.125a4 4 0 00-2.526 5.77"}]],"briefcase":[["path",{"d":"M16 20V4a2 2 0 0 0-2-2h-4a2 2 0 0 0-2 2v16"}],["rect",{"width":"20","height":"14","x":"2","y":"6","rx":"2"}]],"calendar":[["path",{"d":"M8 2v3"}],["path",{"d":"M16 2v3"}],["rect",{"x":"3","y":"3","width":"18","height":"18","rx":"2"}],["path",{"d":"M3 9h18"}]],"camera":[["path",{"d":"M13.997 4a2 2 0 0 1 1.76 1.05l.486.9A2 2 0 0 0 18.003 7H20a2 2 0 0 1 2 2v9a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V9a2 2 0 0 1 2-2h1.997a2 2 0 0 0 1.759-1.048l.489-.904A2 2 0 0 1 10.004 4z"}],["circle",{"cx":"12","cy":"13","r":"3"}]],"chart-column":[["path",{"d":"M3 3v16a2 2 0 0 0 2 2h16"}],["path",{"d":"M18 17V9"}],["path",{"d":"M13 17V5"}],["path",{"d":"M8 17v-3"}]],"chart-line":[["path",{"d":"M3 3v16a2 2 0 0 0 2 2h16"}],["path",{"d":"m19 9-5 5-4-4-3 3"}]],"code":[["path",{"d":"m16 18 6-6-6-6"}],["path",{"d":"m8 6-6 6 6 6"}]],"coins":[["path",{"d":"M13.744 17.736a6 6 0 1 1-7.48-7.48"}],["path",{"d":"M15 6h1v4"}],["path",{"d":"m6.134 14.768.866-.5 2 3.464"}],["circle",{"cx":"16","cy":"8","r":"6"}]],"compass":[["circle",{"cx":"12","cy":"12","r":"10"}],["path",{"d":"m16.24 7.76-1.804 5.411a2 2 0 0 1-1.265 1.265L7.76 16.24l1.804-5.411a2 2 0 0 1 1.265-1.265z"}]],"database":[["ellipse",{"cx":"12","cy":"5","rx":"9","ry":"3"}],["path",{"d":"M3 5V19A9 3 0 0 0 21 19V5"}],["path",{"d":"M3 12A9 3 0 0 0 21 12"}]],"file-text":[["path",{"d":"M6 22a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h8a2.4 2.4 0 0 1 1.704.706l3.588 3.588A2.4 2.4 0 0 1 20 8v12a2 2 0 0 1-2 2z"}],["path",{"d":"M14 2v5a1 1 0 0 0 1 1h5"}],["path",{"d":"M10 9H8"}],["path",{"d":"M16 13H8"}],["path",{"d":"M16 17H8"}]],"film":[["rect",{"width":"18","height":"18","x":"3","y":"3","rx":"2"}],["path",{"d":"M7 3v18"}],["path",{"d":"M3 7.5h4"}],["path",{"d":"M3 12h18"}],["path",{"d":"M3 16.5h4"}],["path",{"d":"M17 3v18"}],["path",{"d":"M17 7.5h4"}],["path",{"d":"M17 16.5h4"}]],"flask-conical":[["path",{"d":"M14 2v6a2 2 0 0 0 .245.96l5.51 10.08A2 2 0 0 1 18 22H6a2 2 0 0 1-1.755-2.96l5.51-10.08A2 2 0 0 0 10 8V2"}],["path",{"d":"M6.453 15h11.094"}],["path",{"d":"M8.5 2h7"}]],"folder":[["path",{"d":"M20 20a2 2 0 0 0 2-2V8a2 2 0 0 0-2-2h-7.9a2 2 0 0 1-1.69-.9L9.6 3.9A2 2 0 0 0 7.93 3H4a2 2 0 0 0-2 2v13a2 2 0 0 0 2 2Z"}]],"globe":[["circle",{"cx":"12","cy":"12","r":"10"}],["path",{"d":"M12 2a14.5 14.5 0 0 0 0 20 14.5 14.5 0 0 0 0-20"}],["path",{"d":"M2 12h20"}]],"graduation-cap":[["path",{"d":"M21.42 10.922a1 1 0 0 0-.019-1.838L12.83 5.18a2 2 0 0 0-1.66 0L2.6 9.08a1 1 0 0 0 0 1.832l8.57 3.908a2 2 0 0 0 1.66 0z"}],["path",{"d":"M22 10v6"}],["path",{"d":"M6 12.5V16a6 3 0 0 0 12 0v-3.5"}]],"headphones":[["path",{"d":"M3 14h3a2 2 0 0 1 2 2v3a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-7a9 9 0 0 1 18 0v7a2 2 0 0 1-2 2h-1a2 2 0 0 1-2-2v-3a2 2 0 0 1 2-2h3"}]],"heart":[["path",{"d":"M2 9.5a5.5 5.5 0 0 1 9.591-3.676.56.56 0 0 0 .818 0A5.49 5.49 0 0 1 22 9.5c0 2.29-1.5 4-3 5.5l-5.492 5.313a2 2 0 0 1-3 .019L5 15c-1.5-1.5-3-3.2-3-5.5"}]],"house":[["path",{"d":"M15 21v-8a1 1 0 0 0-1-1h-4a1 1 0 0 0-1 1v8"}],["path",{"d":"M3 10a2 2 0 0 1 .709-1.528l7-6a2 2 0 0 1 2.582 0l7 6A2 2 0 0 1 21 10v9a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"}]],"layers":[["path",{"d":"M12.83 2.18a2 2 0 0 0-1.66 0L2.6 6.08a1 1 0 0 0 0 1.83l8.58 3.91a2 2 0 0 0 1.66 0l8.58-3.9a1 1 0 0 0 0-1.83z"}],["path",{"d":"M2 12a1 1 0 0 0 .58.91l8.6 3.91a2 2 0 0 0 1.65 0l8.58-3.9A1 1 0 0 0 22 12"}],["path",{"d":"M2 17a1 1 0 0 0 .58.91l8.6 3.91a2 2 0 0 0 1.65 0l8.58-3.9A1 1 0 0 0 22 17"}]],"lightbulb":[["path",{"d":"M15 14c.2-1 .7-1.7 1.5-2.5 1-.9 1.5-2.2 1.5-3.5A6 6 0 0 0 6 8c0 1 .2 2.2 1.5 3.5.7.7 1.3 1.5 1.5 2.5"}],["path",{"d":"M9 18h6"}],["path",{"d":"M10 22h4"}]],"message-circle":[["path",{"d":"M2.992 16.342a2 2 0 0 1 .094 1.167l-1.065 3.29a1 1 0 0 0 1.236 1.168l3.413-.998a2 2 0 0 1 1.099.092 10 10 0 1 0-4.777-4.719"}]],"notebook-pen":[["path",{"d":"M13.4 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-7.4"}],["path",{"d":"M2 6h4"}],["path",{"d":"M2 10h4"}],["path",{"d":"M2 14h4"}],["path",{"d":"M2 18h4"}],["path",{"d":"M21.378 5.626a1 1 0 1 0-3.004-3.004l-5.01 5.012a2 2 0 0 0-.506.854l-.837 2.87a.5.5 0 0 0 .62.62l2.87-.837a2 2 0 0 0 .854-.506z"}]],"package":[["path",{"d":"M11 21.73a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73z"}],["path",{"d":"M12 22V12"}],["polyline",{"points":"3.29 7 12 12 20.71 7"}],["path",{"d":"m7.5 4.27 9 5.15"}]],"palette":[["path",{"d":"M12 22a1 1 0 0 1 0-20 10 9 0 0 1 10 9 5 5 0 0 1-5 5h-2.25a1.75 1.75 0 0 0-1.4 2.8l.3.4a1.75 1.75 0 0 1-1.4 2.8z"}],["circle",{"cx":"13.5","cy":"6.5","r":".5"}],["circle",{"cx":"17.5","cy":"10.5","r":".5"}],["circle",{"cx":"6.5","cy":"12.5","r":".5"}],["circle",{"cx":"8.5","cy":"7.5","r":".5"}]],"pen-tool":[["path",{"d":"m18 13-.654-4.736a4 4 0 00-2.756-3.266l-9.163-2.9a2 2 0 00-2.017.493l-.82.819a2 2 0 00-.492 2.017l2.9 9.163a4 4 0 003.266 2.756L13 18"}],["path",{"d":"M18.648 12.352a1.205 1.205 0 011.704 0l1.296 1.296a1.205 1.205 0 010 1.704l-6.296 6.296a1.205 1.205 0 01-1.704 0l-1.296-1.296a1.205 1.205 0 010-1.704z"}],["path",{"d":"m3 3 6.586 6.586"}],["circle",{"cx":"11","cy":"11","r":"2"}]],"plane":[["path",{"d":"M17.8 19.2 16 11l3.5-3.5C21 6 21.5 4 21 3c-1-.5-3 0-4.5 1.5L13 8 4.8 6.2c-.5-.1-.9.1-1.1.5l-.3.5c-.2.5-.1 1 .3 1.3L9 12l-2 3H4l-1 1 3 2 2 3 1-1v-3l3-2 3.5 5.3c.3.4.8.5 1.3.3l.5-.2c.4-.3.6-.7.5-1.2z"}]],"puzzle":[["path",{"d":"M15.39 4.39a1 1 0 0 0 1.68-.474 2.5 2.5 0 1 1 3.014 3.015 1 1 0 0 0-.474 1.68l1.683 1.682a2.414 2.414 0 0 1 0 3.414L19.61 15.39a1 1 0 0 1-1.68-.474 2.5 2.5 0 1 0-3.014 3.015 1 1 0 0 1 .474 1.68l-1.683 1.682a2.414 2.414 0 0 1-3.414 0L8.61 19.61a1 1 0 0 0-1.68.474 2.5 2.5 0 1 1-3.014-3.015 1 1 0 0 0 .474-1.68l-1.683-1.682a2.414 2.414 0 0 1 0-3.414L4.39 8.61a1 1 0 0 1 1.68.474 2.5 2.5 0 1 0 3.014-3.015 1 1 0 0 1-.474-1.68l1.683-1.682a2.414 2.414 0 0 1 3.414 0z"}]],"rocket":[["path",{"d":"M12 15v5s3.03-.55 4-2c1.08-1.62 0-5 0-5"}],["path",{"d":"M4.5 16.5c-1.5 1.26-2 5-2 5s3.74-.5 5-2c.71-.84.7-2.13-.09-2.91a2.18 2.18 0 0 0-2.91-.09"}],["path",{"d":"M9 12a22 22 0 0 1 2-3.95A12.88 12.88 0 0 1 22 2c0 2.72-.78 7.5-6 11a22.4 22.4 0 0 1-4 2z"}],["path",{"d":"M9 12H4s.55-3.03 2-4c1.62-1.08 5 .05 5 .05"}]],"search":[["path",{"d":"m21 21-4.34-4.34"}],["circle",{"cx":"11","cy":"11","r":"8"}]],"shopping-cart":[["path",{"d":"m2.05 2.05 1.099-.028a1 1 0 0 1 1.008.815l2.69 14.347A1 1 0 0 0 7.83 18H18"}],["path",{"d":"M4.563 5h16.435a1 1 0 0 1 .981 1.204l-1.026 6.226A2 2 0 0 1 18.962 14H6.25"}],["circle",{"cx":"18","cy":"20","r":"2"}],["circle",{"cx":"8","cy":"20","r":"2"}]],"sparkles":[["path",{"d":"M11.017 2.814a1 1 0 0 1 1.966 0l1.051 5.558a2 2 0 0 0 1.594 1.594l5.558 1.051a1 1 0 0 1 0 1.966l-5.558 1.051a2 2 0 0 0-1.594 1.594l-1.051 5.558a1 1 0 0 1-1.966 0l-1.051-5.558a2 2 0 0 0-1.594-1.594l-5.558-1.051a1 1 0 0 1 0-1.966l5.558-1.051a2 2 0 0 0 1.594-1.594z"}],["path",{"d":"M20 2v4"}],["path",{"d":"M22 4h-4"}],["circle",{"cx":"4","cy":"20","r":"2"}]],"sprout":[["path",{"d":"M14 9.536V7a4 4 0 0 1 4-4h1.5a.5.5 0 0 1 .5.5V5a4 4 0 0 1-4 4 4 4 0 0 0-4 4c0 2 1 3 1 5a5 5 0 0 1-1 3"}],["path",{"d":"M4 9a5 5 0 0 1 8 4 5 5 0 0 1-8-4"}],["path",{"d":"M5 21h14"}]],"target":[["circle",{"cx":"12","cy":"12","r":"10"}],["circle",{"cx":"12","cy":"12","r":"6"}],["circle",{"cx":"12","cy":"12","r":"2"}]],"terminal":[["path",{"d":"M12 19h8"}],["path",{"d":"m4 17 6-6-6-6"}]],"trophy":[["path",{"d":"M10 14.66V17a1 1 0 0 1-1 1 2 2 0 0 0-2 2v2"}],["path",{"d":"M14 14.66V17a1 1 0 0 0 1 1 2 2 0 0 1 2 2v2"}],["path",{"d":"M17.916 10H19.5A2.5 2.5 0 0 0 22 7.5V5a1 1 0 0 0-1-1h-3"}],["path",{"d":"M4 22h16"}],["path",{"d":"M6 9a6 6 0 0 0 12 0V3a1 1 0 0 0-1-1H7a1 1 0 0 0-1 1z"}],["path",{"d":"M6.084 10H4.5A2.5 2.5 0 0 1 2 7.5V5a1 1 0 0 1 1-1h3"}]],"users":[["path",{"d":"M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"}],["path",{"d":"M16 3.128a4 4 0 0 1 0 7.744"}],["path",{"d":"M22 21v-2a4 4 0 0 0-3-3.87"}],["circle",{"cx":"9","cy":"7","r":"4"}]],"wrench":[["path",{"d":"M14.7 6.3a1 1 0 0 0 0 1.4l1.6 1.6a1 1 0 0 0 1.4 0l3.106-3.105c.32-.322.863-.22.983.218a6 6 0 0 1-8.259 7.057l-7.91 7.91a1 1 0 0 1-2.999-3l7.91-7.91a6 6 0 0 1 7.057-8.259c.438.12.54.662.219.984z"}]]};
		const PROJECT_ICON_NAMES = Object.keys(PROJECT_ICON_SHAPES);
		/** The first version used emoji; one still stored is shown as the library icon that means the same. */
		const LEGACY_PROJECT_EMOJI = { "📚": "book-open", "📊": "chart-column", "🎨": "palette", "💼": "briefcase", "🧪": "flask-conical", "🧠": "brain", "🔍": "search", "📝": "notebook-pen", "🛠️": "wrench", "🚀": "rocket", "🎯": "target", "🌱": "sprout", "🧩": "puzzle", "📦": "package", "🗂️": "folder", "💡": "lightbulb", "📈": "chart-line", "🤖": "bot", "🎬": "film", "🎵": "headphones", "✈️": "plane", "🏠": "house", "💰": "coins", "❤️": "heart" };
		function projectLook(p) {
			let hash = 0;
			for (const ch of String((p && p.id) || "")) hash = (hash * 31 + ch.codePointAt(0)) >>> 0;
			const colors = Object.keys(PROJECT_TINTS), raw = p && p.icon;
			const icon = raw && PROJECT_ICON_SHAPES[raw] ? raw : raw && LEGACY_PROJECT_EMOJI[raw] ? LEGACY_PROJECT_EMOJI[raw] : PROJECT_ICON_NAMES[hash % PROJECT_ICON_NAMES.length];
			return { icon, color: p && PROJECT_TINTS[p.color] ? p.color : colors[hash % colors.length] };
		}
		function ProjectGlyph({ name, color, size }) {
			return h("svg", { width: size, height: size, viewBox: "0 0 24 24", fill: "none", stroke: PROJECT_TINTS[color] || "currentColor", strokeWidth: 2, strokeLinecap: "round", strokeLinejoin: "round", "aria-hidden": true },
				(PROJECT_ICON_SHAPES[name] || []).map(([tag, attrs], i) => h(tag, { key: i, ...attrs })));
		}
		function ProjectMark({ p, size = 18, plain = false }) {
			const look = projectLook(p), tint = PROJECT_TINTS[look.color];
			// In lists the icon stands alone, the size of the host's line icons; its colour tells projects apart.
			if (plain) return h("span", { className: "mp4-pmark mp4-pmark-plain", "aria-hidden": true, style: { width: size, height: size } }, h(ProjectGlyph, { name: look.icon, color: look.color, size }));
			return h("span", { className: "mp4-pmark", "aria-hidden": true, style: { width: size, height: size, background: `color-mix(in srgb, ${tint} 14%, transparent)`, boxShadow: `inset 0 0 0 1px color-mix(in srgb, ${tint} 28%, transparent)` } }, h(ProjectGlyph, { name: look.icon, color: look.color, size: Math.round(size * 0.56) }));
		}
		/** 项目设置: the project's icon and colour, saved the moment they are picked. */
		function ProjectIconPicker({ project }) {
			const [error, setError] = useState(null), [busy, setBusy] = useState(false);
			const look = projectLook(project);
			const pick = async patch => {
				setBusy(true); setError(null);
				try { await act("setProjectIcon", { projectId: project.id, icon: look.icon, color: look.color, ...patch }); }
				catch (e) { setError(String(e?.message || e)); }
				finally { setBusy(false); }
			};
			const [open, setOpen] = useState(false);
			return h("div", { className: "mp4-iconpick" + (open ? "" : " mp4-iconpick-closed"), role: "group", "aria-label": "项目图标" },
				h("div", { className: "mp4-iconpick-head" }, h(ProjectMark, { p: project, size: 30 }), h("b", null, "图标和颜色"),
					h("button", { type: "button", className: "mp4-link-btn mp4-iconpick-toggle", "aria-expanded": open, onClick: () => setOpen(v => !v) }, open ? "收起" : "换一个")),
				open ? h("div", { className: "mp4-iconpick-grid" }, PROJECT_ICON_NAMES.map(icon => h("button", { key: icon, type: "button", disabled: busy, className: "mp4-iconpick-emoji" + (icon === look.icon ? " mp4-iconpick-on" : ""), "aria-label": `图标 ${icon}`, title: icon, onClick: () => pick({ icon }) }, h(ProjectGlyph, { name: icon, color: look.color, size: 18 })))) : null,
				open ? h("div", { className: "mp4-iconpick-colors" }, Object.entries(PROJECT_TINTS).map(([name, tint]) => h("button", { key: name, type: "button", disabled: busy, className: "mp4-iconpick-color" + (name === look.color ? " mp4-iconpick-on" : ""), "aria-label": `颜色 ${name}`, style: { background: tint }, onClick: () => pick({ color: name }) }))) : null,
				error ? h("p", { className: "mp-error" }, error) : null);
		}
		// ---------------------------------------------------------------- source and app icons
		// Brand outlines from the CC0 simple-icons set (24 × 24); 飞书 and 访达 are drawn here as stand-ins, not the official marks.
		const APP_ICON_PATHS = {
			claude: "m4.7144 15.9555 4.7174-2.6471.079-.2307-.079-.1275h-.2307l-.7893-.0486-2.6956-.0729-2.3375-.0971-2.2646-.1214-.5707-.1215-.5343-.7042.0546-.3522.4797-.3218.686.0608 1.5179.1032 2.2767.1578 1.6514.0972 2.4468.255h.3886l.0546-.1579-.1336-.0971-.1032-.0972L6.973 9.8356l-2.55-1.6879-1.3356-.9714-.7225-.4918-.3643-.4614-.1578-1.0078.6557-.7225.8803.0607.2246.0607.8925.686 1.9064 1.4754 2.4893 1.8336.3643.3035.1457-.1032.0182-.0728-.164-.2733-1.3539-2.4467-1.445-2.4893-.6435-1.032-.17-.6194c-.0607-.255-.1032-.4674-.1032-.7285L6.287.1335 6.6997 0l.9957.1336.419.3642.6192 1.4147 1.0018 2.2282 1.5543 3.0296.4553.8985.2429.8318.091.255h.1579v-.1457l.1275-1.706.2368-2.0947.2307-2.6957.0789-.7589.3764-.9107.7468-.4918.5828.2793.4797.686-.0668.4433-.2853 1.8517-.5586 2.9021-.3643 1.9429h.2125l.2429-.2429.9835-1.3053 1.6514-2.0643.7286-.8196.85-.9046.5464-.4311h1.0321l.759 1.1293-.34 1.1657-1.0625 1.3478-.8804 1.1414-1.2628 1.7-.7893 1.36.0729.1093.1882-.0183 2.8535-.607 1.5421-.2794 1.8396-.3157.8318.3886.091.3946-.3278.8075-1.967.4857-2.3072.4614-3.4364.8136-.0425.0304.0486.0607 1.5482.1457.6618.0364h1.621l3.0175.2247.7892.522.4736.6376-.079.4857-1.2142.6193-1.6393-.3886-3.825-.9107-1.3113-.3279h-.1822v.1093l1.0929 1.0686 2.0035 1.8092 2.5075 2.3314.1275.5768-.3218.4554-.34-.0486-2.2039-1.6575-.85-.7468-1.9246-1.621h-.1275v.17l.4432.6496 2.3436 3.5214.1214 1.0807-.17.3521-.6071.2125-.6679-.1214-1.3721-1.9246L14.38 17.959l-1.1414-1.9428-.1397.079-.674 7.2552-.3156.3703-.7286.2793-.6071-.4614-.3218-.7468.3218-1.4753.3886-1.9246.3157-1.53.2853-1.9004.17-.6314-.0121-.0425-.1397.0182-1.4328 1.9672-2.1796 2.9446-1.7243 1.8456-.4128.164-.7164-.3704.0667-.6618.4008-.5889 2.386-3.0357 1.4389-1.882.929-1.0868-.0062-.1579h-.0546l-6.3385 4.1164-1.1293.1457-.4857-.4554.0608-.7467.2307-.2429 1.9064-1.3114Z",
			chrome: "M12 0C8.21 0 4.831 1.757 2.632 4.501l3.953 6.848A5.454 5.454 0 0 1 12 6.545h10.691A12 12 0 0 0 12 0zM1.931 5.47A11.943 11.943 0 0 0 0 12c0 6.012 4.42 10.991 10.189 11.864l3.953-6.847a5.45 5.45 0 0 1-6.865-2.29zm13.342 2.166a5.446 5.446 0 0 1 1.45 7.09l.002.001h-.002l-5.344 9.257c.206.01.413.016.621.016 6.627 0 12-5.373 12-12 0-1.54-.29-3.011-.818-4.364zM12 16.364a4.364 4.364 0 1 1 0-8.728 4.364 4.364 0 0 1 0 8.728Z",
			gmail: "M24 5.457v13.909c0 .904-.732 1.636-1.636 1.636h-3.819V11.73L12 16.64l-6.545-4.91v9.273H1.636A1.636 1.636 0 0 1 0 19.366V5.457c0-2.023 2.309-3.178 3.927-1.964L5.455 4.64 12 9.548l6.545-4.91 1.528-1.145C21.69 2.28 24 3.434 24 5.457z",
			openai: "M22.2819 9.8211a5.9847 5.9847 0 0 0-.5157-4.9108 6.0462 6.0462 0 0 0-6.5098-2.9A6.0651 6.0651 0 0 0 4.9807 4.1818a5.9847 5.9847 0 0 0-3.9977 2.9 6.0462 6.0462 0 0 0 .7427 7.0966 5.98 5.98 0 0 0 .511 4.9107 6.051 6.051 0 0 0 6.5146 2.9001A5.9847 5.9847 0 0 0 13.2599 24a6.0557 6.0557 0 0 0 5.7718-4.2058 5.9894 5.9894 0 0 0 3.9977-2.9001 6.0557 6.0557 0 0 0-.7475-7.0729zm-9.022 12.6081a4.4755 4.4755 0 0 1-2.8764-1.0408l.1419-.0804 4.7783-2.7582a.7948.7948 0 0 0 .3927-.6813v-6.7369l2.02 1.1686a.071.071 0 0 1 .038.052v5.5826a4.504 4.504 0 0 1-4.4945 4.4944zm-9.6607-4.1254a4.4708 4.4708 0 0 1-.5346-3.0137l.142.0852 4.783 2.7582a.7712.7712 0 0 0 .7806 0l5.8428-3.3685v2.3324a.0804.0804 0 0 1-.0332.0615L9.74 19.9502a4.4992 4.4992 0 0 1-6.1408-1.6464zM2.3408 7.8956a4.485 4.485 0 0 1 2.3655-1.9728V11.6a.7664.7664 0 0 0 .3879.6765l5.8144 3.3543-2.0201 1.1685a.0757.0757 0 0 1-.071 0l-4.8303-2.7865A4.504 4.504 0 0 1 2.3408 7.872zm16.5963 3.8558L13.1038 8.364 15.1192 7.2a.0757.0757 0 0 1 .071 0l4.8303 2.7913a4.4944 4.4944 0 0 1-.6765 8.1042v-5.6772a.79.79 0 0 0-.407-.667zm2.0107-3.0231l-.142-.0852-4.7735-2.7818a.7759.7759 0 0 0-.7854 0L9.409 9.2297V6.8974a.0662.0662 0 0 1 .0284-.0615l4.8303-2.7866a4.4992 4.4992 0 0 1 6.6802 4.66zM8.3065 12.863l-2.02-1.1638a.0804.0804 0 0 1-.038-.0567V6.0742a4.4992 4.4992 0 0 1 7.3757-3.4537l-.142.0805L8.704 5.459a.7948.7948 0 0 0-.3927.6813zm1.0976-2.3654l2.602-1.4998 2.6069 1.4998v2.9994l-2.5974 1.4997-2.6067-1.4997Z",
			spark: "M12 2.5l2.4 7.1 7.1 2.4-7.1 2.4-2.4 7.1-2.4-7.1L2.5 12l7.1-2.4z",
			mail: "M2 5.5A1.5 1.5 0 0 1 3.5 4h17A1.5 1.5 0 0 1 22 5.5v13a1.5 1.5 0 0 1-1.5 1.5h-17A1.5 1.5 0 0 1 2 18.500v-13zm2.3.5 7.7 6.2L19.7 6H4.3zM20 7.300l-8 6.400-8-6.400V18h16V7.300z",
		};
		const APP_ICONS = {
			claude: { d: "claude", bg: "#F7ECE6", fg: "#D97757" },
			claudeCode: { d: "claude", bg: "#D97757", fg: "#fff" },
			codex: { d: "openai", bg: "#111", fg: "#fff" },
			chrome: { d: "chrome", bg: "#fff", fg: "#4285F4" },
			gmail: { d: "gmail", bg: "#fff", fg: "#EA4335" },
			mail: { d: "mail", bg: "#2F7BF6", fg: "#fff" },
			mira: { sticker: "memory" },
			feishu: { bg: "#fff", shapes: [["M7.300 3.500h7.400c1.700 1.900 2.800 4.100 3.300 6.500-1.700-.300-3.300.100-4.700 1C12.300 8 10.200 5.300 7.300 3.500z", "#00D6B9"], ["M2.500 9.200c2.800 3 6.200 5 9.900 5.600 2.600.400 4.900-.400 6.700-2.200.900-.900 1.700-1.700 2.900-2.100-2.200-1-4.600-.700-6.500.700-1.300 1-2.500 1.900-4.200 1.800C8.400 12.900 5.200 11.500 2.500 9.200z", "#3370FF"], ["M2.500 9.200v7.600c0 .500.300 1 .700 1.200 3.800 2.100 8.600 1.700 11.900-1.100 1.200-1 2-2.200 2.700-3.500-1.600 1.300-3.500 1.700-5.400 1.400C8.700 14.200 5.300 12.200 2.500 9.200z", "#133C9A"]] },
			finder: { bg: "#3B9DF8", shapes: [["M0 0h12.600c-1.500 3.600-2.300 7.500-2.300 11.500h3.200c.300 4.300 0 8.500-.500 12.500H0z", "#E9F3FF"], ["M6.300 7.500h1.400v3H6.300zM16.300 7.500h1.400v3h-1.400zM5.800 16.200c3.800 2.600 8.600 2.600 12.400 0l.800 1.200c-4.300 2.900-9.700 2.900-14 0z", "#14365F"]], full: true },
			kimi: { letter: "K", bg: "#111", fg: "#fff" },
		};
		const APP_ICON_NAMES = { claude: "claude", "claude code": "claudeCode", codex: "codex", chatgpt: "codex", "google chrome": "chrome", chrome: "chrome", gmail: "gmail", "邮件": "mail", mail: "mail", "飞书": "feishu", feishu: "feishu", lark: "feishu", "访达": "finder", finder: "finder", "kimi code": "kimi", kimi: "kimi", "助理": "mira", "个人助理": "mira", mira: "mira" };
		/** How a source or an app is drawn: a known mark, or the first letter on a tile whose colour follows the name. */
		function iconSpec(name) {
			const text = String(name || "").trim(), known = APP_ICONS[APP_ICON_NAMES[text.toLowerCase()]];
			if (known) return known;
			let hash = 0;
			for (const ch of text) hash = (hash * 31 + ch.codePointAt(0)) % 360;
			return { letter: ([...text][0] || "·").toUpperCase(), bg: `hsl(${hash} 42% 46%)`, fg: "#fff" };
		}
		function AppIcon({ name, size = 16 }) {
			const spec = iconSpec(name), inner = Math.round(size * (spec.full ? 1 : 0.68));
			// Mira is a character, not a brand: her own pixel slime, as on her avatar.
			if (spec.sticker) return h(MiraAvatar, { size, title: name });
			return h("span", { className: "mp4-app", title: name, "aria-hidden": true, style: { width: size, height: size, background: spec.bg, color: spec.fg, fontSize: Math.round(size * 0.58) } },
				spec.letter ? spec.letter : h("svg", { viewBox: "0 0 24 24", width: inner, height: inner, fill: "currentColor" },
					spec.shapes ? spec.shapes.map(([d, fill], i) => h("path", { key: i, d, fill })) : h("path", { d: APP_ICON_PATHS[spec.d], fillRule: "evenodd" })));
		}
		/** The personal assistant wherever it appears as a source: its pixel figure, as the owner made it. */
		function MiraAvatar({ size = 16, title }) {
			return h("span", { className: "mp4-app mp4-app-sticker", title, "aria-hidden": true, style: { width: size, height: size } }, h(MiraPortrait, { size }));
		}
		function Dot({ tone }) { return h("i", { className: "mp4-dot mp4-dot-" + tone, "aria-hidden": true }); }
		const allTasks = snap => (snap.projects || []).flatMap(p => (p.threads || []).map(t => ({ p, t })));
		/** A task's Session: the one it is bound to, or, while Claude Code / Codex has the task, the DSH Session it goes back to. */
		const ownsSession = (t, sessionId) => t.sessionId === sessionId || t.dshSessionId === sessionId;

		/** The project task a DSH Session belongs to: navigation row first, full detail once loaded. */
		function useTaskOf(sessionId) {
			const all = useSnapshot();
			const owner = sessionId ? allTasks(all).find(x => ownsSession(x.t, sessionId)) : undefined;
			const detail = useSnapshot(owner ? owner.p.id : undefined);
			const project = owner && detail.project && detail.project.id === owner.p.id ? detail.project : null;
			const thread = project ? project.threads.find(t => t.id === owner.t.id) || null : null;
			return { loaded: all.loaded, owner, project, thread };
		}

		/**
		 * Open a project the DSH way: the project conversation is the project assistant's own Session (made on first open),
		 * with the project's tabs on the right. Falls back to the plugin's project page.
		 */
		async function openProject(projectId) {
			writePref("project", projectId);
			if (shell.workspace) {
				try {
					const { sessionId } = await act("openChat", { projectId });
					shell.workspace.openSession(sessionId);
					revealTabs(sessionId, [PROJECT_TASKS_KIND]);
					return sessionId;
				} catch (_e) { /* fall through to the project page */ }
			}
			navigateProject(projectId);
		}

		/** Who runs a task when it is not a DSH Session: Claude Code or Codex on this machine. Such a task has no Session to open. */
		const RUNNER_NAMES = { claude: "Claude Code", codex: "Codex" };
		const isOutside = thread => Boolean(thread && thread.runtime && thread.runtime.provider !== "dsh");
		const EXECUTOR_NAMES = { dsh: "DSH", ...RUNNER_NAMES };
		/** A model id as people read it: "deepseek-flash" → "DeepSeek Flash", "gpt-5-4" → "GPT 5.4". */
		function modelName(model) {
			const id = typeof model === "string" ? model : model && (model.model || model.id);
			if (!id) return "默认模型";
			const fam = FAMILIES.find(f => f.test.test(id)), out = [];
			for (const w of String(id).split("/").pop().split(/[-_\s]+/).filter(Boolean)) {
				if (/^\d+$/.test(w) && /\d$/.test(out[out.length - 1] || "")) { out[out.length - 1] += "." + w; continue; }
				out.push(fam && w.toLowerCase() === fam.id ? fam.label : w[0].toUpperCase() + w.slice(1));
			}
			return out.join(" ");
		}
		/**
		 * Who executes a task, as the owner sees it: Claude Code or Codex when one of them runs it, else the model it runs
		 * on. The project assistant's per-task roles stay in the data; the owner sees only what does the work.
		 */
		function executorOf(project, thread) {
			const members = (project && project.members) || [], member = members.find(m => m.id === thread.assignee);
			const outside = project && project.members ? outsideRunner(project, thread) : isOutside(thread) ? thread.runtime.provider : null;
			// The task's own pick or its session's known model first; never DSH's default dressed up as what ran.
			const own = thread.modelSelection || thread.model || null;
			const known = member && member.model && member.model.source !== "default" ? member.model : null;
			const model = own || known || (member ? member.modelSelection || null : null);
			return outside ? { name: RUNNER_NAMES[outside] || outside, app: RUNNER_NAMES[outside] || outside, model } : { name: model && (member || own) ? modelName(model) : member ? "DSH" : "", model };
		}
		/** The executor every new task goes to (2026-11-13): id `executor`, or `executor_…` when the first was removed. */
		const isProjectExecutor = member => Boolean(member && member.id && (member.id === "executor" || member.id.startsWith("executor_")));
		/** What a member is called on screen: the project and memory assistants by their role, an executor by what runs it. */
		function shownName(member, fallback) {
			if (!member || member.kind !== "assistant" || member.role !== "worker") return (member && member.name) || fallback || "";
			const runner = member.runtimeSelection && member.runtimeSelection.provider;
			return runner && runner !== "dsh" ? RUNNER_NAMES[runner] || runner : modelName(member.model || member.modelSelection);
		}
		/** The outside runner a task's next round goes to: its own choice, else the one it is bound to, else its assignee's default. Null for DSH. */
		function outsideRunner(project, thread) {
			const provider = thread.runner || (thread.runtime ? thread.runtime.provider : (project.members || []).find(m => m.id === thread.assignee)?.runtimeSelection?.provider);
			return provider && provider !== "dsh" ? provider : null;
		}
		/** Whether a task's conversation is shown by the plugin (no DSH Session to open): an outside runner has it or is about to. */
		const showsOutside = (project, thread) => Boolean(outsideRunner(project, thread)) || isOutside(thread);
		/** Copy `text`, shown in the <code> before the button; where the host denies clipboard writes, select it for ⌘C. */
		function copyCode(ev, text, done) {
			const code = ev.currentTarget.previousSibling;
			const select = () => { const range = document.createRange(); range.selectNodeContents(code); const sel = window.getSelection(); sel.removeAllRanges(); sel.addRange(range); let ok = false; try { ok = document.execCommand("copy"); } catch (_e) {} done(ok ? "已复制" : "已选中，按 ⌘C"); };
			if (navigator.clipboard) navigator.clipboard.writeText(text).then(() => done("已复制"), select); else select();
		}
		/** Open a task the DSH way: its bound Session, with the plugin's tabs on the right. Falls back to the project. */
		function openTask(projectId, thread) {
			if (projectId) writePref("project", projectId);
			// E2E #27: a task handed to Claude Code or Codex keeps the DSH conversation it had. Opening the task stays in that
			// conversation (its panel shows the progress, its composer passes words on) instead of the plugin's older page.
			const sessionId = thread && (isOutside(thread) ? thread.dshSessionId : thread.sessionId);
			if (sessionId && shell.workspace) {
				try {
					shell.workspace.openSession(sessionId);
					revealTabs(sessionId, thread.state === "review_ready" ? [TASK_TAB_KIND, CHANGES_TAB_KIND] : [TASK_TAB_KIND]);
					return;
				} catch (_e) { /* fall through to the project page */ }
			}
			navigateProject(projectId, thread && thread.id);
		}

		/** Whether a right panel can stand beside the conversation; below 900px it is laid over it (see .mp4-panel). */
		function panelFits() { return !(window.matchMedia && window.matchMedia("(max-width:900px)").matches); }
		/**
		 * Once the Session is on screen, open the given tabs (the last one ends up in front) and expand the panel.
		 * The right panel acts on the on-screen Session and throws until its store exists (a render later), so retry.
		 */
		function revealTabs(sessionId, kinds) {
			const right = shell.sidebar, mounted = right && right.mounted;
			if (!right || !mounted || typeof mounted.getSnapshot !== "function") return;
			let tries = 0;
			const attempt = () => {
				tries += 1;
				if (mounted.getSnapshot() === sessionId) {
					try {
						for (const kind of kinds) right.openTab(kind, { revealIfOpened: true });
						if (typeof right.isExpanded === "function" && !right.isExpanded()) right.toggleExpanded();
						return;
					} catch (_e) { /* this Session's panel is not ready yet */ }
				}
				if (tries < 30) window.setTimeout(attempt, 150);
			};
			attempt();
		}

		// ---------------------------------------------------------------- shared frame

		function MoreMenu({ items }) {
			const [open, setOpen] = useState(false), ref = useRef(null);
			useEffect(() => {
				if (!open) return;
				const close = e => { if (!ref.current?.contains(e.target)) setOpen(false); };
				document.addEventListener("pointerdown", close); return () => document.removeEventListener("pointerdown", close);
			}, [open]);
			return h("span", { className: "mp4-more", ref },
				h("button", { type: "button", className: "mp4-icon", "aria-label": "更多", "aria-expanded": open, onClick: () => setOpen(v => !v) }, h(Icon, { name: "more", size: 18 })),
				open ? h("div", { className: "mp4-menu", role: "menu" }, items.filter(Boolean).map(([label, run]) => h("button", { key: label, type: "button", role: "menuitem", onClick: () => { setOpen(false); run(); } }, label))) : null);
		}

		/** Header, conversation, composer — the DSH session layout — plus the right panel when it is open. */
		function SessionFrame({ crumb, title, pill, where, menu, scroll, body, composer, panel, panelOpen, onShowPanel }) {
			return h("div", { className: "mp4-frame" },
				h("section", { className: "mp4-main", "aria-label": title },
					h("header", { className: "mp4-head" },
						h("div", { className: "mp4-head-text" },
							crumb ? h("span", { className: "mp4-crumb" }, h("button", { type: "button", onClick: crumb.onClick }, crumb.label), h("span", { "aria-hidden": true }, "/"), pill ? h(Pill, { tone: pill[1] }, pill[0]) : null) : null,
							h("h1", null, title)),
						where ? h("span", { className: "mp4-where" }, h(Dot, { tone: "idle" }), where) : null,
						menu ? h(MoreMenu, { items: menu }) : null,
						panelOpen ? null : h("button", { type: "button", className: "mp4-icon", "aria-label": "展开右侧面板", onClick: onShowPanel }, h(Icon, { name: "panel", size: 16 }))),
					h("div", { className: "mp4-scroll", ...(scroll || {}) }, h("div", { className: "mp4-column" }, body)),
					h("div", { className: "mp4-composer" }, composer)),
				panelOpen ? panel : null);
		}

		/** Tabs that open from the 「+」 start page, close with ×, and are remembered per place. */
		function usePanelTabs(place, defaults) {
			const key = "panel:" + place;
			const [state, setState] = useState(() => { try { const saved = JSON.parse(readPref(key) || "null"); if (saved && Array.isArray(saved.tabs) && saved.tabs.length) return saved; } catch (_) {} return { tabs: defaults, active: defaults[0] }; });
			useEffect(() => { writePref(key, JSON.stringify(state)); }, [key, state]);
			return {
				tabs: state.tabs, active: state.active,
				open: id => setState(s => ({ tabs: s.tabs.includes(id) ? s.tabs : [...s.tabs, id], active: id })),
				select: id => setState(s => ({ ...s, active: id })),
				close: id => setState(s => { const tabs = s.tabs.filter(t => t !== id); return { tabs, active: s.active === id ? (tabs.at(-1) || "start") : s.active }; }),
			};
		}
		function SidePanel({ tabs, info, guides, onHide, children, wide, onWide }) {
			const list = useRef(null);
			useEffect(() => { list.current?.querySelector(".mp4-tab-on")?.scrollIntoView({ block: "nearest", inline: "nearest" }); }, [tabs.active, tabs.tabs.length]);
			return h("aside", { className: "mp4-panel" + (wide ? " mp4-panel-wide" : ""), "aria-label": "右侧面板" },
				h("div", { className: "mp4-tabs" },
					h("div", { className: "mp4-tabs-list", ref: list }, tabs.tabs.map(id => h("span", { key: id, className: "mp4-tab" + (tabs.active === id ? " mp4-tab-on" : "") },
						h("button", { type: "button", onClick: () => tabs.select(id) }, info[id]?.icon ? h(Icon, { name: info[id].icon, size: 14 }) : null, info[id]?.label || id),
						h("button", { type: "button", className: "mp4-tab-x", "aria-label": `关闭${info[id]?.label || ""}`, onClick: () => tabs.close(id) }, "×")))),
					h("button", { type: "button", className: "mp4-icon", "aria-label": "打开工具", onClick: () => tabs.select("start") }, h(Icon, { name: "plus", size: 15 })),
					h("span", { className: "mp4-tabs-end" },
						h("button", { type: "button", className: "mp4-icon", "aria-label": wide ? "恢复宽度" : "加宽面板", onClick: onWide }, h(Icon, { name: "widen", size: 15 })),
						h("button", { type: "button", className: "mp4-icon", "aria-label": "收起右侧面板", onClick: onHide }, h(Icon, { name: "panel", size: 15 })))),
				// The three core views share DESIGN.md's components here too (the fallback page when her Session is not open).
				h("div", { className: "mp4-panel-body" + (["inbox", "time", "tree"].includes(tabs.active) ? " mp4-core" : "") }, tabs.active === "start" || !tabs.tabs.includes(tabs.active)
					? h("div", { className: "mp4-start" }, guides.map(g => h("button", { key: g.id, type: "button", className: "mp4-guide", onClick: () => tabs.open(g.id) },
						h("span", { className: "mp4-guide-icon" }, h(Icon, { name: g.icon, size: 16 })), h("span", null, h("b", null, g.label)))))
					: children));
		}
		function Section({ label, tone, children }) {
			return h("section", { className: "mp4-sec" }, label ? h("p", { className: "mp4-k" + (tone ? " mp4-k-" + tone : "") }, label) : null, children);
		}

		// ---------------------------------------------------------------- left column (every screen)

		function WorkspaceProjects({ wide, expandSidebar }) {
			// A sub-agent counts as the Session that sent it out, so the place it belongs to stays marked.
			const snap = useSnapshot(), hash = useRoute(), onScreen = useOnScreenSession(), here = useRootSession(onScreen);
			shell.expandSidebar = expandSidebar || null;
			if (!wide) {
				const railBtn = h("button", { type: "button", className: "mp-sidebar-switch mp-sidebar-switch-rail", "aria-label": "新建项目", title: P.Tooltip ? undefined : "新建项目", onClick: () => { expandSidebar?.(); window.dispatchEvent(new CustomEvent("mira-projects:create")); } }, h(Icon, { name: "plus", size: 18 }));
				return h("div", { className: "mp-workspaces-rail" }, P.Tooltip ? h(P.Tooltip, { label: "新建项目", side: "right", delayMs: 500, portal: true }, railBtn) : railBtn);
			}
			const route = readRoute();
			const projectRows = snap.projects.map(p => {
				const threads = p.threads || [];
				const waiting = threads.filter(isWaiting).length, running = threads.filter(isRunning).length;
				// The project conversation is the project assistant's Session; the plugin page is the fallback.
				// Inside one of the project's tasks the row stays marked, lighter: the task's own row in 最近 is the selected one.
				const selected = onScreen ? here === p.chatSessionId : route?.projectId === p.id;
				const within = Boolean(onScreen) && !selected && threads.some(t => t.sessionId === here);
				return h("div", { key: p.id, className: "mp-workspace-row" },
					h("button", { type: "button", className: "mp-workspace-project" + (selected ? " mp-workspace-selected" : within ? " mp-workspace-within" : ""), "aria-current": selected ? "page" : within ? "true" : undefined, onClick: () => openProject(p.id) },
						h(ProjectMark, { p, size: 16, plain: true }), h("span", null, p.name),
						waiting ? h("small", { className: "mp-workspace-meta mp-workspace-meta-wait" }, `${waiting} 待处理`) : running ? h("small", { className: "mp-workspace-meta" }, `${running} 进行中`) : null));
			});
			// 最近 holds conversations only (tasks); a project's own chat is reached from its row above.
			const recent = allTasks(snap).filter(({ t }) => !t.resolved)
				.map(({ p, t }) => ({ key: p.id + t.id, at: t.updatedAt || 0, title: t.title, hint: parts(p.name, taskPill(t)[0]), status: taskStatus(t), selected: Boolean(onScreen && here === t.sessionId), open: () => openTask(p.id, t) }))
				.sort((a, b) => b.at - a.at).slice(0, 5);
			void hash;
			return h("nav", { className: "mp-workspaces", "aria-label": "个人助理与项目" },
				h("div", { className: "mp-workspaces-head" }, h("span", null, "项目"), h("span", { className: "mp-workspaces-acts" }, h("button", { type: "button", "aria-label": "切换到原有工作区与对话", title: "切换到原有工作区与对话", onClick: () => shell.showNative?.() }, h(Icon, { name: "folder", size: 16 })), h("button", { type: "button", "aria-label": "新建项目", title: "新建项目", onClick: () => window.dispatchEvent(new CustomEvent("mira-projects:create")) }, h(Icon, { name: "plus", size: 16 })))),
				projectRows,
				!snap.projects.length ? h("div", { className: "mp-workspace-empty" }, "还没有项目，点上面的 + 新建") : null,
				recent.length ? h("div", { className: "mp-workspaces-head" }, h("span", null, "最近")) : null,
				recent.map(r => h("button", { key: r.key, type: "button", className: "mp-workspace-thread mp-workspace-recent" + (r.selected ? " mp-workspace-selected" : ""), onClick: r.open },
					h(StatusIcon, { status: r.status }),
					h("span", null, h("span", { className: "mp-workspace-recent-title" }, r.title), h("small", { className: "mp-workspace-recent-meta" }, r.hint)))),
				snap.error ? h("p", { role: "status", className: "mp-workspace-empty" }, "正在重新连接") : null);
		}

		/**
		 * Mira in the host's line style: her round body drawn with the same 1.6 px stroke as DSH's nav icons, closed eyes,
		 * a sprout, and her yellow inside — the one bit of colour in the list.
		 */
		/** The assistant in the host's line style, in the owner's colour, body and accessory (the nav entry and other line places). */
		const LINE_BODIES = {
			mochi: { d: "M4 18.2C4 12.6 7.6 8.2 12 8.2s8 4.4 8 10c0 .7-.5 1.2-1.2 1.2H5.2c-.7 0-1.2-.5-1.2-1.2Z" },
			round: { d: "M12 8.6c4.2 0 7.4 2.6 7.4 5.6s-3.2 5.4-7.4 5.4-7.4-2.4-7.4-5.4 3.2-5.6 7.4-5.6Z" },
			block: { d: "M6.4 8.6h11.2a2.2 2.2 0 0 1 2.2 2.2v6.4a2.2 2.2 0 0 1-2.2 2.2H6.4a2.2 2.2 0 0 1-2.2-2.2v-6.4a2.2 2.2 0 0 1 2.2-2.2Z" },
		};
		const LINE_ACCESSORIES = {
			sprout: ["M12 8.2V5.6M12 5.6c0-1.4 1-2.3 2.4-2.3 0 1.4-1 2.3-2.4 2.3Z"],
			antenna: ["M12 8.4V5", "M12 2.6a1.2 1.2 0 1 1 0 2.4 1.2 1.2 0 0 1 0-2.4Z"],
			bow: ["M15.2 7.4l-2.4-1.8v3.6zM15.2 7.4l2.4-1.8v3.6z"],
			cap: ["M6.8 9.2c.7-2.4 2.8-4 5.2-4s4.5 1.6 5.2 4M17 9.2h2.8"],
			headphones: ["M5.2 13.6V12a6.8 6.8 0 0 1 13.6 0v1.6", "M4 13.2h1.8v3.4H4zM18.2 13.2H20v3.4h-1.8z"],
			glasses: [],
			none: [],
		};
		function MiraLineIcon({ size = 20 }) {
			const look = { ...LOOK_DEFAULT, ...(usePersonal().look || {}) };
			const fill = LOOK_FILLS[look.color] || LOOK_FILLS.yellow, body = LINE_BODIES[look.shape] || LINE_BODIES.mochi;
			const eyes = look.accessory === "glasses" ? "M9.4 12.6a1.6 1.6 0 1 1 0 3.2 1.6 1.6 0 0 1 0-3.2ZM14.6 12.6a1.6 1.6 0 1 1 0 3.2 1.6 1.6 0 0 1 0-3.2ZM11 14.2h2" : "M8.6 14.4q1-1 2 0M13.4 14.4q1-1 2 0";
			return h("svg", { width: size, height: size, viewBox: "0 0 24 24", fill: "none", stroke: "currentColor", strokeWidth: 1.6, strokeLinecap: "round", strokeLinejoin: "round", "aria-hidden": true, className: "mp4-mira-line" },
				h("path", { d: body.d, fill, fillOpacity: 0.25 }),
				h("path", { d: eyes }),
				(LINE_ACCESSORIES[look.accessory] || []).map((d, i) => h("path", { key: i, d })));
		}
		/** The assistant in the nav: its dot, a little inside the box the host's line icons use. */
		function MiraEntryDot({ size = 20 }) {
			return h("span", { className: "mp4-entry-dot", style: { width: size, height: size } }, h(MiraPortrait, { size }));
		}
		/** The assistant's pixel avatar at the head of its own conversation; nothing in other conversations. */
		function MiraHeaderAvatar({ sessionId }) {
			const personal = usePersonal();
			if (!sessionId || personal.sessionId !== sessionId) return null;
			return h("span", { className: "mp4-head-avatar", title: personal.look && personal.look.name || "个人助理" }, h(MiraPortrait, { size: 22 }));
		}
		/** The Mira entry in DSH's panel list, with how many things wait for the owner. */
		function MiraEntryIcon({ size }) {
			return h("span", { className: "mp4-entry", "data-mira-entry": "" }, h(MiraEntryDot, { size: size || 20 }));
		}

		// ---------------------------------------------------------------- 项目: every project, then one project's conversation

		function ProjectsIndex({ snap, onCreate }) {
			const rows = [...snap.projects].sort((a, b) => Number((b.threads || []).some(isWaiting)) - Number((a.threads || []).some(isWaiting))).map(p => {
				const threads = p.threads || [];
				const last = [...threads].sort((a, b) => (b.updatedAt || 0) - (a.updatedAt || 0))[0];
				return { p, waiting: threads.filter(isWaiting).length, running: threads.filter(isRunning).length, done: threads.filter(t => t.state === "done").length,
					time: last ? clock(last.updatedAt) : p.updatedAt ? clock(p.updatedAt) : "", last };
			});
			return h("div", { className: "mp4-index" }, h("div", { className: "mp4-index-col" },
				h("div", { className: "mp4-index-head" }, h("h1", null, "项目"),
					h("button", { type: "button", className: "mp4-btn", onClick: onCreate }, h(Icon, { name: "plus", size: 14 }), "新项目")),
				rows.length === 0 ? h("p", { className: "mp4-muted" }, "还没有项目。") : h("div", { className: "mp4-plist" }, rows.map(r => h("button", { key: r.p.id, type: "button", className: "mp4-prow", title: r.p.desc ? r.p.desc.replace(/^目标[:：]\s*/, "") : undefined, onClick: () => openProject(r.p.id) },
					h(ProjectMark, { p: r.p, size: 16, plain: true }),
					h("span", { className: "mp4-prow-main" }, h("span", { className: "mp4-prow-name" }, r.p.name),
						h("span", { className: "mp4-prow-desc" }, r.last ? [h(StatusIcon, { key: "s", status: taskStatus(r.last), size: 12 }), h("span", { key: "t", className: "mp4-ell" }, r.last.title), h("span", { key: "w", className: "mp4-prow-state" }, taskPill(r.last)[0])] : "还没有任务")),
					h("span", { className: "mp4-prow-meta" },
						r.waiting ? h("span", { className: "mp4-prow-wait" }, h(StatusIcon, { status: "decision", size: 12 }), `${r.waiting} 待处理`) : null,
						r.running ? h("span", null, h(StatusIcon, { status: "working", size: 12 }), `${r.running} 进行中`) : null,
						h("span", { className: "mp4-time" }, r.time)))))));
		}

		function ProjectTasks({ project, onOpen, activeId }) {
			// One line per task, as Codex lists its chats: status icon, title, time; the state and summary show on hover and in 任务详情.
			const row = t => h("button", { key: t.id, type: "button", className: "mp4-row mp4-row-line" + (activeId === t.id ? " mp4-row-on" : ""), title: [t.title, t.experience?.reason || taskPill(t)[0], t.summary].filter(Boolean).join("\n"), onClick: () => onOpen(t.id) },
				h(StatusIcon, { status: taskStatus(t) }),
				h("span", { className: "mp4-ell mp4-row-title" }, t.title),
				h("span", { className: "mp4-time mp4-muted" }, t.updatedAt ? clock(t.updatedAt) : ""));
			const waiting = project.threads.filter(isWaiting), running = project.threads.filter(isRunning), done = project.threads.filter(t => t.state === "done"), archived = project.threads.filter(t => t.resolved && t.state !== "done");
			return h(React.Fragment, null,
				waiting.length ? h(Section, { label: `待处理 ${waiting.length}` }, waiting.map(row)) : null,
				running.length ? h(Section, { label: `进行中 ${running.length}` }, running.map(row)) : null,
				done.length ? h(Section, { label: `已完成 ${done.length}` }, done.map(row)) : null,
				waiting.length + running.length + done.length + archived.length === 0 ? h("p", { className: "mp4-muted" }, "还没有任务") : null,
				archived.length ? h("details", { className: "mp4-fold" }, h("summary", null, `已归档 ${archived.length}`), archived.map(row)) : null);
		}

		function ProjectMemory({ project }) {
			const memory = project.memory || {};
			const list = items => (items || []).length ? (items || []).map((item, i) => h("div", { key: i, className: "mp4-li" }, item.text)) : h("p", { className: "mp4-muted" }, "没有");
			const repo = project.gitlab ? `GitLab ${project.gitlab.project || project.gitlab.projectPath || ""}，${project.gitlab.targetBranch || project.gitlab.branch || "main"}` : project.repository ? `仓库快照 v${project.repository.revision}，${String(project.repository.commit || "").slice(0, 12)}` : project.cwd || "没有绑定仓库";
			const empty = !(memory.decisions || []).length && !(memory.open_loops || []).length && !(memory.facts || []).length;
			return h(React.Fragment, null,
				empty ? h("p", { className: "mp4-muted mp4-note" }, "记忆助理还没记下什么。项目里有了决定、没收尾的事，它会记在这里。") : null,
				(memory.decisions || []).length ? h(Section, { label: `做过的决定 ${memory.decisions.length}` }, list(memory.decisions)) : null,
				(memory.open_loops || []).length ? h(Section, { label: `没收尾 ${memory.open_loops.length}` }, list(memory.open_loops)) : null,
				(memory.facts || []).length ? h("details", { className: "mp4-fold" }, h("summary", null, `长期事实 ${memory.facts.length}`), list(memory.facts)) : null,
				h("details", { className: "mp4-fold" }, h("summary", null, "项目说明和仓库"), h("section", { className: "mp4-grid mp4-fold-body" }, h("span", { className: "mp4-k" }, "说明"), h("span", null, project.instructions || "还没写项目说明"), h("span", { className: "mp4-k" }, "仓库"), h("span", { className: "mp4-mono" }, repo))));
		}

		/** What one task is and where it stands — the right-panel 「任务」 tab, also used as a preview before it starts. */
		/** A heading and one bordered card around its rows, as in Codex's settings. */
		function CardGroup({ label, children }) {
			return h("div", { className: "mp4-setgroup" }, h("h4", null, label), h("div", { className: "mp4-setlist" }, children));
		}
		function TaskSummary({ project, thread, inTask }) {
			const [accepting, setAccepting] = useState(false), [acceptError, setAcceptError] = useState(null);
			const exp = thread.experience || {}, summary = exp.summary || {};
			const checks = thread.checklist || [], done = checks.filter(c => c.done).length, files = deliveredFiles(project, thread);
			const related = thread.relatedThreadId ? project.threads.find(t => t.id === thread.relatedThreadId) : null;
			const asked = (thread.cited || []).filter(m => m.author?.kind === "human" && m.text);
			const delivered = thread.state === "review_ready" || thread.state === "done";
			const ownSession = isOutside(thread) ? thread.dshSessionId : thread.sessionId;
			const showChanges = () => {
				if (!inTask) return openTask(project.id, thread);
				try { shell.sidebar?.openTab(CHANGES_TAB_KIND, { revealIfOpened: true }); } catch (_e) { /* the panel is not ready */ }
			};
			const mr = thread.gitlab, mergeReady = !project.gitlab || ["ready", "merge_error"].includes(mr?.status);
			const accept = async () => {
				setAccepting(true); setAcceptError(null);
				try { await act("updateThread", { projectId: project.id, threadId: thread.id, resolved: true, state: "done", expectedSha: mr?.sha }); } catch (e) { setAcceptError(e.message); } finally { setAccepting(false); }
			};
			const reviewAction = thread.state !== "review_ready" || thread.resolved ? null
				: inTask || ownSession ? h("button", { type: "button", className: "mp4-btn mp4-btn-strong", onClick: showChanges }, "查看交付")
				: h("button", { type: "button", className: "mp4-btn mp4-btn-strong", disabled: accepting || !mergeReady, onClick: accept }, project.gitlab ? (mr?.status === "merge_error" ? "重试合并" : "验收通过并合并") : "验收通过");
			const checkRows = checks.map((c, i) => h("div", { key: i, className: "mp4-check" + (c.done ? " mp4-check-done" : "") }, h("span", { className: "mp4-check-mark" }, c.done ? h(Icon, { name: "check", size: 14 }) : h("i", { className: "mp4-check-todo" })), h("span", null, c.text)));
			const brief = thread.brief || summary.goal;
			// A board of what the owner needs, in the order they read it: where it stands, why it exists, what came out, how it went.
			return h(React.Fragment, null,
				h(TaskBanner, { project, thread, action: reviewAction }),
				acceptError ? h("p", { className: "mp-error", role: "alert" }, acceptError) : null,
				brief || asked.length || related ? h(CardGroup, { label: "背景" },
					brief ? h("div", { className: "mp4-says" }, h(Clamp, { text: brief })) : null,
					asked.slice(-2).map(m => h("figure", { key: m.id, className: "mp4-quote" },
						h("blockquote", null, h(Clamp, { text: m.text })),
						h("figcaption", null, `原话，${dayLabel(m.at)} ${clock(m.at)}`))),
					related ? h("button", { type: "button", className: "mp4-setrow", onClick: () => openTask(project.id, related) }, h("span", { className: "mp4-setrow-text" }, h("span", { className: "mp4-setrow-title" }, `接续「${related.title}」`)), h(Icon, { name: "chevron", size: 14 })) : null) : null,
				!delivered && checks.length ? h(CardGroup, { label: `怎么算完成 ${done}/${checks.length}` }, h("div", { className: "mp4-checks" }, checkRows)) : null,
				files.length ? h(CardGroup, { label: `产出 ${files.length}` }, files.map(file => h(DeliveredFile, { key: file.id, project, file }))) : null,
				(thread.inputs || []).length ? h(TaskInputs, { paths: thread.inputs }) : null,
				h(CardGroup, { label: "进展" },
					!delivered && summary.progress ? h("p", { className: "mp4-now" }, summary.progress) : null,
					h(TaskTimeline, { thread }),
					delivered && checks.length ? h("details", { className: "mp4-setfold" }, h("summary", null, h("span", { className: "mp4-setline-k" }, "怎么算完成"), h("span", { className: "mp4-setline-v" }, `${done}/${checks.length}${done === checks.length ? "，都已完成" : ""}`)), h("div", { className: "mp4-checks" }, checkRows)) : null),
				h(TaskRunner, { project, thread }),
				thread.note ? h("details", { className: "mp4-fold" }, h("summary", null, "交代给执行者的要求"), h("p", { className: "mp4-says mp4-fold-body" }, thread.note)) : null);
		}

		/** What a state change means to the owner, by the state it moved to (the server's line names the mechanics). */
		const STATE_EVENT = { working: "开始执行", in_progress: "开始执行", review_ready: "交付了，待验收", blocked: "停下来等你拍板", failed: "执行失败", done: "已验收" };
		/** Task status by the state an event moved it to. */
		const STATE_STATUS = { review_ready: "review", working: "working", in_progress: "working", blocked: "decision", failed: "failed", done: "done" };
		/** A state name an agent wrote into its prose, such as 「（review_ready）」, read out in words. Display only. */
		const RAW_STATE_WORD = { review_ready: "待验收", in_progress: "进行中", working: "进行中", blocked: "待拍板", failed: "失败", done: "已完成" };
		const plainStates = text => String(text || "").replace(/review_ready|in_progress/g, m => RAW_STATE_WORD[m]).replace(/（(working|blocked|failed|done)）/g, (_m, w) => `（${RAW_STATE_WORD[w]}）`);
		/**
		 * A task's history as rows: what was said and what changed, oldest first. Who said it is written out ("你", the executor's
		 * name); the same state change twice in a row is shown once.
		 */
		function timelineRows(thread) {
			const rows = [];
			for (const x of thread.timeline || []) {
				// E2E #13: an outside runner reports each tool call; a stretch of them is one line, so progress is not buried.
				if (x.kind === "event" && /^外部执行：/.test(x.text || "")) {
					const last = rows[rows.length - 1];
					if (last && last.tools) { last.tools += 1; last.text = `执行了 ${last.tools} 步`; last.at = x.at; }
					else rows.push({ id: String(x.id ?? x.seq ?? rows.length), at: x.at, kind: "event", human: false, status: null, text: "执行了 1 步", tools: 1 });
					continue;
				}
				const human = x.author?.kind === "human";
				// A relay with a source task is executors passing word along, never the owner: sent from this task, or come in from another.
				const sent = x.kind === "relay" && x.fromThread && x.fromThread === thread.id, came = x.kind === "relay" && x.fromThread && !sent;
				const said = plainStates(x.text);
				const text = x.kind === "relay" ? `${sent ? (x.toThread ? "转给另一个任务" : "告诉项目助理") : came ? `${x.from || "另一个任务"}转来` : "你补充"}：${said}`
					: x.kind === "message" ? `${human ? "你" : x.author?.name || "执行者"}：${said}` : STATE_EVENT[x.state] || x.text || "";
				const last = rows[rows.length - 1];
				if (x.kind === "event" && last && last.kind === "event" && last.text === text.trim()) continue;
				rows.push({ id: String(x.id ?? x.seq ?? rows.length), at: x.at, kind: x.kind, human: human || (x.kind === "relay" && !x.fromThread), status: STATE_STATUS[x.state] || (x.tone === "closed" || /归档/.test(x.kind === "event" ? x.text || "" : "") ? "archived" : null), text: text.trim() });
			}
			return rows;
		}
		/** 输入: the files the task's own tools read, first seen first; the first few, the rest behind a fold. */
		function TaskInputs({ paths }) {
			const [all, setAll] = useState(false), shown = all ? paths : paths.slice(0, 6);
			return h(CardGroup, { label: `读过的文件 ${paths.length}` },
				h("ul", { className: "mp4-inputs" }, shown.map(path => h("li", { key: path, className: "mp4-mono mp4-ell", title: path }, path))),
				paths.length > shown.length ? h("button", { type: "button", className: "mp4-link-btn mp4-tline-more", onClick: () => setAll(true) }, `还有 ${paths.length - shown.length} 个`) : null);
		}
		/** The task's progress as a timeline: the newest few, the earlier ones folded. */
		function TaskTimeline({ thread }) {
			const rows = timelineRows(thread), [all, setAll] = useState(false), shown = all ? rows : rows.slice(-6);
			if (!rows.length) return h("p", { className: "mp4-muted mp4-note" }, "还没有动静。");
			return h(React.Fragment, null,
				rows.length > shown.length ? h("button", { type: "button", className: "mp4-link-btn mp4-tline-more", onClick: () => setAll(true) }, `更早的 ${rows.length - shown.length} 条`) : null,
				h("ol", { className: "mp4-tline" }, shown.map(r => h("li", { key: r.id, className: r.kind === "event" ? "mp4-tline-event" : "mp4-tline-said" + (r.human ? " mp4-tline-you" : "") },
					h("span", { className: "mp4-tline-mark", "aria-hidden": true }, r.status ? h(StatusIcon, { status: r.status, size: 12 }) : h("i", null)),
					h("span", { className: "mp4-tline-text" }, r.kind === "event" ? r.text : h(Clamp, { text: P.extractMarkdownPlainText ? P.extractMarkdownPlainText(r.text) : r.text })),
					r.at ? h("time", { dateTime: new Date(r.at).toISOString() }, `${dayLabel(r.at)} ${clock(r.at)}`) : null))));
		}

		/** The files a task published to the project's shared files. */
		const canPreview = f => /\.(html?|md|markdown)$/i.test(f.path || "") || /^image\//.test(f.mime || "");
		/**
		 * The 交付 tab's order: the file to look at first on top, the rest newest first. Among what came in with the last
		 * delivery (within a minute of the newest file), a page comes before a document, and both before their screenshots.
		 */
		function deliveryOrder(files) {
			const newest = files[0] ? files[0].at || 0 : 0, batch = files.filter(f => (f.at || 0) >= newest - 60000);
			const pick = batch.find(f => /\.html?$/i.test(f.path || "")) || batch.find(f => /\.(md|markdown)$/i.test(f.path || "")) || batch.find(canPreview) || files.find(canPreview);
			return pick ? [pick, ...files.filter(f => f !== pick)] : files;
		}
		// Newest first: the tab opens on what was delivered last, not on an early draft (E2E #23).
		const deliveredFiles = (project, thread) => (project.files || []).filter(f => f.by?.threadId === thread.id).sort((a, b) => (b.at || 0) - (a.at || 0));
		// Decimal units, as macOS Finder counts them.
		const sizeLabel = n => n < 1000 ? `${n} B` : n < 1e6 ? `${Math.round(n / 1000)} KB` : `${(n / 1e6).toFixed(1)} MB`;

		/**
		 * Where a task stands, in one band shaped like a merge box: its status icon, what state it is in, one line of detail and
		 * at most one action. The head of both 任务详情 and 交付.
		 */
		function TaskBanner({ project, thread, title, action }) {
			const [head, detail] = bannerOf(project, thread);
			// The task's own status icon, the one its rows carry everywhere. The pixel pet stands for the assistant as a whole, not one task.
			// A band only when the owner has something to do here; otherwise one quiet status line, like a thread's status under its title.
			if (!action) return h("p", { className: "mp4-banner-line" }, h(StatusIcon, { status: taskStatus(thread), size: 14 }), h("span", null, title || head), detail ? h("span", { className: "mp4-muted" }, detail) : null);
			return h("section", { className: "mp4-banner" },
				h("span", { className: "mp4-banner-icon" }, h(StatusIcon, { status: taskStatus(thread), size: 20 })),
				h("div", { className: "mp4-banner-text" }, h("b", null, title || head), detail ? h("span", null, detail) : null),
				action || null);
		}

		/** The banner's two lines: the state in words, then when it was delivered and how many files, or what is happening now. */
		function bannerOf(project, thread) {
			const exp = thread.experience || {}, summary = exp.summary || {}, files = deliveredFiles(project, thread);
			const when = thread.updatedAt ? `${dayLabel(thread.updatedAt)} ${clock(thread.updatedAt)}` : "";
			if (thread.state === "done") return ["已验收", when];
			if (thread.resolved) return ["已归档", when];
			if (thread.state === "review_ready") return ["已交付，待验收", files.length ? `${when} 交付了 ${files.length} 个文件` : `${when} 交付`];
			// The head already says it waits on the owner; the detail says what about, not the same words again.
			if (thread.state === "blocked") return ["待拍板", [exp.reason, summary.progress, summary.next].find(x => x && !/^等你(拍板|回答)/.test(x)) || ""];
			if (thread.state === "failed") return ["执行失败", summary.next || ""];
			return [taskPill(thread)[0], (exp.execution?.active ? exp.execution.label : summary.next) || ""];
		}

		/** Long text cut to three lines, with 展开 when there is more. */
		function Clamp({ text }) {
			const [open, setOpen] = useState(false), [over, setOver] = useState(false), box = useRef(null);
			useLayoutEffect(() => { const el = box.current; if (el && !open) setOver(el.scrollHeight > el.clientHeight + 1); }, [text, open]);
			return h("span", { className: "mp4-clampbox" }, h("span", { ref: box, className: open ? undefined : "mp4-clamp" }, text),
				over ? h("button", { type: "button", className: "mp4-link-btn mp4-clamp-more", onClick: () => setOpen(v => !v) }, open ? "收起" : "展开") : null);
		}

		/**
		 * A delivered file in its own right-panel tab (the owner, 2026-11-18: reading a long .md inside the 交付 list, scrolling in
		 * a box, was cramped). The tab's address names the file, so DSH keeps the tab across a reload and focuses the open one
		 * when the same file is clicked again; the name is the tab's title.
		 */
		const DOC_TAB_ID = "mira-projects.doc", DOC_TAB_KIND = "mira-projects-doc", DOC_PREFIX = "dsh-resource://mira-projects-doc/";
		const docAddress = (projectId, file) => DOC_PREFIX + [projectId, file.id, String(file.revision), file.path].map(encodeURIComponent).join("/");
		function docOf(address) {
			if (typeof address !== "string" || !address.startsWith(DOC_PREFIX)) return null;
			try {
				const [projectId, fileId, revision, path] = address.slice(DOC_PREFIX.length).split("/").map(decodeURIComponent);
				return projectId && fileId && path && Number(revision) > 0 ? { projectId, fileId, revision: Number(revision), path, name: path.split("/").at(-1) } : null;
			} catch (_e) { return null; }
		}
		function openDocTab(projectId, file) {
			try { shell.sidebar?.openResource(docAddress(projectId, file), { kind: DOC_TAB_KIND }); return true; } catch (_e) { return false; }
		}
		const isMarkdownPath = (path, mime) => /\.(md|markdown)$/i.test(path) || mime === "text/markdown";
		const isHtmlPath = (path, mime) => /\.html?$/i.test(path) || mime === "text/html";
		const isImagePath = (path, mime) => /^image\/(png|jpe?g|gif|webp|svg\+xml)$/.test(mime || "") || /\.(png|jpe?g|gif|webp|svg)$/i.test(path);
		/** A file's content as the owner reads it: Markdown rendered, a web page in a sandboxed frame, an image as itself, else plain text. */
		function DocContent({ path, mime, text, src, name }) {
			if (isImagePath(path, mime)) return h("img", { className: "mp4-dfile-img", src, alt: name });
			if (text === null) return h(Loading, null);
			if (isMarkdownPath(path, mime)) return h("div", { className: "mp-markdown mp4-dfile-md" }, h(P.MarkdownText, { text, labels: MARKDOWN_LABELS }));
			if (isHtmlPath(path, mime)) return h("iframe", { className: "mp4-dfile-frame", title: name, sandbox: "allow-scripts", srcDoc: text });
			return h("pre", null, text);
		}
		/** 请求修改 from a panel that is not inside the conversation: the composer on screen beside it. */
		function draftIntoVisible(text) {
			const editor = [...document.querySelectorAll('[contenteditable="true"]')].find(e => e.getClientRects().length);
			return editor ? draftInto(editor, text) : false;
		}
		/** The document tab: the file at the version that was clicked, with download and 请求修改, in a full-height reading pane. */
		function DocTab({ useTabInfo }) {
			const info = useTabInfo ? useTabInfo() : null, doc = docOf(info?.tab?.contentId);
			const [text, setText] = useState(null), [error, setError] = useState(null);
			const src = doc ? fileURL(doc.projectId, doc.fileId, doc.revision) : null;
			useEffect(() => {
				if (!doc || isImagePath(doc.path)) return;
				let alive = true;
				setText(null); setError(null);
				fetch(src, { credentials: "same-origin" }).then(r => { if (!r.ok) throw Error("文件暂不可读"); return r.text(); })
					.then(t => { if (alive) setText(t); }, e => { if (alive) setError(e.message); });
				return () => { alive = false; };
			}, [src]);
			if (!doc) return h("div", { className: "mp4-tabpane" }, h("p", { className: "mp4-muted" }, "这个标签页的文件信息丢了，回到任务的交付里再点一次。"));
			const dir = doc.path.slice(0, doc.path.length - doc.name.length).replace(/\/$/, "");
			return h("div", { className: "mp4-doc" },
				h("div", { className: "mp4-doc-bar" },
					h("span", { className: "mp4-doc-meta mp4-ell" }, parts(dir, `第 ${doc.revision} 版`)),
					h("button", { type: "button", className: "mp4-pill-btn", onClick: () => { if (!draftIntoVisible(`请修改 ${doc.path}：`)) setError("没找到这个任务的输入框，先打开任务对话"); } }, h(Icon, { name: "reply", size: 14 }), "请求修改"),
					h("a", { className: "mp4-dfile-dl", href: fileURL(doc.projectId, doc.fileId), download: true, title: "下载", "aria-label": `下载 ${doc.name}` }, h(Icon, { name: "download", size: 16 }))),
				h("div", { className: "mp4-doc-body" + (isHtmlPath(doc.path) ? " mp4-doc-body-wide" : "") },
					error ? h("p", { className: "mp4-err" }, error) : h(DocContent, { path: doc.path, text, src, name: doc.name })));
		}

		/**
		 * A file the task delivered: name, folder, size and version. The row opens the file in its own tab; the arrow unfolds a
		 * preview here, to read without leaving the list; the icon downloads it.
		 */
		function DeliveredFile({ project, file, startOpen = false }) {
			const [open, setOpen] = useState(false), [text, setText] = useState(null), [error, setError] = useState(null);
			// A web page renders in a sandboxed frame (scripts may run, but it gets no access to this page or its cookies); an image shows as itself.
			const isImage = isImagePath(file.path, file.mime), isHtml = isHtmlPath(file.path, file.mime);
			const readable = isImage || ((file.mime.startsWith("text/") || file.mime === "application/json") && file.size <= 512 * 1024);
			const name = file.path.split("/").at(-1), dir = file.path.slice(0, file.path.length - name.length).replace(/\/$/, "");
			useEffect(() => { if (startOpen && readable) toggle(); }, []);
			const toggle = async () => {
				setOpen(v => !v);
				if (text !== null || isImage) return;
				try {
					const response = await fetch(fileURL(project.id, file.id, file.revision), { credentials: "same-origin" });
					if (!response.ok) throw Error("文件暂不可读");
					setText(await response.text());
				} catch (e) { setError(e.message); }
			};
			const openTab = () => { if (!openDocTab(project.id, file)) toggle(); };
			return h("div", { className: "mp4-dfile" },
				h("div", { className: "mp4-dfile-row" },
					h("button", { type: "button", className: "mp4-dfile-main", disabled: !readable, title: readable ? "在新标签页打开" : undefined, onClick: readable ? openTab : undefined },
						h(Icon, { name: "note", size: 16 }),
						h("span", { className: "mp4-dfile-text" }, h("b", { className: "mp4-ell" }, name), h("span", { className: "mp4-ell" }, parts(dir, sizeLabel(file.size), `第 ${file.revision} 版`)))),
					readable ? h("button", { type: "button", className: "mp4-dfile-dl", "aria-expanded": open, title: open ? "收起预览" : "在这里预览", "aria-label": `${open ? "收起" : "预览"} ${name}`, onClick: toggle },
						h("span", { className: "mp-sec-chev" + (open ? " mp-sec-chev-on" : "") }, h(Icon, { name: "chevron", size: 12 }))) : null,
					h("a", { className: "mp4-dfile-dl", href: fileURL(project.id, file.id), download: true, title: "下载", "aria-label": `下载 ${name}` }, h(Icon, { name: "download", size: 16 }))),
				open ? h("div", { className: "mp4-dfile-preview" + (isHtml || isImage ? " mp4-dfile-preview-wide" : "") }, h("div", { className: "mp4-dfile-bar" }, h("button", { type: "button", className: "mp4-pill-btn", onClick: e => { if (!draftInto(e.currentTarget, `请修改 ${file.path}：`) && !draftIntoVisible(`请修改 ${file.path}：`)) setError("没找到这个任务的输入框，先打开任务对话"); } }, h(Icon, { name: "reply", size: 14 }), "请求修改")), error ? h("p", { className: "mp4-err" }, error) : h(DocContent, { path: file.path, mime: file.mime, text, src: fileURL(project.id, file.id, file.revision), name })) : null);
		}

		/** What was counted when a task last changed hands, in a few words. */
		function handoffNote(handoff) {
			if (!handoff) return null;
			const when = `${dayLabel(handoff.at)} ${clock(handoff.at)}`, name = id => EXECUTOR_NAMES[id] || id || "执行者";
			const head = handoff.kind === "return" ? `${when} 你交还给 ${name(handoff.to)}。` : `${when} 从 ${name(handoff.from)} 换成 ${name(handoff.to)}。`;
			const f = handoff.facts, w = f && f.workspace;
			if (!f) return head;
			const counted = [`${f.messages} 条新消息`, `${f.turns} 轮执行`, w && w.commits !== null ? `${w.commits} 次提交` : null, w ? `${w.dirtyCount} 个文件没提交` : null].filter(Boolean).join("，");
			return `${head}这期间有 ${counted}。`;   // no claim that the other side was told: a run that fails to start never reads it (E2E #17)
		}

		/**
		 * Who executes one task, changeable at any time, and the switch that lets the owner drive it for a while.
		 * A change applies from the task's next round; a round that is running finishes with the executor it started with.
		 */
		function TaskRunner({ project, thread }) {
			const runtimes = useSnapshot().runtimes || {}, catalog = useModelCatalog();
			const [busy, setBusy] = useState(false), [error, setError] = useState(null);
			const next = thread.runner || "dsh", bound = thread.runtime ? thread.runtime.provider : null, name = id => EXECUTOR_NAMES[id] || id;
			const closed = thread.state === "done" || thread.resolved, running = thread.status === "running", held = thread.held;
			const call = async (op, body) => {
				setBusy(true); setError(null);
				try { await act(op, { projectId: project.id, threadId: thread.id, ...body }); }
				catch (e) { setError(String(e?.message || e)); }
				finally { setBusy(false); }
			};
			const current = thread.modelSelection || (thread.model && thread.model.model ? { provider: thread.model.provider, model: thread.model.model } : null);
			const groups = (catalog && catalog.groups) || [];
			const find = sel => sel ? groups.find(g => g.id === sel.provider)?.models.find(m => m.id === sel.model) : null;
			const modelLabel = sel => sel ? (find(sel)?.name || modelName(sel)) : "默认模型";
			const fallback = thread.sessionId ? "沿用会话原来的模型" : `默认模型${catalog && catalog.default ? `（${modelLabel(catalog.default)}）` : ""}`;
			const used = name(bound || next) + ((bound || next) === "dsh" && current ? " " + modelLabel(current) : "");
			if (closed) return h(Section, { label: "执行" }, h("p", { className: "mp4-muted" }, `由 ${used} 执行，任务已结束`), thread.handoff ? h("p", { className: "mp4-muted mp4-note" }, handoffNote(thread.handoff)) : null);
			const key = sel => sel ? JSON.stringify({ provider: sel.provider, model: sel.model }) : "";
			const models = next !== "dsh" ? null : h("select", { className: "mp4-agent-select mp4-setline-select", value: key(current), disabled: busy || !catalog, "aria-label": "模型", onChange: e => call("setTaskModel", JSON.parse(e.target.value)) },
				current ? null : h("option", { value: "" }, catalog ? fallback : "正在读取模型…"),
				current && !find(current) ? h("option", { value: key(current) }, modelLabel(current)) : null,
				groups.map(g => h("optgroup", { key: g.id, label: g.name || g.id }, g.models.map(m => h("option", { key: m.id, value: key({ provider: g.id, model: m.id }) }, m.name || m.id)))));
			const status = held ? `你接手中，${clock(held.at)} 起${running ? "，它这一轮跑完就停" : ""}`
				: bound && bound !== next ? `下一轮起换成 ${name(next)}${running ? `，${name(bound)} 这一轮先做完` : ""}`
				: running ? "正在执行" : "自动执行";
			const hint = !held ? null
				: isOutside(thread) ? "想自己在终端里接着同一个会话做，用下面「接着做」里的命令。做完点交还，执行者会被告知这段时间变了什么。"
				: "可以直接在这个任务的对话里说话，也可以自己改工作区。做完点交还，执行者会被告知这段时间变了什么。";
			const row = (key, name, control) => h("div", { key, className: "mp4-setline" }, h("span", { className: "mp4-setline-k" }, name), control);
			return h(CardGroup, { label: "执行" },
				row("harness", "执行方式", h("select", { className: "mp4-agent-select mp4-setline-select", value: next, disabled: busy, "aria-label": "用什么执行", onChange: e => call("setTaskRunner", { provider: e.target.value }) },
					Object.keys(EXECUTOR_NAMES).map(id => h("option", { key: id, value: id, disabled: id !== "dsh" && !runtimes[id] && next !== id }, EXECUTOR_NAMES[id] + (id === "dsh" || runtimes[id] ? "" : "（这台机器没装）"))))),
				models ? row("model", "模型", models) : null,
				h("div", { className: "mp4-setline" },
					h("span", { className: "mp4-setline-k mp4-setline-plain mp4-muted mp4-ell" }, status),
					held
						? h("button", { type: "button", className: "mp4-mem-link", disabled: busy, onClick: () => call("releaseTask") }, `交还给 ${name(next)}`)
						: h("button", { type: "button", className: "mp4-mem-link", disabled: busy, title: "暂停自动执行，你自己来；交还后再继续", onClick: () => call("holdTask") }, "我来接手")),
				hint ? h("p", { className: "mp4-muted mp4-note" }, hint) : null,
				thread.handoff ? h("p", { className: "mp4-muted mp4-note" }, handoffNote(thread.handoff)) : null,
				error ? h("p", { className: "mp4-err" }, error) : null);
		}
		/** DSH's model list (groups of provider → models), fetched once a minute at most and shared. */
		let modelCatalogCache = null;
		function useModelCatalog() {
			const fresh = () => modelCatalogCache && Date.now() - modelCatalogCache.at < 60000 ? modelCatalogCache.data : null;
			const [data, setData] = useState(fresh);
			useEffect(() => {
				if (data) return;
				let live = true;
				fetch("api/mira-projects/models", { credentials: "same-origin", cache: "no-store" }).then(r => r.ok ? r.json() : null)
					.then(d => { if (d && d.groups) { modelCatalogCache = { at: Date.now(), data: d }; if (live) setData(d); } }).catch(() => {});
				return () => { live = false; };
			}, []);
			return data;
		}

		/** A task run by Claude Code or Codex has no DSH Session: its conversation, and the command to carry on in a terminal, live here. */
		function OutsideTask({ project, thread }) {
			// `next` is null while the task is on its way back to DSH but still bound to the outside runner that last had it.
			const next = outsideRunner(project, thread), provider = next || thread.runtime?.provider, name = RUNNER_NAMES[provider] || provider;
			const native = thread.runtime?.externalSessionId || null;
			const [take, setTake] = useState(null), [busy, setBusy] = useState(false), [error, setError] = useState(null), [copied, setCopied] = useState("");
			useEffect(() => { setTake(null); setError(null); }, [native]);
			const fetchTake = async () => {
				setBusy(true); setError(null);
				try { setTake(await act("takeover", { projectId: project.id, threadId: thread.id })); }
				catch (e) { setError(String(e?.message || e)); }
				finally { setBusy(false); }
			};
			const said = label => { setCopied(label); setTimeout(() => setCopied(""), 2000); };
			const answer = (decisionId, chosen) => act("answerDecision", { projectId: project.id, decisionId, chosen });
			const closed = thread.state === "done" || thread.resolved;
			return h(React.Fragment, null,
				h(Section, { label: "接着做" },
					h("p", { className: "mp4-muted" }, next
						? `这个任务由 ${name} 执行，没有 DSH 对话。在下面留言，它下一轮会读到；也可以自己在终端里接着同一个会话做。`
						: `这个任务已经交回 DSH，下一轮起由 DSH 接着做。${name} 做的这一段留在这里。`),
					native
						? h("div", { className: "mp4-follow-acts" }, h("button", { type: "button", className: "mp4-sug", disabled: busy, onClick: fetchTake }, take ? "重新生成命令" : "在终端接着做"))
						: h("p", { className: "mp4-muted mp4-note" }, `${name} 跑完第一轮后，这里会给出接着同一个会话做的命令。`),
					take ? h("div", { className: "mp4-merge" }, h("code", { className: "mp4-mono" }, take.command), h("button", { type: "button", className: "mp4-link-btn", onClick: ev => copyCode(ev, take.command, said) }, copied || "复制")) : null,
					take ? h("p", { className: "mp4-muted mp4-note" }, take.busy ? `${name} 这会儿正在跑这个任务。等它这一轮结束再接手，免得两边同时改。` : "粘到 DSH 这台机器的终端里运行（DSH 的「新建终端」就行）。任务卡、收件箱这些项目工具都带着。") : null,
					error ? h("p", { className: "mp4-err" }, error) : null),
				h(Section, { label: "对话" },
					h("div", { className: "mp4-filepane mp4-thread" },
						(thread.timeline || []).length ? h(Timeline, { rows: thread.timeline, project, onOpenThread: () => {}, onAnswer: answer, showThreadLabel: false }) : h("p", { className: "mp4-muted" }, "还没有消息。"),
						closed ? null : h(Composer, { members: project.members, project, placeholder: `给 ${next ? name : "DSH"} 留言`, draftKey: `${project.id}:${thread.id}`, onSend: text => act("post", { projectId: project.id, threadId: thread.id, text, typed: text }) }))));
		}

		function ProjectSession({ project, projects, threadId, onCreate }) {
			const tabs = usePanelTabs("project", ["ptasks"]);
			const [panelOpen, setPanelOpen] = useState(panelFits), [wide, setWide] = useState(false), [peek, setPeek] = useState(threadId || null);
			useEffect(() => { if (threadId) { setPeek(threadId); tabs.open("peek"); setPanelOpen(true); } }, [threadId]);
			const reading = useReadingPosition(true, project.id, project.chat.length);
			const openThread = id => {
				const t = project.threads.find(x => x.id === id);
				if (t?.sessionId && !isOutside(t) && shell.workspace) { openTask(project.id, t); return; }
				setPeek(id); tabs.open("peek"); setPanelOpen(true);
			};
			const answer = (decisionId, chosen) => act("answerDecision", { projectId: project.id, decisionId, chosen });
			const peekThread = peek ? project.threads.find(t => t.id === peek) : null;
			const info = { ptasks: { label: "任务", icon: "tasks" }, pmem: { label: "项目资料", icon: "note" }, peek: { label: peekThread ? peekThread.title : "任务", icon: "tasks" },
				files: { label: "共享文件", icon: "folder" }, devices: { label: "设备", icon: "folder" }, settings: { label: "项目设置", icon: "note" }, automations: { label: "自动任务", icon: "clock" } };
			const guides = [
				{ id: "ptasks", icon: "tasks", label: "任务", desc: "这个项目的全部任务" },
				{ id: "pmem", icon: "note", label: "项目资料", desc: "指令、仓库、做过的决定、没收尾的事" },
				Array.isArray(project.files) ? { id: "files", icon: "folder", label: "共享文件", desc: "明确发布到项目的文件" } : null,
				project.devices ? { id: "devices", icon: "folder", label: "设备", desc: "授权助理访问的设备目录" } : null,
				{ id: "settings", icon: "note", label: "项目设置", desc: "名称、说明和模型" },
				{ id: "automations", icon: "clock", label: "自动任务", desc: "定时让项目做的事" },
			].filter(Boolean);
			const active = tabs.active;
			const panelBody = active === "ptasks" ? h(ProjectTasks, { project, onOpen: openThread, activeId: peek })
				: active === "pmem" ? h(ProjectMemory, { project })
				: active === "peek" ? (peekThread ? h(React.Fragment, null, h(TaskSummary, { project, thread: peekThread }), showsOutside(project, peekThread) ? h(OutsideTask, { key: peekThread.id, project, thread: peekThread }) : h("p", { className: "mp4-muted mp4-note" }, peekThread.sessionId ? "" : "还没开始执行；开始后点它会进入这个任务的对话。")) : h("p", { className: "mp4-muted" }, "从任务列表里选一个任务。"))
				: active === "files" ? h(SharedFiles, { key: project.id, project })
				: active === "devices" && project.devices ? h(DeviceAccess, { key: project.id, project })
				: active === "settings" ? h(ProjectSettings, { key: project.id, project, onClose: () => tabs.close("settings") })
				: active === "automations" ? h(AutomationPanel, { key: project.id, project, onClose: () => tabs.close("automations") })
				: null;
			const waiting = project.threads.filter(isWaiting).length, running = project.threads.filter(isRunning).length;
			return h(SessionFrame, {
				crumb: { label: "项目", onClick: navigateIndex }, title: project.name,
				where: [waiting ? `${waiting} 待处理` : null, running ? `${running} 进行中` : null].filter(Boolean).join("，") || "项目对话",
				menu: [["项目设置", () => { tabs.open("settings"); setPanelOpen(true); }], ["自动任务", () => { tabs.open("automations"); setPanelOpen(true); }], Array.isArray(project.files) ? ["共享文件", () => { tabs.open("files"); setPanelOpen(true); }] : null, ["新建项目", onCreate]],
				scroll: reading, panelOpen, onShowPanel: () => setPanelOpen(true),
				body: h(React.Fragment, null,
					project.workspace?.reason ? h("div", { className: "mp-notice" }, `本项目使用共享目录：${project.workspace.reason}`) : null,
					project.chat.length === 0 ? h("div", { className: "mp4-empty" }, h("b", null, project.name), h("p", null, "在这里说需求、问进展。需要动手的事会开成任务，之后在任务里继续聊。")) : null,
					h(Timeline, { rows: project.chat, project, onOpenThread: openThread, activeThreadId: peek, onAnswer: answer, showThreadLabel: true }),
					h(LiveReply, { project, memberId: "coordinator", assistant: project.coordinator }),
					h(LiveReply, { project, memberId: "mira", assistant: project.mira })),
				composer: h(React.Fragment, null,
					h(WorkingLine, { project, onOpenThread: openThread }),
					h(Composer, { members: project.members, project, placeholder: "发消息或交代一件事", draftKey: `${project.id}:chat`, onSend: text => act("post", { projectId: project.id, text, typed: text }) })),
				panel: h(SidePanel, { tabs, info, guides, wide, onWide: () => setWide(v => !v), onHide: () => setPanelOpen(false) }, panelBody),
			});
		}

		function ProjectsPage(props) {
			const hash = useRoute();
			const [creating, setCreating] = useState(() => window.location.hash === "#mira-projects-new");
			const route = readRoute();
			const selected = route?.projectId;
			const snap = useSnapshot(selected);
			useEffect(() => {
				const create = () => setCreating(true);
				window.addEventListener("mira-projects:create", create);
				return () => window.removeEventListener("mira-projects:create", create);
			}, []);
			useEffect(() => { if (window.location.hash === "#mira-projects-new") setCreating(true); else if (route) setCreating(false); }, [hash]);
			useEffect(() => { if (!readRoute() && !isIndexRoute() && window.location.hash !== "#mira-projects-new") navigateIndex(); }, []);
			void props;
			// E2E #21, #22: this page is reached when opening a project's conversation failed (the server was down) or from an
			// old link. Once the data is back, open the conversation instead; a task run outside DSH has none, so it stays here.
			const [tried, setTried] = useState(null);
			const routeThread = route?.threadId && snap.project?.id === selected ? (snap.project.threads || []).find(t => t.id === route.threadId) : null;
			const stay = Boolean(routeThread && (isOutside(routeThread) || (routeThread.runner && routeThread.runner !== "dsh")));
			const attempt = selected ? `${selected}/${route.threadId || ""}` : null;
			const ready = Boolean(shell.workspace && attempt && snap.loaded && !snap.error && !stay && (!route.threadId || routeThread));
			useEffect(() => {
				if (!ready || tried === attempt) return;
				setTried(attempt);
				if (routeThread) openTask(selected, routeThread); else openProject(selected);
			}, [ready, attempt]);
			const create = () => setCreating(true);
			if (snap.error && !snap.projects.length) return h(Loading, { text: "连不上服务，正在重连…" });
			if (!snap.loaded) return h("div", { className: "mp-page mp-center" }, h(P.TextShimmer, { active: true }, "正在载入项目"));
			if (ready && tried !== attempt) return h(Loading, { text: "正在打开项目对话…" });
			if (creating || snap.projects.length === 0) {
				return h("div", { className: "mp4-index" }, h("div", { className: "mp4-index-col" }, h("div", { className: "mp-welcome" },
					h("h1", null, "新建项目"),
					h("p", null, "一个项目是一段对话和一组任务：你说需求，项目助理先问清楚，再开成任务交给执行者。"),
					h(CreateProject, { onCreated: id => { setCreating(false); openProject(id); }, onCancel: snap.projects.length ? () => { setCreating(false); if (window.location.hash === "#mira-projects-new") navigateIndex(); } : null }))));
			}
			if (!selected) return h(ProjectsIndex, { snap, onCreate: create });
			const project = snap.project && snap.project.id === selected ? snap.project : null;
			if (!project) return snap.projects.some(p => p.id === selected) ? h("div", { className: "mp-page mp-center" }, h(P.TextShimmer, { active: true }, "正在载入")) : h(ProjectsIndex, { snap, onCreate: create });
			return h(ProjectSession, { key: project.id, project, projects: snap.projects, threadId: route.threadId, onCreate: create });
		}

		// ---------------------------------------------------------------- Mira

		function SourcePanel({ sourceRef }) {
			const [source, setSource] = useState(null), [error, setError] = useState(null);
			useEffect(() => {
				const abort = new AbortController(); setSource(null); setError(null);
				fetch("api/mira-projects/personal/source?ref=" + encodeURIComponent(sourceRef), { credentials: "same-origin", cache: "no-store", signal: abort.signal })
					.then(async r => { const d = await r.json(); if (!r.ok) throw Error(d.error); return d; })
					.then(d => { if (!abort.signal.aborted) setSource(d); }, e => { if (!abort.signal.aborted) setError(e.message); });
				return () => abort.abort();
			}, [sourceRef]);
			if (error) return h("p", { role: "alert", className: "mp-error" }, error);
			if (!source) return h("p", { className: "mp4-muted" }, "正在读取原记录…");
			const kind = ({ observation: "设备观察", external: "外部资料", principal: "你的原话", relay: "助理记录", system: "项目事件" })[source.trust] || source.trust;
			return h(React.Fragment, null,
				h("section", { className: "mp4-grid" },
					h("span", { className: "mp4-k" }, "类别"), h("span", null, `${kind}${source.trust === "external" ? "（只读）" : ""}`),
					h("span", { className: "mp4-k" }, "来自"), h("span", null, source.connectorName || source.deviceName || source.projectName || "个人记录"),
					h("span", { className: "mp4-k" }, "发生"), h("span", null, new Date(source.happenedAt).toLocaleString()),
					source.projectName ? [h("span", { key: "k", className: "mp4-k" }, "关联"), h("span", { key: "v" }, [source.projectName, source.threadTitle].filter(Boolean).join(" / "))] : null),
				h("div", { className: "mp4-source-text" }, source.sourceState === "expired" ? h("p", { className: "mp4-muted" }, "原文已按保留设置清理。") : h(RichText, { text: source.text, members: [] })),
				source.trust === "external" ? h("p", { className: "mp4-muted mp4-note" }, "外部资料，不是你的指令。") : null,
				source.projectId ? h("button", { type: "button", className: "mp4-link-btn", onClick: () => navigateProject(source.projectId, source.threadId) }, source.threadId ? "进入所属任务 ›" : "进入所属项目 ›") : null);
		}

		// Which model each assistant runs on, picked from the models DSH offers. Mira makes the same change when asked.
		/** A button that puts a sentence into the personal assistant's input box for the owner to finish and send. */
		function AskMira({ text, label = "让助理帮我改", strong = false, quiet = false }) {
			const [error, setError] = useState(null);
			return h(React.Fragment, null,
				h("button", { type: "button", className: strong ? "mp4-btn mp4-btn-strong mp4-askmira" : quiet ? "mp4-link-btn mp4-askmira" : "mp4-btn mp4-askmira", onClick: e => { setError(draftInto(e.currentTarget, text) ? null : "没找到个人助理的输入框，先打开个人助理的对话"); } }, h(Icon, { name: "spark", size: 14 }), label),
				error ? h("p", { className: "mp4-err" }, error) : null);
		}

		const HARNESS_LABEL = { dsh: "DSH（本机运行）", ...RUNNER_NAMES };
		const AGENT_STATUS = { running: "正在运行", queued: "排队中", idle: "空闲" };
		const AGENT_ROLE = { coordinator: "项目助理", memory: "记忆助理", worker: "执行者" };

		/** The owner's model strategy for one project (or the personal assistant), in their words: write it, or let Mira set it up. */
		function ModelStrategy({ projectId, name, text, onSaved }) {
			const [edit, setEdit] = useState(false), [value, setValue] = useState(text), [busy, setBusy] = useState(false), [error, setError] = useState(null);
			useEffect(() => { if (!edit) setValue(text); }, [text, edit]);
			const save = async () => {
				setBusy(true); setError(null);
				try { await act("setModelStrategy", { projectId: projectId || null, text: value }); setEdit(false); onSaved(); } catch (e) { setError(e.message); } finally { setBusy(false); }
			};
			return h("div", { className: "mp4-strategy" },
				h("div", { className: "mp4-strategy-head" }, h("span", { className: "mp4-strategy-k" }, "模型策略", text || edit ? null : h("span", { className: "mp4-muted" }, "未设置")),
					edit ? null : h("span", { className: "mp4-strategy-links" }, h("button", { type: "button", className: "mp4-link-btn", onClick: () => setEdit(true) }, text ? "自己改" : "自己写"),
						h(AskMira, { quiet: true, label: text ? "让助理照着配" : "让助理帮我定", text: text ? `按「${name}」的模型策略把每个助理的模型配好：${text}` : `帮「${name}」定一个模型策略，再照着把每个助理的模型配好。我的想法是：` }))),
				edit ? h(React.Fragment, null,
					h("textarea", { className: "mp4-strategy-input", rows: 3, value, maxLength: 500, autoFocus: true, placeholder: "比如：规划和拍板用 Opus，写代码用 DeepSeek Flash，夜里的整理用便宜的", onChange: e => setValue(e.target.value) }),
					h("div", { className: "mp4-strategy-acts" }, h("button", { type: "button", className: "mp4-btn", disabled: busy, onClick: () => { setEdit(false); setValue(text); } }, "取消"),
						h("button", { type: "button", className: "mp4-btn mp4-btn-strong", disabled: busy, onClick: save }, busy ? "正在保存…" : "保存")))
					: text ? h("p", { className: "mp4-says" }, text) : null,
				error ? h("p", { className: "mp4-err" }, error) : null);
		}

		/** One assistant's own page: who it is, what wakes it, what it runs on, and the instructions it reads every turn. */
		function AgentDetail({ agent, data, label, onChange, busy, onBack }) {
			const [p, setP] = useState(null), [error, setError] = useState(null);
			useEffect(() => {
				setP(null); setError(null);
				fetch("api/mira-projects/agent?" + new URLSearchParams({ projectId: agent.projectId || "", member: agent.member }), { credentials: "same-origin", cache: "no-store" })
					.then(async r => { const d = await r.json(); if (!r.ok) throw Error(d.error || "读不到这个助理"); setP(d); }).catch(e => setError(e.message));
			}, [agent.projectId, agent.member]);
			const options = (data.groups || []).flatMap(g => g.models.map(m => ({ value: JSON.stringify({ provider: g.id, model: m.id }), label: `${g.name || g.id} ${m.name || m.id}` })));
			const value = agent.configured ? JSON.stringify({ provider: agent.configured.provider, model: agent.configured.model }) : "";
			const where = agent.projectId ? `「${agent.projectName}」的${agent.name}` : "你自己（个人助理）";
			const role = p?.roleLabel || (agent.projectId ? AGENT_ROLE[agent.role] || "" : "");
			// Laid out like Codex's settings: a heading per group, one bordered card, a row per fact, the name left and the value right.
			const line = (key, name, value) => h("div", { key, className: "mp4-setline" }, h("span", { className: "mp4-setline-k" }, name), h("span", { className: "mp4-setline-v" }, value));
			const prompt = (key, name, text) => h("details", { key, className: "mp4-setfold" }, h("summary", null, h("span", { className: "mp4-setline-k" }, name), h("span", { className: "mp4-setline-v" }, `${text.length} 字`)), h("pre", { className: "mp4-prompt" }, text));
			return h(React.Fragment, null,
				h("button", { type: "button", className: "mp4-link-btn mp4-back", onClick: onBack }, h(Icon, { name: "back", size: 14 }), "助理与模型"),
				h("div", { className: "mp4-page-head" },
					h("h3", { className: "mp4-tab-title" }, p?.name || agent.name),
					h("p", null, role && role !== (p?.name || agent.name) ? role : "")),
				error ? h("p", { className: "mp4-err" }, error) : null,
				h("div", { className: "mp4-setgroup" }, h("h4", null, "运行"),
					h("div", { className: "mp4-setlist" },
						h("div", { key: "status", className: "mp4-setline" }, h("span", { className: "mp4-setline-k" }, "状态", p?.lastTurn?.at ? h("small", null, `上次运行 ${dayLabel(p.lastTurn.at)} ${clock(p.lastTurn.at)}`) : null), h("span", { className: "mp4-setline-v" }, p ? AGENT_STATUS[p.status] || p.status : "…")),
						line("harness", "运行方式", p ? HARNESS_LABEL[p.harness] || p.harness : "…"),
						h("div", { key: "model", className: "mp4-setline" }, h("span", { className: "mp4-setline-k" }, "模型", h(AskMira, { quiet: true, label: "让助理换", text: `把${where}换成更合适的模型。我想要：` })), h("select", { className: "mp4-agent-select mp4-setline-select", value, disabled: busy, "aria-label": `${where}用的模型`, onChange: e => onChange(agent, e.target.value) },
							h("option", { value: "" }, `跟随默认（${data.default ? label(data.default) : "DSH 默认"}）`),
							value && !options.some(o => o.value === value) ? h("option", { value }, `${label(agent.configured)}（已不在列表里）`) : null,
							options.map(o => h("option", { key: o.value, value: o.value }, o.label)))))),
				h("div", { className: "mp4-setgroup" }, h("h4", null, "什么时候醒"),
					h("div", { className: "mp4-setlist" }, p ? p.wakes.map((w, i) => h("div", { key: i, className: "mp4-setline" }, typeof w === "string" ? h("span", { className: "mp4-setline-k mp4-setline-plain" }, w) : h("span", { className: "mp4-setline-k" }, w.name, h("small", null, w.detail)))) : line("loading", "正在读取…", ""))),
				h("div", { className: "mp4-setgroup" }, h("h4", null, p?.example ? `每轮读到的说明，以「${p.example.title}」为例` : "每轮读到的说明"),
					p && !p.instructions && !p.live ? h("p", { className: "mp4-muted" }, "还没接过任务") : h("div", { className: "mp4-setlist" },
						p?.instructions ? prompt("sys", "系统说明", p.instructions) : null,
						p?.live ? prompt("live", "每轮附带的现状", p.live) : null)));
		}

		/** 助理与模型: every assistant by whom it works for, each project's model strategy, and a page per assistant. */
		function AgentModels({ revision }) {
			const [data, setData] = useState(null), [error, setError] = useState(null), [busy, setBusy] = useState(null), [open, setOpen] = useState(null);
			const load = () => fetch("api/mira-projects/models", { credentials: "same-origin", cache: "no-store" })
				.then(async r => { const d = await r.json(); if (!r.ok) throw Error(d.error || "模型列表暂不可用"); setData(d); })
				.catch(e => setError(e.message));
			useEffect(() => { load(); }, [revision]);
			if (!data) return (error ? h("p", { className: "mp4-err" }, error) : h(Loading, null));
			const agents = data.agents || [];
			const label = sel => { if (!sel) return "—"; const m = (data.groups || []).find(g => g.id === sel.provider)?.models.find(x => x.id === sel.model); return m ? (m.name || m.id) : `${sel.provider}/${sel.model}`; };
			const change = async (a, value) => {
				const key = (a.projectId || "") + ":" + a.member, pick = value ? JSON.parse(value) : { model: "default" };
				setBusy(key); setError(null);
				try { await act("setModel", { projectId: a.projectId, member: a.member, ...pick }); await load(); setOpen(o => o && (agents.find(x => x.projectId === o.projectId && x.member === o.member) || o)); }
				catch (e) { setError(e.message); }
				finally { setBusy(null); }
			};
			const nameOf = a => a.projectId ? (isProjectExecutor({ id: a.member }) ? "执行任务" : a.name) : "个人助理";
			if (open) {
				const fresh = agents.find(a => a.projectId === open.projectId && a.member === open.member) || open;
				return h(AgentDetail, { agent: { ...fresh, name: nameOf(fresh) }, data, label, busy: busy === (fresh.projectId || "") + ":" + fresh.member, onChange: change, onBack: () => setOpen(null) });
			}
			const groups = [{ id: "", name: "个人助理", items: agents.filter(a => !a.projectId) },
				...[...new Set(agents.filter(a => a.projectId).map(a => a.projectId))].map(id => ({ id, name: agents.find(a => a.projectId === id).projectName, items: agents.filter(a => a.projectId === id) }))];
			return h(React.Fragment, null,
				h("div", { className: "mp4-page-head mp4-page-head-row" }, h("h3", { className: "mp4-tab-title" }, "助理与模型"), h(AskMira, { quiet: true, label: "让助理帮我配", text: "帮我看看各个助理现在用的模型合不合适，按你的建议配好。我更在意：" })),
				error ? h("p", { className: "mp4-err", role: "alert" }, error) : null,
				agents.length === 0 ? h("p", { className: "mp4-muted" }, "还没有助理。") : null,
				groups.filter(g => g.items.length).map(g => h("div", { key: g.id || "mira", className: "mp4-setgroup" }, h("h4", null, g.id ? g.name : "个人"),
					h("div", { className: "mp4-setlist" }, h(ModelStrategy, { projectId: g.id, name: g.id ? g.name : "个人助理", text: (data.strategies || {})[g.id] || "", onSaved: load }), g.items.map(a => {
						const ran = a.running?.source === "session" ? a.running : null, model = label(ran || a.configured);
						return h("button", { key: (a.projectId || "") + ":" + a.member, type: "button", className: "mp4-setrow", "data-agent": (a.projectId || "") + ":" + a.member, onClick: () => setOpen(a) },
							h("span", { className: "mp4-setrow-text" }, h("span", { className: "mp4-setrow-title" }, nameOf(a)),
								(() => { const sub = parts(a.projectId ? (isProjectExecutor({ id: a.member }) ? "执行者" : AGENT_ROLE[a.role] && AGENT_ROLE[a.role] !== nameOf(a) ? AGENT_ROLE[a.role] : "") : "",
									a.configured && ran && (a.configured.provider !== ran.provider || a.configured.model !== ran.model) ? `下一步起换成 ${label(a.configured)}` : null); return sub.length ? h("span", { className: "mp4-setrow-desc" }, sub) : null; })()),
							h("span", { className: "mp4-setline-v" }, model === "—" ? `跟随默认（${data.default ? label(data.default) : "DSH 默认"}）` : model),
							h(Icon, { name: "chevron", size: 14 }));
					})))));
		}

		/** Everything across projects that waits for the owner: deliveries to accept, decisions, failures; oldest first. */
		function useWaiting() {
			const today = useToday();
			const w = today.waiting || { decisions: [], review: [], failed: [] };
			return [
				...(w.review || []).map(r => ({ key: "r" + r.projectId + r.id, projectId: r.projectId, projectName: r.projectName, threadId: r.id, title: r.title, kind: "review", summary: r.summary || "", ask: "已交付，待验收", say: `${r.assigneeName ? r.assigneeName + " " : ""}做完了，等你看一眼`, since: r.since })),
				...(w.decisions || []).map(d => ({ key: "d" + d.id, projectId: d.projectId, projectName: d.projectName, threadId: d.threadId, title: d.threadTitle || "拍板", kind: "decision", ask: d.question, say: `做到一半停下来问你：${d.question}`, since: d.since, decision: d })),
				...(w.failed || []).map(f => ({ key: "f" + f.projectId + f.id, projectId: f.projectId, projectName: f.projectName, threadId: f.id, title: f.title, kind: "failed", summary: f.summary || "", ask: "执行失败，看看怎么恢复", say: `${f.assigneeName ? f.assigneeName + " " : ""}跑到一半停了，等你看怎么接着做`, since: f.since })),
			// A question stops a running task, so it comes before deliveries waiting for acceptance; each group oldest first.
			].sort((a, b) => (b.decision ? 1 : 0) - (a.decision ? 1 : 0) || (a.since || 0) - (b.since || 0));
		}


		/** Suggested next lines for the owner: answer a decision, accept a delivery, ask about today. */
		function suggestionsFor(waiting, said) {
			// A task with two open decisions gives the same line twice; each line is offered once.
			return [...new Set([
				...waiting.filter(x => x.decision).map(x => `「${x.title}」按推荐的回复`),
				...waiting.filter(x => x.kind === "review").map(x => `帮我把「${x.title}」验收了`),
				"今天做了什么",
			])].filter(s => !said.has(s)).slice(0, 4);
		}

		const HISTORY_GAP = 30 * 60e3, HISTORY_BURST = 15 * 60e3;
		/**
		 * The rows of one day, newest first, laid out by the clock: contiguous stretches of the same activity are joined
		 * (13:00–13:10 and 13:10–13:20 → 13:00–13:20); rows that start at the same moment share one time label; the space
		 * above a new moment grows with the minutes since the one above it (half a pixel a minute, 6 to 40 px), so a busy
		 * hour runs long and a quiet one stays short, and the rail looks the same everywhere.
		 */
		function layoutTimeline(rows) {
			const joined = [], open = new Map();
			for (const row of rows) {
				if (row.kind === "activity") {
					const key = `${row.title}|${row.end}`, newer = open.get(key);
					if (newer) {
						open.delete(key);
						newer.at = row.at;
						newer.apps = [...new Set([...(newer.apps || []), ...(row.apps || [])])];
						open.set(`${row.title}|${row.at}`, newer);
						continue;
					}
					const copy = { ...row };
					open.set(`${row.title}|${row.at}`, copy);
					joined.push(copy);
				} else joined.push(row);
			}
			joined.sort((a, b) => (b.end ?? b.at) - (a.end ?? a.at) || b.at - a.at);
			return joined.map((row, i) => {
				const above = joined[i - 1];
				const sameMoment = Boolean(above) && clock(above.at) === clock(row.at);
				const minutes = above ? Math.max(0, (above.at - (row.end ?? row.at)) / 60e3) : 0;
				return { ...row, sameMoment, gap: !above ? 0 : sameMoment ? 0 : Math.round(Math.min(40, Math.max(6, minutes / 2))) };
			});
		}
		const HISTORY_ASKS = ["我上次休息前在做什么？", "今天做了哪些事，各到哪一步了？", "今天谁找过我、要我跟进？"];
		const HISTORY_TONE = { review: "wait", decision: "wait", failed: "fail", done: "done", thread: "run", owner: "idle", card: "idle" };
		/**
		 * The 时间线 tab, laid out like ChatGPT's Computer History: today and yesterday, newest first, one rail through
		 * the stretches of computer activity and the project events between them; a dashed rail marks a break of more
		 * than half an hour. Questions about it go to Mira's conversation.
		 */
		function MiraHistory({ today, waiting, openTaskBy }) {
			const y = new Date(Date.now() - 86400e3), yKey = `${y.getFullYear()}-${String(y.getMonth() + 1).padStart(2, "0")}-${String(y.getDate()).padStart(2, "0")}`;
			const yesterday = useShared("api/mira-projects/today?day=" + yKey, { timeline: [] }), snap = usePersonal();
			const [askError, setAskError] = useState(null);
			// Only the assistant's own written judgment; a count of what waits is already on 待你决定.
			const judge = today.recap?.latest?.headline || "";
			// What the computer shows right now (the latest screen observation), so the owner sees Mira is following along.
			const live = today.computer?.live, fresh = live && Date.now() - live.at < 5 * 60e3;
			const askMira = async text => {
				setAskError(null);
				try { await act("buttonSent", { text }); await shell.sessions.using(snap.sessionId, { source: "sidebarView" }, async ref => { await ref.ready; const r = await ref.binding.session.prompt([{ type: "text", text }], "queue"); if (r && r.ok === false) throw new Error(r.error?.message || "没发出去"); }); }
				catch (e) { setAskError(String(e?.message || e)); }
			};
			// A burst of one project's events (newest first, each within 15 minutes of the next) is one stop on the rail.
			const fold = rows => rows.reduce((out, e) => {
				const last = out.at(-1);
				if (e.kind !== "activity" && last?.kind === "events" && last.projectId === e.projectId && last.at - e.at <= HISTORY_BURST) { last.lines.push(e);last.at = e.at;return out; }
				out.push(e.kind === "activity" ? e : { kind: "events", ref: e.ref, projectId: e.projectId, projectName: e.projectName, at: e.at, end: e.at, lines: [e] });
				return out;
			}, []);
			const days = [["今天", today.timeline], ["昨天", yesterday.timeline]]
				.map(([label, rows]) => ({ label, rows: layoutTimeline(fold([...(rows || [])].sort((a, b) => b.at - a.at))) })).filter(d => d.rows.length);
			const item = (day, e, i, rows) => {
				const activity = e.kind === "activity", end = e.end ?? e.at, newer = rows[i - 1], older = rows[i + 1];
				const tones = activity ? [] : e.lines.map(l => HISTORY_TONE[l.kind] || "idle"), tone = activity ? "idle" : ["fail", "wait", "done", "run"].find(t => tones.includes(t)) || "idle";
				const thread = !activity && e.lines.every(l => l.threadId && l.threadId === e.lines[0].threadId) ? e.lines[0].threadId : null;
				const gapBefore = newer && newer.at - end > HISTORY_GAP, gapAfter = older && e.at - (older.end ?? older.at) > HISTORY_GAP;
				const body = activity
					? h("div", { className: "mp4-tl-body" },
						h("div", { className: "mp4-tl-head" }, h("h3", { className: "mp4-tl-title" }, e.title),
							snap.sessionId ? h("button", { type: "button", className: "mp4-tl-act", onClick: () => askMira(`我${day === "昨天" ? "昨天" : ""} ${clock(e.at)}–${clock(e.end)} 在「${e.title}」，具体做了什么？`) }, "问助理") : null),
						e.detail ? h("p", { className: "mp4-tl-desc" }, e.detail) : null,
						h("div", { className: "mp4-tl-chips" },
														(e.apps || []).map(app => h("span", { key: app, className: "mp4-tl-chip mp4-tl-app" }, h(AppIcon, { name: app, size: 14 }), app)),
							e.projectId ? h("button", { type: "button", className: "mp4-tl-chip mp4-tl-proj", onClick: () => openProject(e.projectId) }, e.projectLabel) : null))
					: h("button", { type: "button", className: "mp4-tl-body mp4-tl-main", onClick: () => thread ? openTaskBy(e.projectId, thread) : e.projectId ? openProject(e.projectId) : undefined },
						h("span", { className: "mp4-tl-title" }, e.projectName),
						e.lines.slice(0, 3).map(l => h("span", { key: l.ref, className: "mp4-tl-desc" }, l.text)),
						e.lines.length > 3 ? h("span", { className: "mp4-tl-desc mp4-muted" }, `还有 ${e.lines.length - 3} 条`) : null);
				// Several stretches share a summary's ref and a window's start, so the key carries the row's place too.
				return h("article", { key: `${e.ref}:${e.at}:${i}`, className: "mp4-tl" + (e.sameMoment ? " mp4-tl-same" : ""), style: { "--tl-gap": `${e.gap}px` } },
					h("time", { className: "mp4-tl-time", dateTime: new Date(e.at).toISOString(), title: end > e.at ? `${clock(e.at)} – ${clock(end)}` : undefined },
						e.sameMoment ? "" : clock(e.at), !e.sameMoment && end - e.at > 15 * 60e3 ? h("small", { className: "mp4-tl-until" }, clock(end)) : null),
					h("span", { className: "mp4-tl-rail" + (gapBefore ? " mp4-tl-gap-before" : "") + (gapAfter ? " mp4-tl-gap-after" : ""), "aria-hidden": true },
						h("i", { className: "mp4-tl-dot mp4-dot-" + tone })),
					body);
			};
			return h(React.Fragment, null,
				judge ? h(Section, { label: "今天的判断" }, h("p", { className: "mp4-judge" }, judge)) : null,
				h("div", { className: "mp4-tl-top" },
					live ? h("p", { className: "mp4-live", title: "屏幕记录器最近一次截到的前台窗口。开着但一直在后台、没切到前面的应用不会出现在这里。" }, h(Dot, { tone: fresh ? "run" : "idle" }), live.app ? h(AppIcon, { name: live.app, size: 16 }) : null,
						h("span", { className: "mp4-ell" }, fresh ? parts("正在记录", `前台：${live.app || ""}`, live.title && live.title !== live.app ? live.title : null) : parts(`上次记录 ${ago(live.at)}`, live.app))) : null,
					live && fresh && (live.recent || []).length ? h("p", { className: "mp4-live-recent" }, `最近 10 分钟也在前台用过：${live.recent.join("、")}`) : null,
					snap.sessionId ? h("div", { className: "mp4-sugs" }, HISTORY_ASKS.map(q => h("button", { key: q, type: "button", className: "mp4-sug", onClick: () => askMira(q) }, q))) : null,
					askError ? h("span", { className: "mp-error" }, askError) : null),
				days.length === 0 ? h("p", { className: "mp4-muted" }, "还没有电脑记录和项目动态。") : null,
				days.map(d => h(Section, { key: d.label, label: d.label }, h("div", { className: "mp4-tl-list" }, d.rows.map((e, i, rows) => item(d.label, e, i, rows))))));
		}

		/**
		 * Feishu messages that reached the owner directly and still wait for their reply, as the server found them: a
		 * lighter group under 待你决定. Mira only reminds; the owner asks her how to answer, or sets it aside.
		 */

		/** How many of a project's open tasks the tree shows before 「还有 N 个」. */
		const TREE_PEEK = 5;
		/**
		 * Every project and what is still open in it: what waits for the owner first, at most TREE_PEEK tasks until asked
		 * for more, finished work folded. Deleting moves a project to the trash at the foot of the list, where it can be
		 * restored; a task is taken off the list by archiving it.
		 */
		function ProjectTree({ nav }) {
			const [open, setOpen] = useState({}), [whole, setWhole] = useState({}), [doneOpen, setDoneOpen] = useState({});
			const [asking, setAsking] = useState(null), [trashOpen, setTrashOpen] = useState(false), [busy, setBusy] = useState(false), [note, setNote] = useState(null);
			const flip = (set, id, now) => set(s => ({ ...s, [id]: !now }));
			const rank = t => (isWaiting(t) ? 0 : isRunning(t) ? 1 : 2);
			const rows = (nav.projects || []).map(p => {
				const live = (p.threads || []).filter(t => !t.resolved);
				const active = live.filter(t => t.state !== "done").sort((a, b) => rank(a) - rank(b) || (b.updatedAt || 0) - (a.updatedAt || 0));
				return { p, active, done: live.filter(t => t.state === "done"), waiting: active.filter(isWaiting).length, running: active.filter(isRunning).length };
			});
			const trash = nav.trash || [], count = key => rows.reduce((n, r) => n + r[key], 0);
			// `where` is the project the message belongs under, or "list" for the foot of the list.
			const run = async (where, work) => {
				setBusy(true); setNote(null);
				try { await work(); } catch (error) { setNote({ where, text: error.message, bad: true }); }
				setBusy(false);
			};
			const remove = p => run(p.id, async () => { await act("deleteProject", { projectId: p.id }); setAsking(null); setTrashOpen(true); });
			const restore = x => run("list", async () => {
				const { pausedAutomations } = await act("restoreProject", { projectId: x.id });
				if (pausedAutomations) setNote({ where: "list", text: `「${x.name}」恢复了。它的 ${pausedAutomations} 个自动任务还停着，要跑得再打开。` });
			});
			const archive = (p, tasks) => run("list", async () => { for (const t of tasks) await act("updateThread", { projectId: p.id, threadId: t.id, resolved: true }); });
			const taskRow = (p, t, quiet) => {
				const [state] = taskPill(t), queued = t.experience?.execution?.phase === "queued";
				// The counts beside the project already say 待处理 / 进行中; a row spells out only what differs from that.
				const label = t.state === "review_ready" || t.state === "done" || (t.state === "working" && !queued) ? "" : state;
				// Not in the middle of a turn: what the agent is writing would land in a task nobody sees.
				const canArchive = !t.experience?.execution?.active;
				return h("div", { key: t.id, className: "mp4-tree-row mp4-tree-child" + (canArchive ? " mp4-tree-can-archive" : "") },
					h("button", { type: "button", className: "mp4-row" + (quiet ? " mp4-muted" : ""), title: `${t.title}，${state}`, onClick: () => openTask(p.id, t) },
						h(StatusIcon, { status: taskStatus(t), size: 16 }), h("span", { className: "mp4-ell mp4-tree-title" }, t.title),
						label ? h("span", { className: "mp4-muted mp4-tree-meta" }, label) : null,
						h("span", { className: "mp4-muted mp4-tree-meta mp4-tree-when" }, ago(t.updatedAt))),
					canArchive ? h("button", { type: "button", className: "mp4-link-btn mp4-tree-archive", title: "从列表里收起。记录还在，项目的「已归档」里能找回", disabled: busy, onClick: () => archive(p, [t]) }, "归档") : null);
			};
			const project = ({ p, active, done, waiting, running }) => {
				const isOpen = open[p.id] ?? active.length > 0, shown = whole[p.id] ? active : active.slice(0, TREE_PEEK), total = (p.threads || []).length;
				return h("div", { key: p.id, className: "mp4-tree-project" },
					h("div", { className: "mp4-tree-row" },
						h("button", { type: "button", className: "mp4-tree-chev", "aria-label": `${isOpen ? "收起" : "展开"} ${p.name}`, "aria-expanded": isOpen, disabled: !total, style: total ? undefined : { visibility: "hidden" }, onClick: () => flip(setOpen, p.id, isOpen) }, h(Icon, { name: "chevron", size: 12 })),
						h("button", { type: "button", className: "mp4-row", title: `打开项目「${p.name}」`, onClick: () => openProject(p.id) },
							h(ProjectMark, { p, size: 16, plain: true }), h("b", { className: "mp4-ell mp4-tree-title" }, p.name),
							waiting ? h("span", { className: "mp4-count" }, `${waiting} 件等你`) : null,
							running ? h("span", { className: "mp4-count" }, `${running} 件在做`) : null,
							waiting || running || !done.length ? null : h("span", { className: "mp4-muted mp4-tree-meta" }, `${done.length} 已完成`)),
						h(MoreMenu, { items: [
							done.length ? [`归档已完成的 ${done.length} 个任务`, () => archive(p, done)] : null,
							["删除项目…", () => { setNote(null); setAsking(p.id); }]] })),
					asking === p.id ? h("div", { className: "mp4-tree-confirm", role: "group", "aria-label": `删除项目 ${p.name}` },
						h("b", null, `删除「${p.name}」？`),
						h("span", { className: "mp4-muted" }, `${total ? `项目和它的 ${total} 个任务` : "这个项目"}会移进回收站，什么都不会被抹掉，随时能恢复。`),
						note && note.where === p.id ? h("span", { className: "mp4-tree-error" }, note.text) : null,
						h("span", { className: "mp4-tree-confirm-actions" },
							h("button", { type: "button", className: "mp4-sug mp4-sug-danger", disabled: busy, onClick: () => remove(p) }, busy ? "正在移…" : "移进回收站"),
							h("button", { type: "button", className: "mp4-sug", disabled: busy, onClick: () => setAsking(null) }, "取消"))) : null,
					isOpen ? h(React.Fragment, null,
						shown.map(t => taskRow(p, t)),
						active.length > TREE_PEEK ? h("button", { type: "button", className: "mp4-link-btn mp4-tree-fold", onClick: () => flip(setWhole, p.id, whole[p.id]) }, whole[p.id] ? "收起" : `还有 ${active.length - TREE_PEEK} 个`) : null,
						done.length ? h("button", { type: "button", className: "mp4-link-btn mp4-tree-fold", "aria-expanded": Boolean(doneOpen[p.id]), onClick: () => flip(setDoneOpen, p.id, doneOpen[p.id]) }, `${doneOpen[p.id] ? "▾" : "▸"} 已完成 ${done.length}`) : null,
						doneOpen[p.id] ? done.map(t => taskRow(p, t, true)) : null,
						active.length || done.length ? null : h("p", { className: "mp4-muted mp4-tree-fold" }, "还没有任务")) : null);
			};
			return h("div", { className: "mp4-tree" },
				rows.length ? h("div", { className: "mp4-bd-head" }, h("b", null, "项目"), h("span", { className: "mp4-bd-count" }, [`${rows.length} 个项目`, count("waiting") ? `${count("waiting")} 件等你` : null].filter(Boolean).join("，"))) : h("p", { className: "mp4-muted" }, "还没有项目。"),
				rows.map(project),
				note && note.where === "list" ? h("p", { className: note.bad ? "mp4-tree-error" : "mp4-muted mp4-note" }, note.text) : null,
				trash.length ? h("div", { className: "mp4-tree-trash" },
					h("button", { type: "button", className: "mp4-link-btn", "aria-expanded": trashOpen, onClick: () => setTrashOpen(v => !v) }, h("span", { className: "mp4-disclose" }, h(Icon, { name: "chevron", size: 12 }), `回收站 ${trash.length}`)),
					trashOpen ? h(React.Fragment, null,
						trash.map(x => h("div", { key: x.id, className: "mp4-tree-row mp4-tree-gone" },
							h("span", { className: "mp4-ell mp4-tree-title" }, x.name),
							h("span", { className: "mp4-muted mp4-tree-meta" }, parts(x.threads ? x.threads + " 个任务" : "没有任务", `${ago(x.deletedAt)}删除`)),
							h("button", { type: "button", className: "mp4-link-btn", disabled: busy, onClick: () => restore(x) }, "恢复")))) : null) : null);
		}



		// ---------------------------------------------------------------- 待你决定: each project's progress; every ask is a sentence for the composer
		const kanbanDay = at => { const d = new Date(at); return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`; };
		function kanbanHidden(key) { try { return localStorage.getItem("mira-kanban:" + key) === kanbanDay(Date.now()); } catch (_e) { return false; } }
		/** Today's dream picks to show, or null. Pure: the same inputs give the same answer. */
		function kanbanLine({ dream, now, hidden }) {
			const latest = dream && dream.latest;
			if (!latest || latest.day !== kanbanDay(now) || !latest.picked.length || hidden("dream:" + latest.day)) return null;
			return { key: "dream:" + latest.day, dream: latest, why: `昨晚 ${clock(latest.startedAt)} 开始，留下 ${latest.picked.length} 件` };
		}
		/**
		 * Laid out like the Claude Code app's next-step box: what stands is shown, and what you might do about it is offered
		 * in your own words. A chip or a suggestion never acts by itself: it puts its sentence into the personal assistant's
		 * composer, where you can change it before sending, and the assistant does it with its own tools (accept, answer,
		 * relay, open a task). Looking at a task stays a click on its row.
		 */
		/** The sentence a dream pick offers: the night's own words when it wrote some, else one by its lane. Pure. */
		function pickAsk(p, projectName) {
			if (typeof p.ask === "string" && p.ask.trim()) return p.ask.trim();
			if (p.lane === "do") return projectName ? `在「${projectName}」开个任务：${p.title}` : `开个任务：${p.title}`;
			if (p.lane === "read") return `带我看看「${p.title}」，说说要点`;
			return `关于「${p.title}」${p.body ? `：${sentence(p.body)}` : "，"}说说你的看法和建议。`;
		}
		/** A line that ends a sentence, so a draft built from it reads as one whole request. Pure. */
		function sentence(text) { const t = String(text || "").trim(); return !t || /[。！？.!?」）)]$/.test(t) ? t : t + "。"; }
		/** Projects with something to show today: those holding asks first, then the most recently active. Pure. */
		function inboxProjects(projects, waiting) {
			return (projects || []).map(p => {
				const waits = waiting.filter(x => x.projectId === p.id), held = new Set(waits.map(x => x.threadId));
				return {
					id: p.id, name: p.name, lastAt: Math.max(p.lastAt || 0, ...waits.map(x => x.since || 0)), waits,
					working: (p.working || []).filter(t => !held.has(t.id)), done: (p.done || []).filter(t => !held.has(t.id)),
				};
			}).filter(p => p.waits.length || p.working.length || p.done.length)
				.sort((a, b) => (b.waits.length ? 1 : 0) - (a.waits.length ? 1 : 0) || b.lastAt - a.lastAt);
		}
		/** How many open rows a project shows before 「还有 N 件」. */
		const INBOX_PEEK = 4;
		/** What a reminder's chip says and fills in, by where it came from. Pure. */
		function noteAsks(note) {
			if (note.from === "followup") return [["帮我回", `${note.who}在${note.app}「${note.where}」找我，原话「${note.text}」。帮我起草一条回复，先给我看，别直接发。`]];
			if (note.from === "card") return [["接着说", note.card && typeof note.card.ask === "string" && note.card.ask.trim() ? note.card.ask.trim() : `「${note.title}」${note.text ? `：${sentence(note.text)}` : "。"}帮我推进这件事：先说你的建议，需要我拍板的列出选项。`]];
			const label = note.lane === "do" ? "开个任务" : note.lane === "read" ? "带我看看" : "聊聊";
			return [[label, note.ask]];
		}
		/**
		 * Everything that is not a project's progress, as one stream read like a secretary's messages: who it is from, when,
		 * one line, the detail, and what you might say back. Newest first. Pure.
		 */
		function inboxNotes({ followups, cards, dream, projectName }) {
			// kind: "message" is news from outside (a Feishu message, what the computer shows); "suggest" is the assistant's own idea.
			const who = f => !f.ask ? `${f.who}在「${f.title}」找你` : f.who && !f.ask.includes(f.who) ? `${f.who}：${f.ask}` : f.ask;
			const notes = [
				...(followups || []).map(f => ({ key: "f" + f.conversation, from: "followup", kind: "message", rank: 0, app: f.app || "飞书", place: `${f.app || "飞书"}　${f.title}`, who: f.who, where: f.title, title: who(f), text: f.text, at: f.since, f })),
				...(cards || []).filter(c => c.state === "open").map(c => {
					// The icon is where the matter came from (the event the card cites); the foot is its project when it has one.
					const src = c.source || null, project = c.target && c.target.projectName, idea = c.via === "next-steps" || !src;
					return { key: "c" + c.id, from: "card", kind: idea ? "suggest" : "message", rank: 1, app: src ? src.app : "个人助理", place: project || (src ? [src.app === "电脑" ? "" : src.app, src.where].filter(Boolean).join("　") : "个人助理"), who: "个人助理", title: c.title, text: c.body || "", at: c.at, card: c };
				}),
				...(dream ? dream.picked.filter(p => !p.reaction).map(p => ({ key: "p" + p.lane, from: "pick", kind: "suggest", rank: 1, app: "个人助理", place: projectName(p.projectId) || "个人助理", who: "个人助理", lane: p.lane, title: p.title, text: p.body || "", at: dream.startedAt, ask: pickAsk(p, projectName(p.projectId)), pick: p })) : []),
			];
			// Someone waiting on a reply is what slips by most easily: first; the rest newest first.
			return notes.sort((a, b) => a.rank - b.rank || (b.at || 0) - (a.at || 0));
		}
		/** What a task card says it needs from you, in one word before its name. */
		const TASK_VERB = { review: "验收", decision: "拍板", failed: "没做成", working: "在做", done: "做完了" };
		/**
		 * 待你决定 as a board read like an inbox: lanes by what each item needs from you (等你, 进行中, 今天完成), one card per
		 * item, newest first. No buttons on the cards: a task opens on click; a message or a note puts a reply to start from
		 * into the composer. Status names live in tooltips. Lanes sit side by side when the panel is wide and stack when narrow.
		 */
		function MiraInbox({ today, waiting, nav, openTaskBy }) {
			const [error, setError] = useState(null), [whole, setWhole] = useState({}), [showDone, setShowDone] = useState(false);
			const draft = (from, text) => setError(draftInto(from, text) ? null : "先打开个人助理的对话，再点这里");
			const say = kanbanLine({ dream: today.dream, now: Date.now(), hidden: kanbanHidden });
			const projects = inboxProjects(today.projects, waiting);
			const known = id => (nav.projects || []).find(p => p.id === id);
			const nameOf = id => (known(id) || (today.projects || []).find(p => p.id === id) || {}).name || "";
			const notes = inboxNotes({ followups: today.followups, cards: today.cards, dream: say && say.dream, projectName: nameOf });
			const skills = (today.dream && today.dream.skills) || [], anySkill = skills.some(s => s.proposal || s.trial);
			const run = async work => { setError(null); try { await work(); } catch (e) { setError(String(e?.message || e)); } };
			const react = (p, reaction) => run(() => act("dreamReact", { day: say.dream.day, lane: p.lane, reaction }));
			// Where a note came from: the app's own mark (飞书), the computer, or a bulb for the assistant's own idea.
			// Where a note came from: the app's own mark (飞书, 邮件), the computer, or the assistant's own avatar for its ideas.
			const sourceIcon = (n, size = 16) => n.kind === "suggest" || !n.app || n.app === "个人助理" ? h(MiraAvatar, { size, title: "个人助理" })
				: n.app === "电脑" ? h("span", { className: "mp4-kb-glyph" }, h(Icon, { name: "monitor", size })) : h(AppIcon, { name: n.app, size });
			const card = ({ key, icon, iconTip, title, body, when, open, dismiss, quiet, tip }) => h("div", {
				key, className: "mp4-kb-card" + (quiet ? " mp4-kb-quiet" : ""), role: "button", tabIndex: 0, title: tip,
				onClick: e => open(e.currentTarget), onKeyDown: e => { if (e.key === "Enter" && e.target === e.currentTarget) open(e.currentTarget); },
			},
				h("span", { className: "mp4-kb-top" }, h("span", { className: "mp4-kb-icon", title: iconTip }, icon), h("span", { className: "mp4-kb-title" }, title), when ? h("span", { className: "mp4-kb-when" }, when) : null),
				body ? h("span", { className: "mp4-kb-body" }, body) : null,
				dismiss ? h("button", { type: "button", className: "mp4-kb-x", title: dismiss[0], "aria-label": dismiss[0], onClick: e => { e.stopPropagation(); dismiss[1](); } }, h(Icon, { name: "close", size: 12 })) : null);
			// A task says in its first word what it needs (验收, 拍板, 没做成, 在做); its colour says the same at a glance.
			const task = (p, t, status, body) => card({ key: t.key || t.id, icon: h(StatusIcon, { status }), iconTip: STATUS_LOOK[status]?.[0], title: `${TASK_VERB[status]}：${t.title}`,
				body: body || null, when: ago(t.since || t.updatedAt), open: () => openTaskBy(p.id, t.threadId || t.id), quiet: status === "working" || status === "done", tip: `打开「${t.title}」` });
			const waitStatus = x => x.kind === "decision" ? "decision" : x.kind === "failed" ? "failed" : "review";
			const oldest = list => [...list].sort((a, b) => (a.since || 0) - (b.since || 0));
			const groups = projects.map(p => ({ p, cards: [
				...oldest(p.waits).map(x => task(p, x, waitStatus(x), x.kind === "decision" ? x.ask : x.summary)),
				...p.working.map(t => task(p, t, "working", t.summary)),
				...(showDone ? p.done.map(t => task(p, t, "done", t.summary)) : []),
			] })).filter(g => g.cards.length);
			const note = n => {
				const reply = noteAsks(n)[0][1];
				const dismiss = n.from === "followup" ? ["不用回", () => run(() => act("followupDismiss", { conversation: n.f.conversation, through: n.f.through }))]
					: n.from === "card" ? ["不用了", () => run(() => act("cardAction", { projectId: "hub", cardId: n.card.id, actionId: "dismiss" }))]
					: ["不感兴趣", () => react(n.pick, "not_interested")];
				const after = n.from === "pick" ? () => react(n.pick, n.lane === "do" ? "started" : n.lane === "read" ? "opened" : "noted") : null;
				return card({ key: n.key, icon: sourceIcon(n), iconTip: n.kind === "suggest" ? "个人助理的建议" : n.place, title: n.title, body: n.from === "followup" ? `「${n.text}」` : n.text,
					when: n.at ? ago(n.at) : "", dismiss, tip: `填进输入框：${reply}`, open: from => { draft(from, reply); if (after) after(); } });
			};
			// 待你决定 is what needs you: finished work stays out of sight until asked for.
			const doneCount = projects.reduce((n, p) => n + p.done.length, 0);
			const doneTool = doneCount ? h("button", { type: "button", className: "mp4-kb-tool" + (showDone ? " mp4-kb-tool-on" : ""), title: showDone ? "收起今天完成" : `今天完成 ${doneCount} 件`, "aria-label": showDone ? "收起今天完成" : `今天完成 ${doneCount} 件`, "aria-pressed": showDone, onClick: () => setShowDone(v => !v) }, h(Icon, { name: "check", size: 14 })) : null;
			const diary = say ? h("button", { type: "button", className: "mp4-kb-tool", title: "昨晚的日记", "aria-label": "昨晚的日记", onClick: () => window.open("api/mira-projects/dream/diary?day=" + encodeURIComponent(say.dream.day), "_blank", "noopener") }, h(Icon, { name: "moon", size: 14 })) : null;
			const more = (id, list, cap) => list.length > cap || whole[id] && list.length > cap ? h("button", { type: "button", className: "mp4-kb-more", onClick: () => setWhole(s => ({ ...s, [id]: !s[id] })) }, whole[id] ? "收起" : `还有 ${list.length - cap} 件`) : null;
			const head = (label, tools) => h("h3", { className: "mp4-kb-head" }, label, tools.some(Boolean) ? h("span", { className: "mp4-kb-tools" }, tools) : null);
			const news = notes.map(note), shownNews = whole.news ? news : news.slice(0, 8);
			const shownGroups = whole.progress ? groups : groups.slice(0, 4);
			if (today.board) return h(React.Fragment, null,
				h(MiraBoard, { board: today.board, known, notes, noteRow: n => noteRow(n, sourceIcon, run, react, draft), whole, setWhole, diary, openTaskBy }),
				error ? h("p", { className: "mp4-err" }, error) : null);
			const blocks = [
				groups.length || doneCount ? h("section", { key: "progress", className: "mp4-kb-lane mp4-kb-lane-progress" }, head("项目进展", [doneTool]),
					groups.length ? h("div", { className: "mp4-kb-cards" }, shownGroups.map(({ p, cards }) => h("div", { key: p.id, className: "mp4-kb-group" },
						h("button", { type: "button", className: "mp4-kb-proj", title: `打开项目「${p.name}」`, onClick: () => navigateProject(p.id) }, known(p.id) ? h(ProjectMark, { p: known(p.id), size: 14, plain: true }) : null, h("span", { className: "mp4-ell" }, p.name)),
						cards)), more("progress", groups, 4)) : h("p", { className: "mp4-kb-none" }, "项目里没有要你处理的事。")) : null,
				news.length || diary ? h("section", { key: "news", className: "mp4-kb-lane mp4-kb-lane-news" }, head("新消息与建议", [diary]),
					news.length ? h("div", { className: "mp4-kb-cards" }, shownNews, more("news", news, 8)) : h("p", { className: "mp4-kb-none" }, "没有新消息。")) : null,
			].filter(Boolean);
			if (!blocks.length && !anySkill) return h("p", { className: "mp4-ib-empty" }, "现在没有要你处理的事。");
			return h(React.Fragment, null,
				h("div", { className: "mp4-kb" }, blocks),
				h(SkillRows, { skills }),
				error ? h("p", { className: "mp4-err" }, error) : null);
		}

		/**
		 * 待你决定, read like the Claude app's side panel (2026-11-18): the few projects that are really moving, each with a
		 * short paragraph a small model rewrote from the project's record (board-digest.js) and the things that wait on you
		 * as rows; then news and the assistant's suggestions. Quiet projects stay in the sidebar (owner 2026-11-19). Laid out like
		 * Feishu's 任务 panel: white cards on a gray canvas, one per project and one for the news; rows light up on hover. Older waiting rows and
		 * older news fold away. Nothing acts by itself: a row opens its task, a note puts a sentence into the composer.
		 */
		const BOARD_VERB = { review: "等你验收", decision: "等你拍板", failed: "没做成" };
		const BOARD_PEEK = 3;
		function MiraBoard({ board, known, notes, noteRow, whole, setWhole, diary, openTaskBy }) {
			const toggle = key => setWhole(s => ({ ...s, [key]: !s[key] }));
			const head = (label, count, tools) => h("div", { className: "mp4-bd-head" }, h("b", null, label), h("span", { className: "mp4-bd-count" }, count), tools || null);
			const project = a => {
				const rows = whole["rows:" + a.projectId] ? a.rows : a.rows.slice(0, BOARD_PEEK), rest = a.rows.slice(BOARD_PEEK), p = known(a.projectId);
				return h("div", { key: a.projectId, className: "mp4-bd-proj" },
					h("button", { type: "button", className: "mp4-bd-proj-h", title: `打开项目「${a.name}」`, onClick: () => navigateProject(a.projectId) },
						p ? h(ProjectMark, { p, size: 16, plain: true }) : null, h("b", { className: "mp4-ell" }, a.name), h("time", null, ago(a.lastAt))),
					a.story ? h("p", { className: "mp4-bd-story" }, a.story) : null,
					a.rows.length ? h("div", { className: "mp4-bd-rows" },
						rows.map(r => h("button", { key: r.threadId, type: "button", className: "mp4-bd-row", title: `打开「${r.title}」`, onClick: () => openTaskBy(a.projectId, r.threadId) },
							h(StatusIcon, { status: r.kind, size: 16 }), h("span", { className: "mp4-bd-verb" }, BOARD_VERB[r.kind]), h("span", { className: "mp4-bd-line" }, r.line || r.title),
							h("time", null, ago(r.since)))),
						rest.length ? h("button", { type: "button", className: "mp4-bd-more", onClick: () => toggle("rows:" + a.projectId) },
							whole["rows:" + a.projectId] ? "收起" : `还有 ${rest.length} 件等你，最早 ${ago(Math.min(...rest.map(r => r.since)))}`) : null) : null);
			};
			const shownNotes = whole.news ? notes : notes.slice(0, BOARD_PEEK);
			const sections = [
				board.active.length ? h("section", { key: "active", className: "mp4-bd-sec" }, head("项目进展", `${board.active.length} 个项目`), board.active.map(project)) : null,
				notes.length || diary ? h("section", { key: "news", className: "mp4-bd-sec" }, head("新消息与建议", notes.length ? `${notes.length} 条` : "", diary),
					notes.length ? h("div", { className: "mp4-bd-card" }, shownNotes.map(noteRow),
						notes.length > BOARD_PEEK ? h("button", { type: "button", className: "mp4-bd-more", onClick: () => toggle("news") }, whole.news ? "收起" : `还有 ${notes.length - BOARD_PEEK} 条`) : null) : null) : null,
			].filter(Boolean);
			return sections.length ? h("div", { className: "mp4-bd" }, sections) : h("p", { className: "mp4-ib-empty" }, "现在没有要你处理的事。");
		}
		/** One piece of news or suggestion as a list row: where it came from, what it is, one line more, when; × sets it aside. */
		function noteRow(n, sourceIcon, run, react, draft) {
			const reply = noteAsks(n)[0][1];
			const dismiss = n.from === "followup" ? ["不用回", () => run(() => act("followupDismiss", { conversation: n.f.conversation, through: n.f.through }))]
				: n.from === "card" ? ["不用了", () => run(() => act("cardAction", { projectId: "hub", cardId: n.card.id, actionId: "dismiss" }))]
				: ["不感兴趣", () => react(n.pick, "not_interested")];
			const after = n.from === "pick" ? () => react(n.pick, n.lane === "do" ? "started" : n.lane === "read" ? "opened" : "noted") : null;
			const open = from => { draft(from, reply); if (after) after(); };
			return h("div", { key: n.key, className: "mp4-bd-msg", role: "button", tabIndex: 0, title: `填进输入框：${reply}`,
				onClick: e => open(e.currentTarget), onKeyDown: e => { if (e.key === "Enter" && e.target === e.currentTarget) open(e.currentTarget); } },
				h("span", { className: "mp4-bd-msg-ic" }, sourceIcon(n, 16)),
				h("span", { className: "mp4-bd-msg-b" }, h("b", null, n.title), n.text ? h("span", null, n.from === "followup" ? `「${n.text}」` : n.text) : null),
				h("time", null, n.at ? ago(n.at) : ""),
				h("button", { type: "button", className: "mp4-kb-x", title: dismiss[0], "aria-label": dismiss[0], onClick: e => { e.stopPropagation(); dismiss[1](); } }, h(Icon, { name: "close", size: 12 })));
		}

		// ---------------------------------------------------------------- 技能自迭代: an executor's 做事说明 and its versions
		/** Sentences of a 做事说明, for a before/after view: what went, what came. Mechanical, no judgement. */
		function sentencesOf(text) { return String(text || "").split(/(?<=[。；！？\n])/).map(x => x.trim()).filter(Boolean); }
		function skillDiff(before, after) {
			const a = sentencesOf(before), b = sentencesOf(after);
			return { gone: a.filter(x => !b.includes(x)), came: b.filter(x => !a.includes(x)), kept: b.filter(x => a.includes(x)).length };
		}
		/** One drafted change: why, the feedback it rests on, what went and what came, and the three choices. */
		function SkillProposal({ skill }) {
			const [busy, setBusy] = useState(false), [error, setError] = useState(null), p = skill.proposal, d = skillDiff(p.before, p.after);
			const decide = async choice => { setBusy(true); setError(null); try { await act("skillDecide", { id: p.id, choice }); } catch (e) { setError(e.message); } finally { setBusy(false); } };
			return h("div", { className: "mp4-skill-diff" },
				p.why ? h("p", { className: "mp4-skill-why" }, p.why) : null,
				h("div", { className: "mp4-skill-lines" },
					d.gone.map((x, i) => h("div", { key: "g" + i, className: "mp4-skill-gone" }, "− ", x)),
					d.came.map((x, i) => h("div", { key: "c" + i, className: "mp4-skill-came" }, "+ ", x)),
					d.kept ? h("div", { className: "mp4-skill-kept" }, `其余 ${d.kept} 句不变`) : null),
				h("p", { className: "mp4-skill-k" }, `依据：交付后你说过的 ${p.refs.length} 句话`),
				p.refs.map((r, i) => h("p", { key: i, className: "mp4-skill-ref" }, `${clock(r.at)}　任务「${r.thread}」：${r.text}`)),
				h("div", { className: "mp4-kanban-acts" },
					h("button", { type: "button", className: "mp4-btn mp4-btn-strong", disabled: busy, onClick: () => decide("apply") }, "用新版"),
					h("button", { type: "button", className: "mp4-btn", disabled: busy, onClick: () => decide("trial") }, "先试一次"),
					h("button", { type: "button", className: "mp4-btn", disabled: busy, onClick: () => decide("keep") }, "保持旧版")),
				h("p", { className: "mp4-skill-k" }, "改的只是做事说明，不改它能做什么。从下一个新任务开始生效。"),
				error ? h("p", { className: "mp4-err" }, error) : null);
		}
		/** Under the companion: drafts waiting for the owner, and a version on trial waiting to be kept or sent back. */
		function SkillRows({ skills }) {
			const [open, setOpen] = useState(null), [error, setError] = useState(null);
			const rows = skills.filter(s => s.proposal || s.trial);
			if (!rows.length) return null;
			const settle = async (s, keep) => { setError(null); try { await act("skillSettle", { projectId: s.projectId, memberId: s.memberId, keep }); } catch (e) { setError(e.message); } };
			return h("div", { className: "mp4-kanban-rows" },
				rows.map(s => h(React.Fragment, { key: s.key },
					h("div", { className: "mp4-kanban-row" },
						h("span", { className: "mp4-kanban-lane" }, "做事说明"),
						h("span", { className: "mp4-kanban-item" }, s.proposal ? `「${s.projectName}」${s.name}有一版修改` : `「${s.projectName}」${s.name}在试用新说明`),
						s.proposal ? h("span", { className: "mp4-kanban-row-acts" }, h("button", { type: "button", className: "mp4-link-btn", "aria-expanded": open === s.key, onClick: () => setOpen(open === s.key ? null : s.key) }, open === s.key ? "收起" : "看对比"))
							: h("span", { className: "mp4-kanban-row-acts" }, h("button", { type: "button", className: "mp4-link-btn", onClick: () => settle(s, true) }, "留下"), h("button", { type: "button", className: "mp4-link-btn", onClick: () => settle(s, false) }, "退回"))),
					open === s.key && s.proposal ? h(SkillProposal, { skill: s }) : null)),
				error ? h("p", { className: "mp4-err" }, error) : null);
		}
		/** 助理设置 → 我的技能: each executor's uses, rework and versions; a pending draft; going back to any version. */
		function SkillsPanel({ skills }) {
			const [open, setOpen] = useState(null), [error, setError] = useState(null);
			const back = async (s, version) => { setError(null); try { await act("skillSettle", { projectId: s.projectId, memberId: s.memberId, version }); } catch (e) { setError(e.message); } };
			if (!skills.length) return h("div", { className: "mp4-setup" }, h("p", { className: "mp4-muted" }, "还没有执行者。项目里开过任务后，它的做事说明会出现在这里。"));
			return h("div", { className: "mp4-setup mp4-skills" },
				h("div", { className: "mp4-skill-head" }, h("span", null, "执行者"), h("span", null, "用了"), h("span", null, "返工"), h("span", null, "版本")),
				skills.map(s => h(React.Fragment, { key: s.key },
					h("button", { type: "button", className: "mp4-skill-row", "aria-expanded": open === s.key, onClick: () => setOpen(open === s.key ? null : s.key) },
						h("span", { className: "mp4-ell" }, `「${s.projectName}」${s.name}`), h("span", null, s.uses), h("span", null, s.rework),
						h("span", { className: s.proposal ? "mp4-skill-flag" : undefined }, s.proposal ? `v${s.version + 1} 待你看` : s.trial ? `v${s.version} 试用中` : `v${s.version}`)),
					open === s.key ? h("div", { className: "mp4-skill-open" },
						s.proposal ? h(SkillProposal, { skill: s }) : null,
						h("p", { className: "mp4-skill-k" }, s.waiting >= 3 || s.proposal ? "现在的说明" : `现在的说明（交付后再有 ${Math.max(0, 3 - s.waiting)} 句反馈，夜里会考虑改它）`),
						h("p", { className: "mp4-skill-now" }, s.versions.at(-1).persona || "（空）"),
						s.versions.length > 1 ? h("details", { className: "mp4-fold" }, h("summary", null, `全部 ${s.versions.length} 版`),
							[...s.versions].reverse().map(v => h("div", { key: v.version, className: "mp4-skill-ver" },
								h("span", null, `v${v.version}${v.at ? `　${dayLabel(v.at)} ${clock(v.at)}` : ""}${v.why ? `　${v.why}` : ""}`),
								v.version === s.version ? h("span", { className: "mp4-skill-k" }, "现在") : h("button", { type: "button", className: "mp4-link-btn", onClick: () => back(s, v.version) }, "退回到这一版")))) : null) : null)),
				error ? h("p", { className: "mp4-err" }, error) : null);
		}

		/** Mira's overviews: what waits for the owner, today's timeline, projects → tasks. Shared by her page and her Session's right panel. */
		function MiraOverview({ view }) {
			const today = useToday(), nav = useSnapshot(), waiting = useWaiting();
			const openTaskBy = (projectId, threadId) => {
				const row = nav.projects.find(p => p.id === projectId)?.threads?.find(t => t.id === threadId);
				if (row) openTask(projectId, row); else navigateProject(projectId, threadId);
			};
			// Which harness works on a task: a small badge on the agent's face when it is Claude Code or Codex.
			if (view === "inbox") return h(MiraInbox, { today, waiting, nav, openTaskBy });
			if (view === "time") return h(MiraHistory, { today, waiting, openTaskBy });
			if (view === "tree") return h(ProjectTree, { nav });
			return null;
		}

		// ---------------------------------------------------------------- Mira's own DSH Session

		/**
		 * Open Mira the DSH way: her conversation is her own Session (made on first open), with her overviews on the right.
		 * Falls back to her plugin page.
		 */
		let openingMira = false;
		async function openMira(options) {
			if (openingMira) return;
			openingMira = true;
			try {
				if (shell.workspace) {
					const { sessionId } = await act("openMiraChat", {});
					shell.workspace.openSession(sessionId);
					if (!(options && options.keep)) revealTabs(sessionId, [MIRA_TABS[0].kind]);
					miraFellBackAt = 0;
					// A stale fallback address would bring the page back on reload: drop it (a source link keeps it).
					if (isHomeRoute() && !window.location.hash.includes("source=")) window.history.replaceState(window.history.state, "", window.location.pathname + window.location.search);
					return sessionId;
				}
			} catch (_e) { miraFellBackAt = Date.now(); /* fall through to her page */ } finally { openingMira = false; }
			navigateHome();
		}
		/**
		 * When opening her Session last failed (the server restarting, say). Her plugin page is only a fallback: it does not
		 * stream replies. A visit to it that is not a source link goes back to her Session, unless that just failed.
		 */
		let miraFellBackAt = 0;
		const homeIsFallbackOnly = () => isHomeRoute() && !window.location.hash.includes("source=") && Boolean(shell.workspace) && Date.now() - miraFellBackAt > 15e3;

		/** A right-panel tab of Mira's Session; other sessions say so. */
		function miraTab(render, look = "") {
			const pane = "mp4-tabpane" + (look ? " " + look : "");
			return function MiraTabBody({ sessionId }) {
				const snap = usePersonal();
				if (!snap.loaded) return h("div", { className: pane }, h(Loading, null));
				if (snap.sessionId !== sessionId) return h("div", { className: pane }, h("p", { className: "mp4-muted" }, "这个会话不是个人助理的对话。"));
				return h("div", { className: pane }, render(snap));
			};
		}

		const ATTENTION_MODES = { off: "关闭", observe: "只记录", auto: "自动提醒" };
		const ATTENTION_HELP = {
			off: "不看飞书里谁在等你。",
			observe: "只在后台记下「这条可能要你回」，不打扰你，也不叫醒个人助理。",
			auto: "真正在等你的，会叫醒个人助理，由它递卡或提醒你。",
		};
		/** Whether Feishu messages that wait for the owner may wake the assistant: off, record only, or wake it. */
		function AttentionSwitch({ attention }) {
			const [picked, setPicked] = useState(null), [busy, setBusy] = useState(false), [error, setError] = useState(null);
			// The feed catches up within seconds; until it does, show what was just picked.
			useEffect(() => { setPicked(null); }, [attention.mode]);
			const mode = picked || attention.mode;
			const pick = async next => {
				if (next === mode || busy) return;
				setBusy(true); setError(null);
				try { await act("attentionMode", { mode: next }); setPicked(next); }
				catch (e) { setError(String(e?.message || e)); }
				finally { setBusy(false); }
			};
			return h("div", { className: "mp4-setbox" },
				h("div", { className: "mp4-follow-acts", role: "radiogroup", "aria-label": "飞书主动提醒" },
					Object.keys(ATTENTION_MODES).map(id => h("button", { key: id, type: "button", role: "radio", "aria-checked": mode === id, className: "mp4-sug" + (mode === id ? " mp4-sug-on" : ""), disabled: busy, onClick: () => pick(id) }, ATTENTION_MODES[id]))),
				h("p", { className: "mp4-muted" }, ATTENTION_HELP[mode] || ""),
				h("p", { className: "mp4-muted" }, `今天已叫醒个人助理 ${attention.wokeToday || 0} 次（每天最多 ${attention.dailyWakes} 次）${attention.observed ? `；只记录时记下过 ${attention.observed} 条` : ""}。`),
				error ? h("p", { className: "mp4-err" }, error) : null);
		}

		const FEISHU_LIVE_STEPS = {
			no_app: { title: "第 1 步，共 2 步：建一个飞书应用", start: "开始连接", note: "飞书会请你确认新建一个应用，给这台机器上的飞书命令行用；有的公司需要管理员批准。" },
			no_login: { title: "最后一步：用你的飞书账号授权", start: "去授权", note: "只申请读消息、读文档的权限。登录信息保存在 DSH 这台机器上。" },
		};
		/** Connect live Feishu reading: the owner does both browser steps themself; this only hands over the link and checks. */
		function FeishuLiveSetup() {
			const [status, setStatus] = useState(null), [busy, setBusy] = useState(false), [error, setError] = useState(null);
			const call = async op => {
				setBusy(true); setError(null);
				try { setStatus(await act(op, {})); }
				catch (e) { setError(String(e?.message || e)); }
				finally { setBusy(false); }
			};
			useEffect(() => { call("feishuLiveStatus"); }, []);
			const step = status ? FEISHU_LIVE_STEPS[status.state] : null;
			const link = step && /^https:\/\//.test(status.url || "") ? status.url : null;
			return h("div", { className: "mp4-setbox" },
				!status && !error ? h("p", { className: "mp4-muted" }, "正在检查…") : null,
				status?.state === "not_installed" ? h("p", { className: "mp4-muted" }, "这台机器上没有装飞书命令行，装好后才能连接。") : null,
				status?.state === "ready" ? h("p", null, "已连上，只读。个人助理可以当场搜飞书消息、读会话和文档。") : null,
				step ? h(React.Fragment, null,
					h("p", null, h("b", null, step.title)),
					h("p", { className: "mp4-muted" }, step.note),
					link ? h(React.Fragment, null,
						h("p", { className: "mp4-muted" }, "在浏览器里打开这个链接，按页面提示做完，再回来点「我做完了」："),
						h("a", { href: link, target: "_blank", rel: "noopener noreferrer" }, link),
						h("p", { className: "mp4-muted" }, "链接大约 10 分钟内有效。过期了点「我做完了，检查一下」，页面会让你重新拿一个。"),
						h("div", { className: "mp4-follow-acts" }, h("button", { type: "button", className: "mp4-sug", disabled: busy, onClick: () => call("feishuLiveStatus") }, busy ? "正在检查…" : "我做完了，检查一下")))
						: h("div", { className: "mp4-follow-acts" }, h("button", { type: "button", className: "mp4-sug", disabled: busy, onClick: () => call("feishuLiveConnect") }, busy ? "正在拿链接…" : step.start))) : null,
				error ? h("p", { className: "mp4-err" }, error) : null);
		}

		const MAC_FILE_OP_LABELS = { roots: "看有哪些文件夹", list: "列目录", search: "搜索", read: "读文件" };
		/** Reading the owner's own computer: whether the reader there is connected, which folders it offers, what was read lately, and a pause. */
		function MacFilesSetup() {
			const [status, setStatus] = useState(null), [busy, setBusy] = useState(false), [error, setError] = useState(null);
			const call = async (op, body = {}) => {
				setBusy(true); setError(null);
				try { setStatus(await act(op, body)); }
				catch (e) { setError(String(e?.message || e)); }
				finally { setBusy(false); }
			};
			useEffect(() => { call("macFilesStatus"); }, []);
			const mode = status?.mode || "on", folders = (status?.roots || []).filter(r => !r.whole);
			// Three modes, within what the computer offers: 全部文件 only when the computer itself opened the home folder.
			const MODES = [["off", "暂停"], ["on", "指定文件夹"], ["all", "全部文件"]];
			const reach = mode === "off" ? "现在读不了你的电脑" : mode === "all" ? (status?.whole ? "个人目录里的全部文件，不含资源库（Library）和隐藏文件夹" : "电脑上还没开放全部文件，现在只读下面的文件夹") : "只读下面这几个文件夹";
			return h("div", { className: "mp4-setgroup" },
				!status && !error ? h("p", { className: "mp4-muted" }, "正在检查…") : null,
				status ? h("div", { className: "mp4-setlist" },
					h("div", { className: "mp4-setline" }, h("span", { className: "mp4-setline-k" }, "连接", h("small", null, status.connected ? "只读；电脑主动连过来，这边连不进你的电脑" : "电脑要开着、没睡眠，读取器在运行")), h("span", { className: "mp4-setline-v" }, status.connected ? `已连上「${status.device}」` : "没连上")),
					h("div", { className: "mp4-setline" }, h("span", { className: "mp4-setline-k" }, "能读什么", h("small", null, reach)),
						h("span", { className: "mp4-seg", role: "radiogroup", "aria-label": "读取范围" }, MODES.map(([id, label]) => h("button", { key: id, type: "button", role: "radio", "aria-checked": mode === id, className: mode === id ? "mp4-seg-on" : "", disabled: busy || (id === "all" && !status.whole && mode !== "all"), title: id === "all" && !status.whole ? "要先在电脑上开放全部文件" : undefined, onClick: () => mode === id || call("macFilesMode", { mode: id }) }, label)))),
					folders.length ? h("div", { className: "mp4-setline" }, h("span", { className: "mp4-setline-k mp4-setline-plain" }, "放行的文件夹"), h("span", { className: "mp4-setline-v" }, folders.map(r => r.label).join("、"))) : null) : null,
				status ? h("p", { className: "mp4-setgroup-desc" }, "读回来的内容会留在个人助理的对话记录里。") : null,
				status?.recent?.length ? h(React.Fragment, null, h("h4", null, "最近读过"),
					h("div", { className: "mp4-setlist" }, status.recent.slice(0, 8).map((r, i) => h("div", { key: i, className: "mp4-setline" }, h("span", { className: "mp4-setline-k mp4-setline-plain mp4-ell" }, `${MAC_FILE_OP_LABELS[r.op] || r.op}${r.query ? `「${r.query}」` : ""}${r.path ? ` ${r.path}` : ""}${r.ok ? "" : "（没读成）"}`), h("span", { className: "mp4-setline-v" }, clock(r.at)))))) : null,
				status ? h("button", { type: "button", className: "mp4-link-btn", style: { alignSelf: "flex-start" }, disabled: busy, onClick: () => call("macFilesStatus") }, busy ? "正在检查…" : "刷新") : null,
				error ? h("p", { className: "mp4-err" }, error) : null);
		}

		const retentionDays = days => days ? `保留 ${days} 天` : "长期保留";
		const retentionLine = policy => !policy ? "" : policy.rawDays === policy.summaryDays ? `原文和活动摘要${retentionDays(policy.rawDays)}` : `原文${retentionDays(policy.rawDays)}，活动摘要${retentionDays(policy.summaryDays)}`;
		/** Mira's settings and sources, reachable from her Session: device activity, mail and Feishu, retention, today's board. */
		function MiraMore({ snap }) {
			const [dialog, setDialog] = useState(null), [sourceRef, setSourceRef] = useState(null), today = useToday(), attention = today.attention, skills = (today.dream && today.dream.skills) || [];
			const openSource = ref => { setDialog(null); setSourceRef(ref); };
			const row = (label, desc, onClick, icon = "chevron") => h("button", { key: label, type: "button", className: "mp4-setrow", onClick },
				h("span", { className: "mp4-setrow-text" }, h("span", { className: "mp4-setrow-title" }, label), desc ? h("span", { className: "mp4-setrow-desc" }, desc) : null), h(Icon, { name: icon, size: 14 }));
			const openTab = kind => { try { shell.sidebar?.openTab(kind, { revealIfOpened: true }); } catch (_e) { /* the panel is not ready */ } };
			return h(React.Fragment, null,
				sourceRef ? null : h("div", { className: "mp4-page-head" }, h("h3", { className: "mp4-tab-title" }, "助理设置")),
				sourceRef ? h(React.Fragment, null, h(SourcePanel, { key: sourceRef, sourceRef }), h("button", { type: "button", className: "mp4-link-btn", onClick: () => setSourceRef(null) }, "‹ 返回")) : h(React.Fragment, null, h("div", { className: "mp4-setlist" },
					row("形象与快捷键", snap.look ? `${snap.look.name}，${hotkeyLabel(snap.look.hotkey)}${snap.look.hotkey ? " 呼出" : ""}` : "捏一个你的助理", () => openStudio()),
					Array.isArray(snap.observationDevices) ? row("活动与设备", "电脑端采集的活动和设备", () => setDialog("observations")) : null,
					row("邮件与飞书来源", "个人助理能读的外部资料", () => setDialog("connectors")),
					row("飞书实时读取", "读取飞书消息和文档，只读", () => setDialog(dialog === "feishuLive" ? null : "feishuLive")),
					dialog === "feishuLive" ? h("div", { className: "mp4-setup" }, h(FeishuLiveSetup, null), h(AskMira, { text: "帮我设置飞书实时读取：" })) : null,
					row("电脑文件", "暂停、指定文件夹或全部文件，只读", () => setDialog(dialog === "macFiles" ? null : "macFiles")),
					dialog === "macFiles" ? h("div", { className: "mp4-setup" }, h(MacFilesSetup, null), h(AskMira, { text: "帮我管理个人助理能读的电脑文件夹：" })) : null,
					attention ? row("飞书主动提醒", ATTENTION_MODES[attention.mode] || attention.mode, () => setDialog(dialog === "attention" ? null : "attention")) : null,
					dialog === "attention" && attention ? h("div", { className: "mp4-setup" }, h(AttentionSwitch, { attention }), h(AskMira, { text: "把飞书主动提醒改成：" })) : null,
					snap.retention ? row("资料保留", retentionLine(snap.retention.policy), () => setDialog("retention")) : null,
					row("今日看板", "", () => window.open("api/mira-projects/today/view", "_blank", "noopener"), "popout"),
					), h("p", { className: "mp4-k mp4-adv-k" }, "高级"), h("div", { className: "mp4-setlist" },
					row("记忆", "关于你和工作的记录", () => openTab("mira-projects-mira-memory")),
					row("助理与模型", "每个助理什么时候醒、读到什么、用什么模型", () => openTab("mira-projects-mira-agents")),
					row("我的技能", skills.some(s => s.proposal) ? `${skills.filter(s => s.proposal).length} 份做事说明的修改等你看` : `${skills.length} 个执行者的做事说明，会从你的反馈里改进`, () => setDialog(dialog === "skills" ? null : "skills")),
					dialog === "skills" ? h(SkillsPanel, { skills }) : null,
					row("开发者视图", "查看各环节的输入与输出", () => window.open("api/mira-projects/workers/view", "_blank", "noopener"), "popout"))),
				dialog === "retention" && snap.retention ? h(RetentionDialog, { data: snap.retention, onClose: () => setDialog(null) }) : null,
				dialog === "connectors" ? h(ConnectorsWorkspace, { data: snap.connectors, onClose: () => setDialog(null), onSource: openSource }) : null,
				dialog === "observations" ? h(ObservationWorkspace, { revision: snap.rev, retentionVersion: `${snap.retention?.policy?.revision}:${snap.retention?.expired}:${snap.retention?.cleaned}`, now: snap.retention?.now, onClose: () => setDialog(null), onSource: openSource }) : null);
		}

		/** Which of the plugin's levels a DSH Session is: Mira's own, a project's conversation, a task, or none (null). */
		function useSessionLevel(sessionId) {
			const personal = usePersonal(), all = useSnapshot(), root = useRootSession(sessionId);
			if (!sessionId) return null;
			if (personal.sessionId === sessionId) return "mira";
			if ((all.projects || []).some(p => p.chatSessionId === sessionId)) return "project";
			// A task's sub-agents keep the task's cards.
			if (allTasks(all).some(x => ownsSession(x.t, root))) return "task";
			return null;
		}
		/**
		 * A plugin tab's card on DSH's 开始 page, drawn like DSH's own card, in Sessions of its level only: Mira's panels
		 * in Mira, a project's in its conversation, a task's in the task. Elsewhere it leaves a marker that hides its cell.
		 * A quiet tab (level "quiet": memory, agents and models) never takes a card; it is opened from the settings page.
		 */
		const GUIDE_ICONS = { "mira-projects-mira-inbox": "inbox", "mira-projects-mira-time": "clock", "mira-projects-mira-tree": "tree", "mira-projects-mira-memory": "note",
			"mira-projects-mira-agents": "agents", "mira-projects-mira-more": "sliders", "mira-projects-task": "tasks", "mira-projects-changes": "diff", "mira-projects-agents": "agents" };
		function guideEntry(level) {
			return function MiraGuideEntry({ sessionId, kind, title, description, useTabInfo }) {
				const { tab } = useTabInfo();
				if (useSessionLevel(sessionId) !== level) return h("i", { "data-mira-guide-skip": "", hidden: true });
				return h("button", { type: "button", className: "mp4-guide-entry", "data-sidebar-right-guide-entry": kind, onClick: () => tab.actions.openTab(kind, { replaceTab: true }) },
					h("span", { className: "mp4-guide-entry-icon" }, h(Icon, { name: GUIDE_ICONS[kind] || "panel", size: 18 })),
					h("span", { className: "mp4-guide-entry-text" }, h("span", { className: "mp4-guide-entry-title" }, title)));
			};
		}

		const HUB_MEMORY_LABELS = { focus: "今天的重心", links: "跨项目关联", stalled: "待跟进", rhythm: "节奏" };

		/** 记忆: what the personal assistant keeps about the owner (their own words) and about the work (her notes). Every line can be changed or forgotten. */
		function MiraMemory() {
			const today = useToday();
			const [draft, setDraft] = useState(""), [editing, setEditing] = useState(null), [busy, setBusy] = useState(false), [error, setError] = useState(null);
			const call = async (op, body) => {
				setBusy(true); setError(null);
				try { await act(op, body); return true; }
				catch (e) { setError(String(e?.message || e)); return false; }
				finally { setBusy(false); }
			};
			if (!today.loaded) return h(Loading, null);
			const about = today.about || [], memory = today.memory || {}, gone = today.forgotten || { about: [], memory: [] };
			const work = Object.keys(HUB_MEMORY_LABELS).map(section => ({ section, items: memory[section] || [] })).filter(group => group.items.length);
			const workCount = work.reduce((n, group) => n + group.items.length, 0), goneCount = gone.about.length + gone.memory.length;
			const add = async (event) => { event.preventDefault(); if (draft.trim() && await call("aboutAdd", { text: draft })) setDraft(""); };
			const save = async (event) => { event.preventDefault(); if (await call("aboutEdit", { itemId: editing.id, text: editing.text })) setEditing(null); };
			const source = item => item.source?.kind === "said" ? `你在对话里说的，${dayLabel(item.at)}` : item.source?.kind === "auto" ? `从你在对话里打的字里自动记下的，${dayLabel(item.at)}` : `你在这里写的，${dayLabel(item.at)}`;
			const goneWhy = { replaced: "被新的一条替换", merged: "整理时合并掉的", outdated: "整理时判为过时", undone: "你撤销的" };
			// Laid out like Codex's settings: a heading per group, one bordered card, a row per memory; actions show on hover.
			const acts = list => h("span", { className: "mp4-memrow-acts" }, list.map(([label, onClick]) => h("button", { key: label, type: "button", className: "mp4-mem-link", disabled: busy, onClick }, label)));
			const row = (key, text, meta, actions, title) => h("div", { key, className: "mp4-memrow", title },
				h("div", { className: "mp4-memrow-body" }, h("p", { className: "mp4-mem-text" }, text), meta ? h("p", { className: "mp4-memrow-meta" }, meta) : null), actions);
			return h(React.Fragment, null,
				h("div", { className: "mp4-page-head" }, h("h3", { className: "mp4-tab-title" }, "记忆")),
				h("div", { className: "mp4-setgroup" },
					h("h4", null, "关于你", h("span", { className: "mp4-setgroup-n" }, about.length)),
					h("p", { className: "mp4-setgroup-desc" }, "只记你说过的话，每条带原话，可以撤销。"),
					h("div", { className: "mp4-setlist" },
						h("form", { className: "mp4-mem-add mp4-memrow", onSubmit: add },
							h("input", { className: "mp4-input", value: draft, maxLength: 200, disabled: busy, onChange: e => setDraft(e.target.value), placeholder: "写一条关于你自己的事，比如：我下午不排会", "aria-label": "写一条关于你自己的事" }),
							h("button", { type: "submit", className: "mp4-btn", disabled: busy || !draft.trim() }, "记下")),
						about.map(item => editing?.id === item.id
							? h("form", { key: item.id, className: "mp4-mem-add mp4-memrow", onSubmit: save },
								h("input", { className: "mp4-input", value: editing.text, maxLength: 200, autoFocus: true, disabled: busy, onChange: e => setEditing({ id: item.id, text: e.target.value }), "aria-label": "改这一条" }),
								h("button", { type: "submit", className: "mp4-btn", disabled: busy || !editing.text.trim() }, "保存"),
								h("button", { type: "button", className: "mp4-btn", disabled: busy, onClick: () => setEditing(null) }, "取消"))
							: row(item.id, item.text, item.source?.quote ? `「${item.source.quote}」` : source(item),
								acts([["改", () => setEditing({ id: item.id, text: item.text })], ["忘掉", () => call("aboutForget", { itemId: item.id })]]),
								source(item) + (item.history?.length ? `，改过，原来是「${item.history[item.history.length - 1].text}」` : ""))))),
				h("div", { className: "mp4-setgroup" },
					h("h4", null, "工作", h("span", { className: "mp4-setgroup-n" }, workCount), workCount && memory.updatedAt ? h("span", { className: "mp4-setgroup-when" }, `更新于 ${ago(memory.updatedAt)}`) : null),
					workCount ? null : h("p", { className: "mp4-setgroup-desc" }, "还没有"),
					work.map(group => h(React.Fragment, { key: group.section },
						h("p", { className: "mp4-mem-sec" }, HUB_MEMORY_LABELS[group.section]),
						h("div", { className: "mp4-setlist" }, group.items.map((item, i) => row(group.section + i, item.text, null,
							acts([["忘掉", () => call("hubMemoryForget", { section: group.section, text: item.text })]]),
							item.refs?.length ? `依据 ${item.refs.length} 条` : "没有依据")))))),
				goneCount ? h("details", { className: "mp4-fold" }, h("summary", null, `已忘掉的 ${goneCount}`),
					h("div", { className: "mp4-setlist" },
						gone.about.map(item => row(item.id, item.text, goneWhy[item.forgottenWhy] ? `关于你，${goneWhy[item.forgottenWhy]}` : "关于你", acts([["恢复", () => call("aboutRestore", { itemId: item.id })]]))),
						gone.memory.map((item, i) => row("m" + i, item.text, `工作，${HUB_MEMORY_LABELS[item.section] || item.section}`, acts([["恢复", () => call("hubMemoryRestore", { section: item.section, text: item.text })]]))))) : null,
				error ? h("p", { className: "mp4-err", role: "alert" }, error) : null);
		}

		const MIRA_TABS = [
			{ id: "mira-projects.mira-inbox", kind: "mira-projects-mira-inbox", title: "待你决定", order: 3, desc: "跨项目的拍板、验收和失败", body: miraTab(() => h(MiraOverview, { view: "inbox" }), "mp4-core") },
			{ id: "mira-projects.mira-time", kind: "mira-projects-mira-time", title: "时间线", order: 4, desc: "今天按时间排的核心活动", body: miraTab(() => h(MiraOverview, { view: "time" }), "mp4-core") },
			{ id: "mira-projects.mira-tree", kind: "mira-projects-mira-tree", title: "项目与任务", order: 5, desc: "所有项目和它们的任务", body: miraTab(() => h(MiraOverview, { view: "tree" }), "mp4-core") },
			{ id: "mira-projects.mira-memory", kind: "mira-projects-mira-memory", title: "记忆", order: 6, desc: "个人助理记着的：关于你、工作", quiet: true, body: miraTab(() => h(MiraMemory)) },
			{ id: "mira-projects.mira-agents", kind: "mira-projects-mira-agents", title: "助理与模型", order: 12, desc: "每个助理什么时候醒、读到什么、用什么模型", quiet: true, body: miraTab(snap => h(AgentModels, { revision: snap.rev })) },
			{ id: "mira-projects.mira-more", kind: "mira-projects-mira-more", title: "助理设置", order: 13, desc: "活动与设备、邮件与飞书、资料保留、今日看板；记忆和模型收在「高级」里", body: miraTab(snap => h(MiraMore, { snap })) },
		];

		/** Above Mira's composer: suggested next lines; a pick is sent as the owner's own message in her Session. */
		function MiraSuggestions({ sessionId }) {
			const snap = usePersonal(), waiting = useWaiting();
			const [error, setError] = useState(null);
			if (!snap.loaded || snap.sessionId !== sessionId) return null;
			const said = new Set((snap.messages || []).filter(m => m.author.kind === "human").map(m => m.text.trim()));
			const suggestions = suggestionsFor(waiting, said);
			const send = async text => {
				setError(null);
				try { await act("buttonSent", { text }); await shell.sessions.using(sessionId, { source: "sidebarView" }, async ref => { await ref.ready; const r = await ref.binding.session.prompt([{ type: "text", text }], "queue"); if (r && r.ok === false) throw new Error(r.error?.message || "没发出去"); }); }
				catch (e) { setError(String(e?.message || e)); }
			};
			// E2E #3: an empty conversation shows what can be said here as starters. They only fill the composer; the owner finishes the sentence.
			const starters = said.size ? [] : ["记住：", "新建一个项目，"];
			if (!suggestions.length && !starters.length) return null;
			return h("div", { className: "mp4-sugs mp4-dock-sugs" },
				suggestions.map(s => h("button", { key: s, type: "button", className: "mp4-sug", title: s === "今天做了什么" ? undefined : "填进输入框，由你发送",
					// Looking is a click; answering a decision or accepting a task is a sentence the owner sends themselves.
					onClick: e => { if (s === "今天做了什么") send(s); else if (!draftInto(e.currentTarget, s)) setError("没找到输入框"); } }, s)),
				starters.map(s => h("button", { key: s, type: "button", className: "mp4-sug", title: "填进输入框", onClick: e => { if (!draftInto(e.currentTarget, s)) setError("没找到输入框"); } }, s.replace(/[：，]$/, "…"))),
				error ? h("span", { className: "mp-error" }, error) : null);
		}

		function PersonalHome() {
			const snap = usePersonal(), waiting = useWaiting();
			const [older, setOlder] = useState({ messages: [], hasMore: undefined }), [loadingOlder, setLoadingOlder] = useState(false);
			const [retrying, setRetrying] = useState(false), [retryError, setRetryError] = useState(null);
			const [dialog, setDialog] = useState(null), [sourceRef, setSourceRef] = useState(() => new URLSearchParams(window.location.hash.split("?")[1] || "").get("source"));
			const [panelOpen, setPanelOpen] = useState(panelFits), [wide, setWide] = useState(false);
			const tabs = usePanelTabs("home", ["inbox", "time"]);
			useEffect(() => { if (sourceRef) tabs.open("source"); }, [sourceRef]);
			const messages = [...new Map([...older.messages, ...(snap.messages || [])].map(m => [m.id, m])).values()].sort((a, b) => a.seq - b.seq);
			const reading = useReadingPosition(true, "personal", (messages.at(-1)?.seq || 0) + (snap.actions || []).length);
			const send = (text, typed) => personalCommand("personalPost", typed ? { text, typed: text } : { text }, "personal-post");
			const openSource = ref => { setSourceRef(ref); tabs.open("source"); setPanelOpen(true); };
			const retry = async () => { setRetrying(true); setRetryError(null); try { await personalCommand("personalRetry", { messageId: snap.retryMessageId }, "personal-retry:" + snap.retryMessageId); } catch (e) { setRetryError(e.message); } finally { setRetrying(false); } };
			const loadOlder = async () => {
				setLoadingOlder(true);
				try { const r = await fetch("api/mira-projects/personal?before=" + messages[0].seq, { credentials: "same-origin", cache: "no-store" }); const d = await r.json(); if (!r.ok) throw Error(d.error); setOlder(old => ({ messages: [...d.messages, ...old.messages], hasMore: d.hasMore })); } catch (_e) { /* the button stays */ } finally { setLoadingOlder(false); }
			};
			const said = new Set(messages.filter(m => m.author.kind === "human").map(m => m.text.trim()));
			const suggestions = suggestionsFor(waiting, said);
			// Mira's actions sit in the conversation where they happened.
			const rows = [...messages.map(m => ({ kind: "message", seq: m.seq, m })), ...(snap.actions || []).map(a => ({ kind: "action", seq: a.seq, a }))].sort((a, b) => a.seq - b.seq);
			const info = { inbox: { label: "待你决定", icon: "inbox" }, time: { label: "时间线", icon: "clock" }, tree: { label: "项目与任务", icon: "tree" }, source: { label: "来源", icon: "quote" }, agents: { label: "模型", icon: "agents" } };
			const guides = [
				{ id: "inbox", icon: "inbox", label: "待你决定" },
				{ id: "time", icon: "clock", label: "时间线" },
				{ id: "tree", icon: "tree", label: "项目与任务" },
			];
			const panelBody = ["inbox", "time", "tree"].includes(tabs.active) ? h(MiraOverview, { view: tabs.active })
				: tabs.active === "source" ? (sourceRef ? h(SourcePanel, { key: sourceRef, sourceRef }) : h("p", { className: "mp4-muted" }, "点回答里的「来源」在这里看原文。"))
				: tabs.active === "agents" ? h(AgentModels, { revision: snap.rev })
				: null;
			const latestOwner = messages.findLast(m => m.author.kind === "human");
			const menu = [
				Array.isArray(snap.observationDevices) ? ["活动与设备", () => setDialog("observations")] : null,
				["模型", () => { tabs.open("agents"); setPanelOpen(true); }],
				["邮件与飞书来源", () => setDialog("connectors")],
				snap.retention ? ["资料保留", () => setDialog("retention")] : null,
				latestOwner && (snap.projects || []).length ? ["把上一句交给项目…", () => setDialog({ handoff: { messageId: latestOwner.id, text: latestOwner.text, refs: [] } })] : null,
				["今日看板", () => window.open("api/mira-projects/today/view", "_blank", "noopener")],
				["开发者视图", () => window.open("api/mira-projects/workers/view", "_blank", "noopener")],
			];
			if (snap.loaded && snap.enabled === false) return h("div", { className: "mp-page mp-center" }, "当前环境未开启个人助理");
			return h(React.Fragment, null, h(SessionFrame, {
				title: "个人助理", where: "全局", menu, scroll: reading, panelOpen, onShowPanel: () => setPanelOpen(true),
				body: h(React.Fragment, null,
					shell.workspace ? h("div", { className: "mp4-fallback", role: "status" }, "这是备用页面，回复要写完才显示。", h("button", { type: "button", className: "mp4-link-btn", onClick: () => openMira() }, "回到正常对话")) : null,
					(older.hasMore ?? snap.hasMore) ? h("button", { type: "button", className: "mp4-link-btn", disabled: loadingOlder, onClick: loadOlder }, loadingOlder ? "正在读取…" : "加载更早对话") : null,
					messages.length === 0 && snap.loaded ? h("div", { className: "mp4-empty" }, h("b", null, "从这里理清今天的事"), h("p", null, "问进展、问等你的事，或者直接交代一件事给某个项目。")) : null,
					rows.map(r => r.kind === "action"
						? h("div", { key: "a" + r.seq, className: "mp4-act" }, h("span", { className: "mp4-act-mark" }, "✓"), h("span", null, parts(h("b", null, "已执行"), r.a.text), r.a.sub ? h("small", null, r.a.sub) : null))
						: r.m.author.kind === "human"
							? h("div", { key: r.m.id, className: "mp4-user" }, h("div", { className: "mp4-bubble" }, r.m.text), h("span", { className: "mp4-mono mp4-muted" }, clock(r.m.at)))
							: h("div", { key: r.m.id, className: "mp4-assistant" }, h(RichText, { text: r.m.text, members: [] }),
								(r.m.refs || []).length ? h("div", { className: "mp4-srcs" }, r.m.refs.map((ref, i) => h("button", { key: ref, type: "button", className: "mp4-src", onClick: () => openSource(ref) }, `来源 ${i + 1}`))) : null,
								(snap.handoffs || []).filter(x => x.sourceMessageId === r.m.replyTo).map(x => h("button", { key: x.id, type: "button", className: "mp4-link-btn", onClick: () => navigateProject(x.projectId) }, `已交给「${snap.projects?.find(p => p.id === x.projectId)?.name || "项目"}」› `)))),
					["running", "queued"].includes(snap.status) ? h("div", { className: "mp4-live", role: "status" }, h("span", { className: "mp-spin", "aria-hidden": true }), snap.status === "queued" ? "消息已排队" : "助理正在处理…") : null,
					snap.retryMessageId ? h("div", { className: "mp4-live", role: "status" }, "上次没能回复，原消息还在。", h("button", { type: "button", className: "mp4-link-btn", disabled: retrying, onClick: retry }, retrying ? "正在重试…" : "重试"), retryError ? h("span", { className: "mp-error" }, retryError) : null) : null),
				composer: h(React.Fragment, null,
					suggestions.length ? h("div", { className: "mp4-sugs" }, suggestions.map(s => h("button", { key: s, type: "button", className: "mp4-sug", onClick: () => { if (s === "今天做了什么") { tabs.open("time"); setPanelOpen(true); } send(s); } }, s))) : null,
					h(Composer, { members: [{ kind: "human", id: "owner", name: "我" }, { kind: "assistant", id: "mira", name: "个人助理", role: "memory" }], project: { members: [] }, placeholder: "发消息或交代一件事", draftKey: "personal:chat", onSend: text => send(text, true) })),
				panel: h(SidePanel, { tabs, info, guides, wide, onWide: () => setWide(v => !v), onHide: () => setPanelOpen(false) }, panelBody),
			}),
				dialog === "retention" && snap.retention ? h(RetentionDialog, { data: snap.retention, onClose: () => setDialog(null) }) : null,
				dialog === "connectors" ? h(ConnectorsWorkspace, { data: snap.connectors, onClose: () => setDialog(null), onSource: ref => { setDialog(null); openSource(ref); } }) : null,
				dialog === "observations" ? h(ObservationWorkspace, { revision: snap.rev, retentionVersion: `${snap.retention?.policy?.revision}:${snap.retention?.expired}:${snap.retention?.cleaned}`, now: snap.retention?.now, onClose: () => setDialog(null), onSource: ref => { setDialog(null); openSource(ref); } }) : null,
				dialog?.handoff ? h(HandoffDialog, { request: dialog.handoff, projects: snap.projects || [], onClose: () => setDialog(null) }) : null);
		}

		// ---------------------------------------------------------------- inside a task's DSH Session

		// A task's sub-agents are part of the task: its tabs describe the task there too (useRootSession finds it).

		/** Sub-agent Sessions whose right panel was looked at in this page load. */
		const panelKept = new Set();
		/**
		 * A sub-agent opened from DSH's own places starts with an empty right panel. Give it its task's card and the list
		 * of sibling sub-agents once; a panel that already holds them is the owner's own layout and stays as it is.
		 */
		function keepTaskPanel(sessionId) {
			if (panelKept.has(sessionId)) return;
			panelKept.add(sessionId);
			const open = shell.sidebar?.openTabs;
			const tabs = open && typeof open.getSnapshot === "function" ? open.getSnapshot() : [];
			if (tabs.some(t => t.sessionId === sessionId && (t.kind === TASK_TAB_KIND || t.kind === AGENTS_TAB_KIND))) return;
			revealTabs(sessionId, [TASK_TAB_KIND, AGENTS_TAB_KIND]);
		}

		/** What the "one level up" key is called on this machine. */
		const UP_KEYS = /Mac|iPhone|iPad/.test(globalThis.navigator?.platform || globalThis.navigator?.userAgent || "") ? "⌘↑" : "Ctrl+↑";
		/** The text typed so far into the field the keyboard is in; empty outside a field. */
		function typedText(target) {
			const field = target && typeof target.closest === "function" ? target.closest("input, textarea, [contenteditable]") : null;
			return field ? String(field.value ?? field.textContent ?? "").trim() : "";
		}
		/**
		 * One level up from the Session on screen: a sub-agent's parent conversation, a task's project, a project's
		 * assistant. Null at the top, and in Sessions that are not the plugin's.
		 */
		function upMove() {
			const read = store => (store && typeof store.getSnapshot === "function" ? store.getSnapshot() : undefined);
			const id = read(shell.sidebar?.mounted);
			if (!id || !shell.workspace) return null;
			const me = read(shell.sessions?.list)?.byId?.[id];
			if (me && me.origin === "subagent" && me.parentId) return () => shell.workspace.openSession(me.parentId);
			for (const p of sharedFeed(STATE_URL + "?" + new URLSearchParams({ project: "" }), EMPTY_STATE).getSnapshot().projects || []) {
				if ((p.threads || []).some(t => t.sessionId === id)) return () => openProject(p.id);
				if (p.chatSessionId === id) return () => openMira({ keep: true });
			}
			return null;
		}
		/**
		 * ⌘↑ (Ctrl+↑ off the Mac) goes one level up, as it does in Finder. The key is listened for here because DSH's
		 * shortcut service takes no arrow key with a single modifier in the browser. A terminal, an open dialog and a
		 * field that already holds text keep the key for themselves.
		 */
		function onUpKey(event) {
			const mac = UP_KEYS === "⌘↑";
			if (event.code !== "ArrowUp" || event.isComposing || event.repeat || event.shiftKey || event.altKey || event.metaKey !== mac || event.ctrlKey === mac) return;
			const target = typeof event.composedPath === "function" ? event.composedPath()[0] : event.target;
			if (target?.closest?.(".xterm") || document.querySelector('[aria-modal="true"]') || typedText(target)) return;
			const go = upMove();
			if (!go) return;
			event.preventDefault(); event.stopPropagation();
			go();
		}

		/**
		 * Beside the Session title, the way back up: a task names its project (a click opens the project conversation,
		 * whose task list marks the task just left) and who works on it; a task's sub-agent names the project too.
		 */
		/** Lucide corner-left-up (ISC licence): "one level up", drawn like the host's own header icons. */
		function UpIcon() {
			return h("svg", { className: "mp4-crumb-icon", width: 14, height: 14, viewBox: "0 0 24 24", fill: "none", stroke: "currentColor", strokeWidth: 1.75, strokeLinecap: "round", strokeLinejoin: "round", "aria-hidden": true },
				h("path", { d: "M14 9 9 4 4 9" }), h("path", { d: "M20 20h-7a4 4 0 0 1-4-4V4" }));
		}
		function SessionProject({ sessionId }) {
			const root = useRootSession(sessionId), onScreen = useOnScreenSession();
			const { owner, thread, project } = useTaskOf(root);
			const sub = Boolean(owner) && root !== sessionId;
			useEffect(() => { if (sub && onScreen === sessionId) keepTaskPanel(sessionId); }, [sub, onScreen, sessionId]);
			if (!owner) return null;
			return h("span", { className: "mp4-crumb" },
				h("button", { type: "button", className: "mp4-crumb-up", title: sub ? `回到项目「${owner.p.name}」` : `回到项目「${owner.p.name}」（${UP_KEYS}）`, "aria-keyshortcuts": sub ? undefined : (UP_KEYS === "⌘↑" ? "Meta+ArrowUp" : "Control+ArrowUp"), onClick: () => openProject(owner.p.id) },
					h(UpIcon), h("span", { className: "mp4-crumb-name" }, owner.p.name)),
				!sub && thread && executorOf(project, thread).name ? h("span", { className: "mp4-crumb-who", title: "执行这个任务的模型" }, executorOf(project, thread).name) : null);
		}

		function TaskTab({ sessionId }) {
			const { loaded, owner, project, thread } = useTaskOf(useRootSession(sessionId));
			if (!owner) return h("div", { className: "mp4-tabpane" }, h("p", { className: "mp4-muted" }, loaded ? "这个会话不是项目里的任务。" : "正在读取…"));
			if (!thread) return h("div", { className: "mp4-tabpane" }, h("p", { className: "mp4-muted" }, "正在读取任务…"));
			return h("div", { className: "mp4-tabpane" },
				h("h3", { className: "mp4-tab-title" }, thread.title),
				h(TaskSummary, { project, thread, inTask: owner.t.sessionId === sessionId }),
				owner.t.sessionId !== sessionId ? h("button", { type: "button", className: "mp4-link-btn", onClick: () => openTask(project.id, thread) }, "回到这个任务的对话 ›") : null,
				h("button", { type: "button", className: "mp4-link-btn", onClick: () => openProject(project.id) }, `回到「${project.name}」的对话 ›`));
		}

		/** The project whose conversation (the project assistant's Session) is on screen. */
		function useProjectOfChat(sessionId) {
			const all = useSnapshot();
			const summary = sessionId ? all.projects.find(p => p.chatSessionId === sessionId) : undefined;
			const detail = useSnapshot(summary ? summary.id : undefined);
			const project = summary && detail.project && detail.project.id === summary.id ? detail.project : null;
			return { loaded: all.loaded, summary, project };
		}

		/** A right-panel tab of a project conversation: one of the project page's panels, for the project on screen. */
		function projectTab(render) {
			return function ProjectTabBody({ sessionId }) {
				const { loaded, summary, project } = useProjectOfChat(sessionId);
				if (!summary) return h("div", { className: "mp4-tabpane" }, h("p", { className: "mp4-muted" }, loaded ? "这个会话不是项目对话。" : "正在读取…"));
				if (!project) return h("div", { className: "mp4-tabpane" }, h("p", { className: "mp4-muted" }, "正在读取项目…"));
				return h("div", { className: "mp4-tabpane" }, render(project, sessionId));
			};
		}

		/** Tasks of the project on screen; a started task opens as its own Session, one not started yet shows its summary here. */
		function TaskList({ project }) {
			const [peek, setPeek] = useState(null);
			// Coming back from a task (or one of its sub-agents), its row is marked and brought into view.
			const from = useRootSession(usePreviousSession()), box = useRef(null);
			const cameFrom = from ? project.threads.find(t => t.sessionId === from)?.id : undefined;
			useEffect(() => { if (cameFrom) box.current?.querySelector(".mp4-row-on")?.scrollIntoView({ block: "nearest" }); }, [cameFrom]);
			const thread = peek ? project.threads.find(t => t.id === peek) : null;
			const open = id => { const t = project.threads.find(x => x.id === id); if (t?.sessionId && !isOutside(t) && shell.workspace) openTask(project.id, t); else setPeek(id); };
			if (thread) return h(React.Fragment, null,
				h(TaskSummary, { project, thread }),
				showsOutside(project, thread) ? h(OutsideTask, { key: thread.id, project, thread }) : h("p", { className: "mp4-muted" }, "还没开始执行"),
				h("button", { type: "button", className: "mp4-link-btn", onClick: () => setPeek(null) }, "‹ 全部任务"));
			return h("div", { ref: box, style: { display: "contents" } }, h(ProjectTasks, { project, onOpen: open, activeId: cameFrom }));
		}
		const ProjectTasksTab = projectTab(project => h(TaskList, { key: project.id, project }));
		const ProjectMemoryTab = projectTab(project => h(ProjectMemory, { project }));
		const ProjectFilesTab = projectTab(project => h(SharedFiles, { key: project.id, project }));
		const ProjectSettingsTab = projectTab((project, sessionId) => h(React.Fragment, null,
			h(ProjectSettings, { key: project.id, project, onClose: () => revealTabs(sessionId, [PROJECT_TASKS_KIND]) }),
			h("button", { type: "button", className: "mp4-link-btn", onClick: () => revealTabs(sessionId, ["mira-projects-project-memory"]) }, "项目记忆 ›")));
		const ProjectAutomationsTab = projectTab((project, sessionId) => h(AutomationPanel, { key: project.id, project, onClose: () => revealTabs(sessionId, [PROJECT_TASKS_KIND]) }));
		const PROJECT_TAB_BODIES = { "mira-projects.project-tasks": ProjectTasksTab, "mira-projects.project-memory": ProjectMemoryTab, "mira-projects.project-files": ProjectFilesTab, "mira-projects.project-settings": ProjectSettingsTab, "mira-projects.project-automations": ProjectAutomationsTab };

		function ChangesTab({ sessionId }) {
			const { owner, project, thread } = useTaskOf(useRootSession(sessionId));
			const [copied, setCopied] = useState(""), [busy, setBusy] = useState(false), [error, setError] = useState(null);
			if (!thread) return h("div", { className: "mp4-tabpane" }, h("p", { className: "mp4-muted" }, owner ? "正在读取…" : "这个会话不是项目里的任务。"));
			// Without GitLab, acceptance commits on the task branch (worktrees.js land); merging into the main folder stays the owner's.
			const branchTask = !project.gitlab && thread.workspace?.branch && !thread.workspace.type;
			const landed = !thread.gitlab && thread.state === "done" ? thread.workspace?.landed : null;
			const LANDED = { committed: "已验收，改动已提交在任务分支上，还没合入主目录", empty: "已验收，任务分支上没有改动要提交", failed: "已验收，但自动提交没成功，改动还在任务工作目录里" };
			// Some hosts deny clipboard writes (DSH's built-in browser pane does): then select the command for ⌘C.
			const copy = ev => {
				const code = ev.currentTarget.previousSibling;
				const done = label => { setCopied(label); setTimeout(() => setCopied(""), 2000); };
				const select = () => { const range = document.createRange(); range.selectNodeContents(code); const sel = window.getSelection(); sel.removeAllRanges(); sel.addRange(range); let ok = false; try { ok = document.execCommand("copy"); } catch (_e) {} done(ok ? "已复制" : "已选中，按 ⌘C"); };
				if (navigator.clipboard) navigator.clipboard.writeText(landed.merge).then(() => done("已复制"), select); else select();
			};
			const mr = thread.gitlab, delivered = thread.state === "review_ready" || thread.state === "done" || Boolean(mr?.sha);
			const MR = { ready: "已就绪", merged: "已合并", merge_error: "合并失败", publishing: "正在提交", publish_error: "提交失败", closed: "已关闭" };
			const checks = thread.checklist || [], files = deliveredFiles(project, thread);
			// The owner's own 验收通过, the same action the server takes when they say it in the conversation.
			const canAccept = thread.state === "review_ready" && !thread.resolved, ready = !project.gitlab || ["ready", "merge_error"].includes(mr?.status);
			const accept = async () => {
				setBusy(true); setError(null);
				try { await act("updateThread", { projectId: project.id, threadId: thread.id, resolved: true, state: "done", expectedSha: mr?.sha }); } catch (e) { setError(e.message); } finally { setBusy(false); }
			};
			const acceptLabel = project.gitlab ? (mr?.status === "merge_error" ? "重试合并" : "验收通过并合并") : "验收通过";
			const after = mr ? "验收后合并这个提交。" : branchTask ? "验收后改动提交在任务分支上，不合入主目录。" : "";
			const openFiles = () => { try { shell.sidebar?.openTab("files", { revealIfOpened: true }); } catch (_e) { /* the panel is not ready */ } };
			const workspace = h("div", { className: "mp4-meta-row" }, h("span", { className: "mp4-k" }, "工作区"), h("span", { className: "mp4-mono mp4-ell" }, thread.workspace?.branch || project.cwd || "共享目录"),
				h("button", { type: "button", className: "mp4-link-btn mp4-files-link", onClick: openFiles }, "查看文件", h(Icon, { name: "chevron", size: 12 })));
			if (!delivered) return h("div", { className: "mp4-tabpane" }, h(TaskBanner, { project, thread }),
				h("p", { className: "mp4-muted mp4-note" }, "交付后，交付的文件、执行者的说明和检查结果都在这里。每一轮的逐文件改动，在对话里那一轮结尾的「改动文件」卡里打开。"), workspace);
			return h("div", { className: "mp4-tabpane" },
				h(TaskBanner, { project, thread,
					title: mr ? `MR ${MR[mr.status] || mr.status || ""}${mr.sha ? "，" + String(mr.sha).slice(0, 8) : ""}` : thread.state === "done" ? LANDED[landed?.status] || "已验收" : undefined,
					action: canAccept ? h("button", { type: "button", className: "mp4-btn mp4-btn-strong", disabled: busy || !ready, onClick: accept }, h(Icon, { name: "check", size: 14 }), busy ? "正在验收…" : acceptLabel) : null }),
				error ? h("p", { className: "mp4-err" }, error) : null,
				canAccept && after ? h("p", { className: "mp4-muted mp4-note" }, after) : null,
				landed?.status === "failed" ? h("div", { className: "mp-error" }, landed.error) : null,
				mr?.url ? h("a", { className: "mp4-mono", href: mr.url, target: "_blank", rel: "noopener" }, mr.url) : null,
				mr?.error ? h("div", { className: "mp-error" }, mr.error) : null,
				files.length ? h(Section, { label: `交付文件 ${files.length}` }, deliveryOrder(files).map((file, i) => h(DeliveredFile, { key: file.id, project, file, startOpen: i === 0 && canPreview(file) }))) : null,
				thread.summary ? h(Section, { label: "执行者说明" }, h("p", { className: "mp4-says" }, thread.summary)) : null,
				landed?.status === "committed" ? h(Section, { label: "合入" },
					h("div", { className: "mp4-mono" }, `提交 ${String(landed.sha).slice(0, 7)}，${landed.fileCount} 个文件`),
					(landed.files || []).map(f => h("div", { key: f, className: "mp4-mono mp4-ell", title: f }, f)),
					landed.fileCount > (landed.files || []).length ? h("div", { className: "mp4-muted" }, `另有 ${landed.fileCount - landed.files.length} 个文件`) : null,
					h("p", { className: "mp4-muted mp4-note" }, "平台没有动项目主目录。要合入，在主目录当前分支上运行："),
					h("div", { className: "mp4-merge" }, h("code", { className: "mp4-mono" }, landed.merge), h("button", { type: "button", className: "mp4-link-btn", onClick: copy }, copied || "复制"))) : null,
				checks.length ? h(Section, { label: `检查 ${checks.filter(c => c.done).length}/${checks.length}` }, checks.map((c, i) => h("div", { key: i, className: "mp4-check mp4-check-evidence" + (c.done ? " mp4-check-done" : "") }, h("span", { className: "mp4-check-mark" }, c.done ? h(Icon, { name: "check", size: 14 }) : h("i", { className: "mp4-check-todo" })), h("span", null, c.text)))) : null,
				workspace);
		}

		function AgentsTab({ sessionId }) {
			const { owner, thread } = useTaskOf(useRootSession(sessionId));
			if (!thread) return h("div", { className: "mp4-tabpane" }, h("p", { className: "mp4-muted" }, owner ? "正在读取…" : "这个会话不是项目里的任务。"));
			const exec = thread.experience?.execution || {};
			const children = thread.children || [];
			const tone = c => c.running ? "run" : c.status === "idle" || c.status === "not_loaded" ? "done" : "idle";
			// The same list in the task and in each of its sub-agents: the row on screen is marked, the others switch to it
			// and keep this list in front.
			const main = owner.t.sessionId;
			const go = id => { try { shell.workspace?.openSession(id); revealTabs(id, [AGENTS_TAB_KIND]); } catch (_e) {} };
			const mainRow = [h(Dot, { key: "dot", tone: exec.active ? "run" : "idle" }), h("span", { key: "body", className: "mp4-row-body" }, h("span", { className: "mp4-row-top" }, h("b", null, "主会话"), h("span", { className: "mp4-muted" }, exec.active ? "运行中" : "本回合结束")), h("span", { className: "mp4-muted" }, exec.label || ""))];
			return h("div", { className: "mp4-tabpane" },
				sessionId === main ? h("div", { className: "mp4-row mp4-row-static mp4-row-on", "aria-current": "true" }, mainRow) : h("button", { type: "button", className: "mp4-row", onClick: () => go(main) }, mainRow),
				children.length === 0 ? h("p", { className: "mp4-muted" }, "这个任务没有另外派出去并行做的事。") : null,
				children.map(c => h("button", { key: c.id, type: "button", className: "mp4-row mp4-row-child" + (c.id === sessionId ? " mp4-row-on" : ""), "aria-current": c.id === sessionId ? "true" : undefined, style: { paddingLeft: 10 + 16 * Math.max(1, c.depth || 1) }, onClick: () => go(c.id) },
					h(Dot, { tone: tone(c) }),
					h("span", { className: "mp4-row-body" }, h("span", { className: "mp4-row-top" }, h("b", { className: "mp4-ell" }, c.label || "子智能体"), h("span", { className: "mp4-muted" }, c.running ? "运行中" : "已结束")), h("span", { className: "mp4-muted" }, c.mode || "")))));
		}

		// ---------------------------------------------------------------- the plugin's tools inside a native conversation

		function toolArgs(block) {
			const raw = "kind" in block ? block.call?.argsRaw : block.phase === "start" ? block.argsRaw : "";
			try { return JSON.parse(raw || "{}") || {}; } catch (_) { return {}; }
		}
		function ToolLine({ icon, children }) {
			return h("div", { className: "mp4-toolline" }, h(Icon, { name: icon || "check", size: 13 }), h("span", { className: "mp4-ell" }, children));
		}
		/** A question waiting on the owner, as option buttons: the same card in the conversation and above the task's composer. */
		function DecisionAsk({ project, decision, question, options, best }) {
			const [busy, setBusy] = useState(false), [error, setError] = useState(null);
			const choose = async i => { if (!decision) return; setBusy(true); setError(null); try { await act("answerDecision", { projectId: project.id, decisionId: decision.id, chosen: i }); } catch (e) { setError(e.message); } finally { setBusy(false); } };
			return h("div", { className: "mp4-ask" },
				h("div", { className: "mp4-ask-k" }, "等你回答"),
				h("div", { className: "mp4-ask-q" }, question),
				options.map((o, i) => h("button", { key: i, type: "button", className: "mp4-opt", disabled: busy || !decision, onClick: () => choose(i) },
					h("span", { className: "mp4-opt-mark" }), h("span", null, h("b", null, o.label, i === best ? "（推荐）" : ""), o.consequence ? h("span", { className: "mp4-muted mp4-part" }, o.consequence) : null))),
				h("span", { className: "mp4-muted" }, "都不合适就在下面输入框直接说"),
				error ? h("span", { className: "mp-error" }, error) : null);
		}
		function DecisionToolView({ block, sessionId }) {
			const args = toolArgs(block), { project, thread } = useTaskOf(sessionId);
			if (!args.question) return h(ToolLine, { icon: "decide" }, "正在准备提问…");
			const decision = project && thread ? Object.values(project.decisions || {}).filter(d => d.threadId === thread.id && d.question === args.question).sort((a, b) => b.seq - a.seq)[0] : null;
			const options = decision?.options || args.options || [];
			if (decision && decision.chosen !== null) return h("div", { className: "mp4-ask mp4-ask-done" }, "提问：", decision.question, h("br"), h("b", null, "已回答：", options[decision.chosen]?.label ?? decision.chosen));
			if (decision?.withdrawn) return h("div", { className: "mp4-ask mp4-ask-done" }, "提问：", decision.question, h("br"), h("span", { className: "mp4-muted" }, "已作废", decision.withdrawReason ? `：${decision.withdrawReason}` : ""));
			return h(DecisionAsk, { project, decision, question: args.question, options, best: decision?.recommended ?? args.recommended });
		}
		/**
		 * Above a task's composer: the questions the task waits on the owner for. DSH folds a finished turn's tool calls,
		 * and the question card lives on the executor's request_decision call, so without this the options stay hidden.
		 */
		function TaskDecisionDock({ sessionId }) {
			const { project, thread } = useTaskOf(sessionId);
			if (!project || !thread) return null;
			const open = Object.values(project.decisions || {}).filter(d => d.threadId === thread.id && d.chosen == null && !d.withdrawn).sort((a, b) => a.seq - b.seq);
			if (!open.length) return null;
			return h("div", { className: "mp4-dock-asks" }, open.map(d => h(DecisionAsk, { key: d.id, project, decision: d, question: d.question, options: d.options || [], best: d.recommended })));
		}
		/** E2E #12: above the composer of a task's DSH conversation while another executor has the task: where typed words go. */
		function TaskElsewhereDock({ sessionId }) {
			const { project, thread } = useTaskOf(sessionId);
			const next = thread && (thread.runner || "dsh");
			if (!project || !thread || next === "dsh" || thread.resolved || thread.state === "done") return null;
			const name = EXECUTOR_NAMES[next] || next;
			return h("div", { className: "mp4-dock-notes" }, h("div", { className: "mp4-dock-note" },
				h("span", { className: "mp4-dock-note-text" }, `由 ${name} 执行，这里输入的话会转给它`),
				h("button", { type: "button", className: "mp4-mem-link", onClick: () => navigateProject(project.id, thread.id) }, `打开 ${name} 的对话`)));
		}
		/**
		 * Above the composer: what the background reader just kept in 关于你 from this conversation, one quiet line each,
		 * with 撤销. The assistant's own conversation also shows lines picked up in projects and what the evening tidy retired.
		 */
		function MiraMemoryNotes({ sessionId }) {
			const personal = usePersonal(), today = useToday(), all = useSnapshot(), task = useTaskOf(sessionId), chat = useProjectOfChat(sessionId);
			const [busy, setBusy] = useState(false), [error, setError] = useState(null), box = useRef(null);
			const home = !!sessionId && personal.sessionId === sessionId, recent = at => Date.now() - at < 7 * 24 * 3600e3;
			const here = s => home ? true : s.where === "hub" ? false : chat.summary ? chat.summary.id === s.where && !s.threadId : task.owner ? task.owner.p.id === s.where && task.owner.t.id === s.threadId : false;
			const kept = !sessionId || !today.loaded ? [] : (today.about || []).filter(item => item.source?.kind === "auto" && !item.ackedAt && recent(item.at) && here(item.source));
			const retired = home && today.loaded ? (today.forgotten?.about || []).filter(item => (item.forgottenWhy === "merged" || item.forgottenWhy === "outdated") && !item.ackedAt && recent(item.forgottenAt)) : [];
			const rows = [...kept.map(item => ({ item, kept: true })), ...retired.map(item => ({ item, kept: false }))];
			const shown = rows.map(r => r.item.id).join(",");
			// Seen once the owner sends the next message here: no "知道了" to click.
			useEffect(() => {
				if (!shown) return;
				const ids = shown.split(",");
				let done = false;
				const ack = target => {
					const area = composerArea(box.current);
					if (done || !area || !area.contains(target)) return;
					done = true;
					setTimeout(() => { act("aboutAck", { itemIds: ids }).catch(() => {}); }, 800);
				};
				const onKey = e => { if (e.key === "Enter" && !e.shiftKey && !e.isComposing) ack(e.target); };
				const onClick = e => { const b = e.target.closest && e.target.closest("button"); if (b && /发送|send/i.test(b.getAttribute("aria-label") || b.title || "")) ack(b); };
				document.addEventListener("keydown", onKey, true);
				document.addEventListener("click", onClick, true);
				return () => { document.removeEventListener("keydown", onKey, true); document.removeEventListener("click", onClick, true); };
			}, [shown]);
			if (!rows.length) return null;
			const call = async (op, body) => {
				setBusy(true); setError(null);
				try { await act(op, body); } catch (e) { setError(String(e?.message || e)); } finally { setBusy(false); }
			};
			const from = s => home && s.where !== "hub" ? `（来自「${(all.projects || []).find(p => p.id === s.where)?.name || "项目"}」）` : "";
			return h("div", { ref: box, className: "mp4-dock-notes", role: "status" },
				rows.slice(0, 3).map(({ item, kept }) => h("div", { key: item.id, className: "mp4-dock-note" },
					h("button", { type: "button", className: "mp4-dock-note-text", title: "点一下，在输入框里改正这条",
						onClick: e => { if (!draftInto(e.currentTarget, kept ? `这条记忆不准确：「${item.text}」。正确的是：` : `这条不该去掉：「${item.text}」。`)) setError("没找到输入框"); } },
						kept ? `已生成记忆：${item.text}${item.source.replacedText ? `（替换「${item.source.replacedText}」）` : ""}${from(item.source)}`
							: `${item.forgottenWhy === "merged" ? "已合并重复记忆" : "已移除过时记忆"}：${item.text}`),
					h("button", { type: "button", className: "mp4-dock-x", disabled: busy, title: kept ? "撤销" : "恢复", "aria-label": kept ? `撤销这条记忆：${item.text}` : `恢复这条：${item.text}`,
						onClick: () => call(kept ? "aboutUndo" : "aboutRestore", { itemId: item.id }) }, h(Icon, { name: "close", size: 12 })))),
				rows.length > 3 ? h("div", { className: "mp4-dock-note mp4-muted" }, `还有 ${rows.length - 3} 条`) : null,
				error ? h("span", { className: "mp-error", role: "alert" }, error) : null);
		}

		// ---------------------------------------------------------------- 截图来问
		/**
		 * The screenshot shortcut on the owner's Mac hands its picture to the server, which picks the conversation it belongs
		 * to; the helper then points this page at `#mira-shot/<id>`. The page opens that conversation, puts the picture in its
		 * composer the way a paste would (DSH's own attachment path), and leaves the cursor there. Sending stays his.
		 */
		const SHOT_HASH = /^#mira-shot\/(s_[0-9a-f]{12})(?:\/(\d))?$/;
		const shotBus = { value: null, subs: new Set(), set(v) { this.value = v; this.subs.forEach(f => f()); } };
		const useShotBus = () => useSyncExternalStore(f => { shotBus.subs.add(f); return () => shotBus.subs.delete(f); }, () => shotBus.value, () => null);
		const sleep = ms => new Promise(r => setTimeout(r, ms));
		/**
		 * Put the picture in one composer the way a paste would. DSH keeps the conversation it just left mounted (hidden),
		 * so the editor is found from the strip DSH renders in the target conversation's own composer, never the first on the page.
		 */
		async function pasteShot(shot, editor) {
			const response = await fetch(shot.image, { credentials: "same-origin", cache: "no-store" });
			if (!response.ok) throw new Error(response.status === 410 ? "截图已清理，再按一次快捷键" : "截图没取到");
			const blob = await response.blob(), data = new DataTransfer();
			data.items.add(new File([blob], blob.type === "image/png" ? "截图.png" : "截图.jpg", { type: blob.type }));
			editor.focus();
			editor.dispatchEvent(new ClipboardEvent("paste", { clipboardData: data, bubbles: true, cancelable: true }));
		}
		/** Open one place for a shot (a task, a project's conversation, the assistant) and put the picture there. */
		async function goShot(shot, place) {
			let sessionId = null;
			if (place.kind === "task") {
				const state = await (await fetch(STATE_URL + "?project=", { credentials: "same-origin", cache: "no-store" })).json();
				const thread = (state.projects || []).find(p => p.id === place.projectId)?.threads?.find(t => t.id === place.threadId);
				sessionId = thread ? (isOutside(thread) ? thread.dshSessionId : thread.sessionId) : null;
				// A task with no conversation of its own to type into: its project's conversation instead.
				if (sessionId) openTask(place.projectId, thread);
				else place = { kind: "project", projectId: place.projectId, title: place.projectName, projectName: place.projectName };
			}
			if (place.kind === "project") sessionId = await openProject(place.projectId);
			if (place.kind === "assistant") sessionId = await openMira();
			// The strip in that conversation's composer puts the picture in (ShotDock); it waits for this mark.
			shotBus.set({ shot, here: place, sessionId, error: null, paste: Date.now() });
		}
		let shotTaking = null;
		async function takeShot() {
			const m = SHOT_HASH.exec(window.location.hash || "");
			if (!m || shotTaking === m[1]) return;
			shotTaking = m[1];
			window.history.replaceState(window.history.state, "", window.location.pathname + window.location.search);
			// The card on the Mac may have picked another row than the first: its number comes after the id.
			try { const shot = await act("shotClaim", { id: m[1] }); await goShot(shot, shotPlaces(shot)[Number(m[2] || 0)] || shot.target); }
			catch (e) { const sessionId = await openMira().catch(() => null); shotBus.set({ shot: null, here: { kind: "assistant" }, sessionId, error: String(e?.message || e) }); }
			finally { shotTaking = null; }
		}
		/** Where a shot can go, in the order the Mac's card lists them: the pick, the alternatives, then the personal assistant. */
		const samePlace = (a, b) => a.kind === b.kind && a.projectId === b.projectId && a.threadId === b.threadId;
		const shotPlaces = shot => [shot.target, ...(shot.others || []), { kind: "assistant" }].filter((p, i, all) => p && all.findIndex(q => q && samePlace(p, q)) === i);
		/**
		 * A task link the assistants write, [任务标题](https://task.local/<线程id>): it opens that task in its own conversation,
		 * in this tab. DSH's Markdown keeps only http(s) links (a #… link is dropped to plain text), so the address is a full
		 * one that leads nowhere outside; the click is taken before DSH would open it. #mira-task/<id> in the address bar works too.
		 */
		const TASK_LINK = /^(?:#mira-task\/|https:\/\/task\.local\/)(t_[0-9a-f]{8,12})\/?$/;
		async function openTaskLink(threadId) {
			const state = await (await fetch(STATE_URL + "?project=", { credentials: "same-origin", cache: "no-store" })).json();
			for (const p of state.projects || []) {
				const thread = (p.threads || []).find(t => t.id === threadId);
				if (thread) { openTask(p.id, thread); return true; }
			}
			return false;
		}
		const placeName = p => p.kind === "task" ? p.title : p.kind === "project" ? `「${p.title}」的讨论` : "个人助理";
		/** Above the composer the shot landed in: the other likely places, one click each. Gone once he sends or closes it. */
		function ShotDock({ sessionId }) {
			const bus = useShotBus(), box = useRef(null), [busy, setBusy] = useState(false);
			const mine = !!bus && !!sessionId && bus.sessionId === sessionId;
			const paste = mine && bus.paste && bus.shot ? bus.paste : 0;
			useEffect(() => {
				if (!paste) return;
				let gone = false;
				(async () => {
					let editor = null;
					for (let i = 0; i < 40 && !gone && !editor; i++) {
						const area = composerArea(box.current), found = area && area.querySelector('[contenteditable="true"]');
						if (found && found.getClientRects().length) editor = found; else await sleep(150);
					}
					if (gone || shotBus.value?.paste !== paste) return;
					try {
						if (!editor) throw new Error("没找到输入框，截图没放进去");
						await pasteShot(shotBus.value.shot, editor);
						if (shotBus.value?.paste === paste) shotBus.set({ ...shotBus.value, paste: 0 });
					} catch (e) { if (shotBus.value?.paste === paste) shotBus.set({ ...shotBus.value, paste: 0, error: String(e?.message || e) }); }
				})();
				return () => { gone = true; };
			}, [paste]);
			useEffect(() => {
				if (!mine) return;
				const done = target => { const area = composerArea(box.current); if (area && area.contains(target)) setTimeout(() => shotBus.set(null), 300); };
				const onKey = e => { if (e.key === "Enter" && !e.shiftKey && !e.isComposing) done(e.target); };
				const onClick = e => { const b = e.target.closest && e.target.closest("button"); if (b && /发送|send/i.test(b.getAttribute("aria-label") || b.title || "")) done(b); };
				document.addEventListener("keydown", onKey, true); document.addEventListener("click", onClick, true);
				return () => { document.removeEventListener("keydown", onKey, true); document.removeEventListener("click", onClick, true); };
			}, [mine]);
			if (!mine) return null;
			const hold = h("span", { ref: box, hidden: true });
			const others = bus.shot ? shotPlaces(bus.shot).filter(p => !samePlace(p, bus.here)).slice(0, 3) : [];
			if (!bus.error && !others.length) return hold;
			const move = async place => { setBusy(true); try { await goShot(bus.shot, place); } catch (e) { shotBus.set({ ...bus, error: String(e?.message || e) }); } finally { setBusy(false); } };
			return h("div", { ref: box, className: "mp4-dock-notes", role: "status" },
				h("div", { className: "mp4-dock-note" },
					bus.error ? h("span", { className: "mp-error" }, bus.error) : others.length ? h(React.Fragment, null,
						h("span", { className: "mp4-shot-q", title: bus.shot.why || undefined }, "不是这里？"),
						others.map(p => h("button", { key: [p.kind, p.projectId, p.threadId].join(":"), type: "button", className: "mp4-shot-alt", disabled: busy,
							title: p.kind === "task" ? `项目「${p.projectName}」` : undefined, onClick: () => move(p) }, placeName(p)))) : null,
					h("button", { type: "button", className: "mp4-dock-x", title: "关掉", "aria-label": "关掉截图提示", onClick: () => shotBus.set(null) }, h(Icon, { name: "close", size: 12 }))));
		}
		/** The nearest ancestor of `from` that holds DSH's composer editor. */
		function composerArea(from) {
			for (let n = from; n && n !== document.body; n = n.parentElement) if (n.querySelector && n.querySelector('[contenteditable="true"]')) return n;
			return null;
		}
		/** Put a sentence at the end of DSH's composer, as typing would, and leave the cursor after it. */
		/**
		 * The assistant's mark: no character, only the pixel feel. A 7 × 7 tile in the colour of what it is doing; the inner pixels
		 * say the rest: running pixels cycle, waiting is one dot, done is a check, idle is a dim line.
		 */
		const MARK_FILL = { working: "#0090ff", waiting: "#eab308", done: "#30a46c", sleeping: "#8d8d8d" };
		const MARK_INNER = { working: [[2, 2], [3, 2], [4, 2], [4, 3], [4, 4], [3, 4], [2, 4], [2, 3]], waiting: [[3, 3]], done: [[1, 3], [2, 4], [3, 3], [4, 2], [5, 1]], sleeping: [[2, 3], [3, 3], [4, 3]] };
		function PixelMark({ state = "sleeping", size = 44 }) {
			const fill = MARK_FILL[state] || MARK_FILL.sleeping, inner = MARK_INNER[state] || MARK_INNER.sleeping;
			const tile = [];
			for (let y = 0; y < 7; y++) for (let x = 0; x < 7; x++) if (!((x === 0 || x === 6) && (y === 0 || y === 6))) tile.push([x, y]);
			return h("svg", { viewBox: "0 0 7 7", width: size, height: size, shapeRendering: "crispEdges", className: "mp4-mark mp4-mark-" + state, "aria-hidden": true },
				tile.map(([x, y]) => h("rect", { key: x + "-" + y, x, y, width: 1.02, height: 1.02, fill })),
				inner.map(([x, y], i) => h("rect", { key: "i" + i, x, y, width: 1.02, height: 1.02, fill: "#fff", style: state === "working" ? { animationDelay: `${i * 120}ms` } : undefined })));
		}
		/** A plain letter avatar with a faint pixel grain, in place of the pixel faces. */
		function LetterAvatar({ name, tint, size }) {
			const ch = String(name || "助").trim().charAt(0) || "助";
			return h("span", { className: "mp4-letter-av", style: { width: size, height: size, fontSize: Math.max(10, Math.round(size * 0.46)), "--av-tint": tint || "#8d8d8d" }, "aria-hidden": true }, ch);
		}
		/** Loading, as Codex shows it: a small spinner and one grey line, centred in the pane. */
		function Loading({ text = "正在读取…" }) {
			return h("div", { className: "mp4-loading", role: "status" }, h("i", { className: "mp4-spin", "aria-hidden": true }), text);
		}
		function draftInto(from, text) {
			const area = composerArea(from), editor = area && area.querySelector('[contenteditable="true"]');
			if (!editor) return false;
			editor.focus();
			const range = document.createRange(), selection = window.getSelection();
			range.selectNodeContents(editor); range.collapse(false);
			selection.removeAllRanges(); selection.addRange(range);
			return document.execCommand("insertText", false, (editor.textContent.trim() ? " " : "") + text);
		}
		function UpdateCardView({ block }) {
			const args = toolArgs(block), state = args.state ? STATE[args.state]?.label : null;
			if ("kind" in block && block.isError) return h(ToolLine, { icon: "close" }, "任务卡没更新");
			return h(ToolLine, { icon: "tasks" }, ["更新了任务卡", state, args.summary].filter(Boolean).join("，"));
		}
		/**
		 * What inbox_read handed the agent, read back from the tool's own output (owner 2026-11-19: 「具体读了什么新消息，我希望也能展开看到」):
		 * one line per item, who and where in gray, what was said in full colour. Plain words only: no ids, no markup.
		 */
		const INBOX_KIND = { thread_opened: "新任务", relay: "转达", decision_requested: "等你拍板", executor_changed: "换了执行者", task_returned: "任务交还", project_file: "文件更新", external_event: "外部事件", automation_triggered: "定时任务", project_configured: "项目设置改了", merge_request: "合并请求", repository_imported: "导入了仓库", thread_state: "任务状态", card_updated: "任务卡更新", card_held: "任务卡", decision_answered: "你拍了板", decision_withdrawn: "拍板作废", card_acted: "你点了卡片", card_dismissed: "你忽略了卡片", mira_requested: "整理请求", turn_failed: "唤醒失败" };
		const inboxPlain = s => String(s || "").replace(/<shared-files>[\s\S]*?<\/shared-files>/g, "").replace(/<[^>]+>/g, " ")
			.replace(/&quot;/g, '"').replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&amp;/g, "&").replace(/线程/g, "任务").replace(/\s+/g, " ").trim();
		const inboxWho = name => name === "用户" ? "你" : name;
		const inboxWhere = w => !w || w === "群聊" ? "项目对话" : /^任务「/.test(w) ? w : "一个任务";
		function inboxItems(text) {
			const items = [];
			for (const m of String(text || "").matchAll(/<item\s([^>]*)>([\s\S]*?)<\/item>/g)) {
				const a = {};
				for (const x of m[1].matchAll(/([\w-]+)="([^"]*)"/g)) a[x[1]] = inboxPlain(x[2]);
				const title = a.title ? `「${a.title}」` : "";
				if (a.kind === "message") items.push({ who: `${inboxWho(a.from)}，${inboxWhere(a.where)}`, what: inboxPlain(m[2]) });
				else if (a.kind === "thread_opened") items.push({ who: `新任务${title}`, what: inboxPlain((m[2].match(/<brief>([\s\S]*?)<\/brief>/) || [])[1]) });
				else if (a.kind === "relay") items.push({ who: `${inboxWho(a.from)} 转达`, what: inboxPlain(m[2]) });
				else items.push({ who: (INBOX_KIND[a.kind] || "项目动态") + (a.kind === "decision_requested" ? title : ""), what: inboxPlain(m[2]) });
			}
			return items;
		}
		function InboxReadView({ block }) {
			const [open, setOpen] = useState(false);
			const text = "kind" in block ? (block.content || []).filter(c => c.type === "text").map(c => c.text).join("\n") : "";
			const items = inboxItems(text);
			if (!items.length) return h(ToolLine, { icon: "inbox" }, /没有未读/.test(text) ? "看了项目里的消息，没有新的" : "读了项目里的新消息");
			return h("div", { className: "mp4-inbox" },
				h("button", { type: "button", className: "mp4-toolline mp4-inbox-head", "aria-expanded": open, onClick: () => setOpen(v => !v) },
					h(Icon, { name: "inbox", size: 13 }), h("span", { className: "mp4-ell" }, `读了项目里的 ${items.length} 条新消息`), h("span", { className: "mp4-inbox-chev" }, h(Icon, { name: "chevron", size: 12 }))),
				open ? h("ol", { className: "mp4-inbox-list" }, items.map((it, i) => h("li", { key: i }, h("span", { className: "mp4-inbox-who" }, it.who), it.what))) : null);
		}
		function PostMessageView({ block }) {
			// While the call streams its arguments are partial JSON: read the text so far, so the reply grows like DSH's own.
			const raw = "kind" in block ? block.call?.argsRaw : block.phase === "start" ? block.argsRaw : "";
			const text = usePacedText(toolArgs(block).text || partialTextField(raw || ""));
			if (!text) return h(ToolLine, { icon: "reply" }, "正在写回复…");
			return h("div", { className: "mp4-posted" }, h(RichText, { text, members: [] }));
		}
		// The project assistant's own tools, one line each in its conversation.
		function OpenThreadView({ block }) { const a = toolArgs(block); return h(ToolLine, { icon: "tasks" }, a.title ? `开了任务「${a.title}」` : "正在开任务…"); }
		function AddMemberView({ block }) { const a = toolArgs(block); return h(ToolLine, { icon: "agents" }, a.name ? `写了一份做事说明${a.persona ? `：${a.persona}` : ""}` : "正在写做事说明…"); }
		function MessageThreadView({ block }) { const a = toolArgs(block); return h(ToolLine, { icon: "reply" }, "转达给任务：", a.text || ""); }
		function ListThreadsView() { return h(ToolLine, { icon: "tasks" }, "看了任务列表"); }
		function ReadThreadView() { return h(ToolLine, { icon: "tasks" }, "看了一个任务的记录"); }
		function NoReplyView() { return h(ToolLine, { icon: "check" }, "这一轮不用回复"); }
		function MessageCoordinatorView({ block }) { const args = toolArgs(block); return h(ToolLine, { icon: "reply" }, "告诉项目助理：", args.text || ""); }
		function WithdrawDecisionView({ block }) { const args = toolArgs(block); return h(ToolLine, { icon: "close" }, "撤回了一个提问", args.reason ? `：${args.reason}` : ""); }

		// ---------------------------------------------------------------- 随手问: the floating ball

		const SPRITE_PAL = { h: "#2B4C8F", H: "#4176E6", s: "#F8DCCB", e: "#0F1115", b: "#F4A3A3", m: "#C2410C", c: "#7FB2FF", w: "#FFFFFF" };
		const SPRITE = ["................", ".....hhhhhh.....", "...hhHHhhhhhh...", "..hhHhhhhhhhhhc.", "..hhhhhhhhhhhccc", ".hhhhsssssshhhc.", ".hhhsssssssshhh.", ".hhssessssesshh.", ".hhssessssesshh.", ".hhsbssssssbshh.", "..hhsssmmssshh..", "..hhhsssssshhh..", "...hh.ssss.hh...", "....HHHHHHHH....", "...HHwwHHHHwwHH.", "...HHHHHHHHHHH.."];
		function PixelMira({ size = 40 }) {
			const rects = [];
			SPRITE.forEach((row, y) => row.split("").forEach((ch, x) => { if (SPRITE_PAL[ch]) rects.push(h("rect", { key: x + "-" + y, x, y, width: 1, height: 1, fill: SPRITE_PAL[ch] })); }));
			return h("svg", { width: size, height: size, viewBox: "0 0 16 16", shapeRendering: "crispEdges", "aria-hidden": true }, rects);
		}
		/** Radix step-9 colours the assistant can wear (same names as project colours). */
		const LOOK_FILLS = { yellow: "#ffc53d", orange: "#f76b15", red: "#e5484d", pink: "#d6409f", purple: "#8e4ec6", blue: "#0090ff", teal: "#12a594", green: "#30a46c", gray: "#8d8d8d" };
		const LOOK_DEFAULT = { name: "Tacit", color: "yellow", shape: "mochi", eyes: "dot", accessory: "sprout", traits: ["温和", "细心"] };
		/**
		 * The assistant's figure (2026-11-18): the brand's character, a round dot with pixel eyes, in the owner's colour and
		 * eyes (the old pixel figure and its accessories are retired; shape and accessory stay in the data, unused). Below 24 px
		 * it is the dot alone: eyes that small do not read (the brand rule).
		 */
		const BUDDY_FILLS = { blue: "#3964fe", purple: "#8e4ec6", teal: "#12a594", green: "#30a46c", orange: "#f76b15", pink: "#d6409f", red: "#e5484d", yellow: "#ffc53d", gray: "#8d8d8d" };
		const BUDDY_EYES = { dot: [[4, 5, 1, 2], [9, 5, 1, 2]], big: [[3, 5, 2, 2], [9, 5, 2, 2]], sleepy: [[3, 6, 2, 1], [9, 6, 2, 1]], star: [[4, 4, 1, 3], [3, 5, 3, 1], [9, 4, 1, 3], [8, 5, 3, 1]] };
		const BUDDY_FACES = { done: [[3, 6, 1, 1], [4, 5, 1, 1], [5, 6, 1, 1], [8, 6, 1, 1], [9, 5, 1, 1], [10, 6, 1, 1]], sleeping: [[3, 6, 2, 1], [9, 6, 2, 1]], waiting: [[3, 5, 2, 2], [9, 5, 2, 2]] };
		function BuddyDot({ look, size = 20, blink = false, face = null, eyesFrom = 24 }) {
			// No look saved yet: the brand's blue (the retired figure's yellow default stays with it).
			const L = { ...LOOK_DEFAULT, color: "blue", ...(look || {}) };
			const fill = face === "waiting" ? BUDDY_FILLS.yellow : face === "done" ? BUDDY_FILLS.green : BUDDY_FILLS[L.color] || BUDDY_FILLS.blue;
			const ink = fill === BUDDY_FILLS.yellow ? "#1d1d1f" : "#fff", eyes = BUDDY_FACES[face] || BUDDY_EYES[L.eyes] || BUDDY_EYES.dot;
			return h("svg", { viewBox: "0 0 14 14", width: size, height: size, "aria-hidden": true, className: "mp4-buddy" + (blink || face === "waiting" ? " mp4-buddy-blink" : "") + (face === "working" ? " mp4-buddy-glance" : "") },
				h("circle", { cx: 7, cy: 7, r: 7, fill }),
				size >= eyesFrom ? h("g", { className: "mp4-buddy-eyes", fill: ink, shapeRendering: "crispEdges" }, eyes.map(([x, y, w, hh], i) => h("rect", { key: i, x, y, width: w, height: hh }))) : null);
		}
		const LOOK_BODIES = {
			mochi: ["", "", ".....oooo.....", "...ooyyyyoo...", "..oyhhyyyyyo..", ".oyhyyyyyyyyo.", ".oyyyyyyyyyyo.", "oyyyyyyyyyyyyo", "oyyyyyyyyyyyyo", "oyyyyyyyyyyyyo", "oyssssssssssyo", ".oooooooooooo."],
			round: ["", "", "....oooooo....", "...oyyyyyyo...", "..oyhhyyyyyo..", ".oyhyyyyyyyyo.", ".oyyyyyyyyyyo.", ".oyyyyyyyyyyo.", ".oyyyyyyyyyyo.", ".oyyyyyyyyyyo.", "..oyssssssyo..", "...oooooooo..."],
			block: ["", "", ".oooooooooooo.", ".oyhhyyyyyyyo.", ".oyhyyyyyyyyo.", ".oyyyyyyyyyyo.", ".oyyyyyyyyyyo.", ".oyyyyyyyyyyo.", ".oyyyyyyyyyyo.", ".oyyyyyyyyyyo.", ".oyssssssssyo.", ".oooooooooooo."],
		};
		/** Open eyes by style, for the working (looking at the screen: one column left) and asking faces. */
		const LOOK_EYES = {
			dot: c => [[7, c, "e"], [8, c, "e"], [7, c + 5, "e"], [8, c + 5, "e"]],
			big: c => [[7, c, "e"], [7, c + 1, "W"], [8, c, "e"], [8, c + 1, "e"], [7, c + 4, "e"], [7, c + 5, "W"], [8, c + 4, "e"], [8, c + 5, "e"]],
			sleepy: c => [[8, c, "e"], [8, c + 1, "e"], [8, c + 4, "e"], [8, c + 5, "e"], [7, c, "s"], [7, c + 1, "s"], [7, c + 4, "s"], [7, c + 5, "s"]],
			star: c => [[7, c, "e"], [8, c, "e"], [7, c + 5, "e"], [8, c + 5, "e"], [6, c + 1, "W"], [6, c + 6, "W"]],
		};
		/** Accessories as [row, column, colour key] on the 14-wide figure; rows 0–1 sit above the head. */
		const LOOK_ACCESSORIES = {
			sprout: [[0, 6, "g"], [0, 8, "G"], [1, 7, "G"]],
			bow: [[1, 9, "P"], [1, 10, "P"], [1, 12, "P"], [1, 13, "P"], [2, 9, "P"], [2, 10, "P"], [2, 11, "Q"], [2, 12, "P"], [2, 13, "P"], [3, 9, "P"], [3, 10, "P"], [3, 12, "P"], [3, 13, "P"]],
			cap: [[0, 5, "k"], [0, 6, "k"], [0, 7, "k"], [0, 8, "k"], [1, 4, "k"], [1, 5, "k"], [1, 6, "k"], [1, 7, "k"], [1, 8, "k"], [1, 9, "k"], [2, 3, "k"], [2, 4, "k"], [2, 5, "k"], [2, 6, "k"], [2, 7, "k"], [2, 8, "k"], [2, 9, "k"], [2, 10, "k"], [2, 11, "k"], [1, 10, "u"]],
			glasses: [[6, 2, "W"], [6, 3, "W"], [6, 4, "W"], [6, 5, "W"], [6, 8, "W"], [6, 9, "W"], [6, 10, "W"], [6, 11, "W"], [7, 2, "W"], [7, 5, "W"], [7, 8, "W"], [7, 11, "W"], [8, 2, "W"], [8, 5, "W"], [8, 8, "W"], [8, 11, "W"], [7, 6, "W"], [7, 7, "W"], [9, 2, "W"], [9, 3, "W"], [9, 4, "W"], [9, 5, "W"], [9, 8, "W"], [9, 9, "W"], [9, 10, "W"], [9, 11, "W"]],
			headphones: [[1, 4, "k"], [1, 5, "k"], [1, 6, "k"], [1, 7, "k"], [1, 8, "k"], [1, 9, "k"], [2, 3, "k"], [2, 10, "k"], [3, 2, "k"], [3, 11, "k"], [6, 0, "k"], [7, 0, "k"], [8, 0, "k"], [6, 13, "k"], [7, 13, "k"], [8, 13, "k"], [7, 1, "u"], [7, 12, "u"]],
			antenna: [[0, 7, "r"], [0, 8, "r"], [1, 7, "k"], [1, 8, "r"]],
			none: [],
		};
		const pxShadeOf = (hex, k) => { const n = parseInt(hex.slice(1), 16), t = k < 0 ? 0 : 255, a = Math.abs(k); return "#" + [n >> 16, (n >> 8) & 255, n & 255].map(v => Math.round(v + (t - v) * a).toString(16).padStart(2, "0")).join(""); };
		/** The assistant's figure on a 14 × 12 grid with a face: "look" (working), "ask" (waiting), "happy", "sleep". */
		function lookCells(look, face) {
			const L = { ...LOOK_DEFAULT, ...(look || {}) }, F = LOOK_FILLS[L.color] || LOOK_FILLS.yellow;
			const PAL = { o: pxShadeOf(F, -0.42), y: F, h: pxShadeOf(F, 0.6), s: pxShadeOf(F, -0.16), e: "#202020", c: "#ff8fa8", W: "#ffffff", g: "#46a758", G: "#2a7e3b", k: "#3a3f47", u: "#0090ff", r: "#e5484d", P: "#e93d82", Q: "#ffffff" };
			const at = new Map(), put = (r, c, ch) => { if (r >= 0 && r < 12 && c >= 0 && c < 14 && PAL[ch]) at.set(r * 14 + c, PAL[ch]); };
			(LOOK_BODIES[L.shape] || LOOK_BODIES.mochi).forEach((row, r) => [...row].forEach((ch, c) => put(r, c, ch)));
			const eyes = LOOK_EYES[L.eyes] || LOOK_EYES.dot;
			if (face === "look") eyes(L.accessory === "glasses" ? 4 : 3).forEach(([r, c, ch]) => put(r, c, ch));
			else if (face === "ask") { eyes(4).forEach(([r, c, ch]) => put(r, c, ch)); put(10, 6, "e"); put(10, 7, "e"); }
			else if (face === "happy") [[8, 3], [7, 4], [8, 5], [8, 8], [7, 9], [8, 10]].forEach(([r, c]) => put(r, c, "e"));
			else [[8, 3], [8, 4], [8, 9], [8, 10]].forEach(([r, c]) => put(r, c, "e"));
			put(9, 2, "c"); put(9, 11, "c");
			(LOOK_ACCESSORIES[L.accessory] || []).forEach(([r, c, ch]) => put(r, c, ch));
			return at;
		}
		/** Mira's pixel scenes in the owner's look: 32 × 24 cells, two frames each, as [x, y, w, colour] runs. */
		function miraScene(state, look, count = 0) {
			const PAL = { k: "#3a3f47", b: "#1f2733", K: "#d8dbe0", E: "#a3a8b0", 1: "#5cc46a", 2: "#7cb8ff", 3: "#ff8fa8", C: "#30a46c", W: "#ffffff", B: "#eab308", n: "#4f3422", Z: "#8b8d98", r: "#e5484d", u: "#0090ff", p: "#8e4ec6", a: "#ffc53d", d: "rgba(0,0,0,.1)" };
			const frame = () => {
				const at = new Map();
				return { at,
					put(x, y, ch) { if (x >= 0 && x < 32 && y >= 0 && y < 24 && ch !== ".") at.set(y * 32 + x, PAL[ch] || ch); },
					map(x, y, rows) { rows.forEach((row, r) => [...row].forEach((ch, c) => this.put(x + c, y + r, ch))); },
					figure(x, y, face) { for (const [i, color] of lookCells(look, face)) this.put(x + i % 14, y + Math.floor(i / 14), color); } };
			};
			const frames = [frame(), frame()];
			frames.forEach((f, i) => {
				for (let x = 18; x <= 27; x++) f.put(x, 23, "d");
				if (state === "working") {
					f.map(2, 10, ["kkkkkkkkkkk", "kbbbbbbbbbk", "kbbbbbbbbbk", "kbbbbbbbbbk", "kbbbbbbbbbk", "kbbbbbbbbbk", "kbbbbbbbbbk", "kbbbbbbbbbk", "kkkkkkkkkkk", "....kkk...."]);
					(i === 0 ? [[1, 5], [2, 3], [3, 6]] : [[1, 6], [2, 4], [3, 3]]).forEach(([color, w], n) => { for (let x = 0; x < w; x++) f.put(4 + x + (n === 1 ? 1 : 0), 12 + n * 2, String(color)); });
					f.map(1, 20, ["KKKKKKKKKKKKKK", "EEEEEEEEEEEEEE"]);
					f.put(i === 0 ? 5 : 10, 20, "W");
					f.figure(16, i === 0 ? 11 : 10, "look");
				} else if (state === "done") {
					const lift = i === 0 ? 0 : 2;
					f.figure(16, 11 - lift, "happy");
					f.map(19, 3 - lift, [".CCCCC.", "CCCCCWC", "CCCCWCC", "CWCWCCC", "CCWCCCC", "CCCCCCC", ".CCCCC."]);
					(i === 0 ? [[8, 4, "r"], [12, 2, "u"], [28, 3, "p"], [30, 8, "a"], [10, 9, "1"], [5, 6, "B"]] : [[9, 6, "r"], [13, 4, "u"], [29, 5, "p"], [31, 10, "a"], [11, 11, "1"], [6, 8, "B"]])
						.forEach(([x, y, ch]) => { f.put(x, y, ch); f.put(x + 1, y, ch); });
				} else if (state === "waiting") {
					// Like Codex's pets: the figure alone, its face says it is waiting; no bubble or count over its head.
					f.figure(16, i === 0 ? 11 : 10, "ask");
				} else {
					f.figure(16, 11, "sleep");
					const Z = ["ZZZZ", "..Z.", ".Z..", "ZZZZ"];
					if (i === 0) f.map(25, 5, Z); else { f.map(26, 3, Z); f.put(30, 0, "Z"); f.put(31, 0, "Z"); }
				}
			});
			return frames.map(f => {
				const rows = [];
				for (let y = 0; y < 24; y++) for (let x = 0; x < 32; x++) {
					const color = f.at.get(y * 32 + x);
					if (!color) continue;
					let w = 1;
					while (x + w < 32 && f.at.get(y * 32 + x + w) === color) w++;
					rows.push([x, y, w, color]);
					x += w - 1;
				}
				return rows;
			});
		}
		/** The figure alone, 14 × 12, as [x, y, w, colour] runs: for portraits and template cards. */
		function lookPortrait(look, face = "ask") {
			const at = lookCells(look, face), rows = [];
			for (let y = 0; y < 12; y++) for (let x = 0; x < 14; x++) {
				const color = at.get(y * 14 + x);
				if (!color) continue;
				let w = 1;
				while (x + w < 14 && at.get(y * 14 + x + w) === color) w++;
				rows.push([x, y, w, color]);
				x += w - 1;
			}
			return rows;
		}
		const sceneCache = new Map();
		const SCENE_LABEL = { working: "在干活", done: "做完了", waiting: "等你确认", sleeping: "没在跑" };
		/** Mira in pixels doing what the work is doing: two frames, swapped in steps (one still frame when motion is reduced). */
		/** How far to shift a scene so what is drawn sits in the middle of its 32-cell width. */
		function sceneShift(frames) {
			let lo = 32, hi = 0;
			for (const frame of frames) for (const [x, , w] of frame) { lo = Math.min(lo, x); hi = Math.max(hi, x + w); }
			return hi > lo ? Math.round((lo + hi) / 2 - 16) : 0;
		}
		function MiraScene({ state, scale = 2, label, look, count = 0, center = false }) {
			const personal = usePersonal(), L = look || personal.look || null;
			const key = SCENE_LABEL[state] ? state : "sleeping", cacheKey = key + "|" + count + "|" + (L ? [L.color, L.shape, L.eyes, L.accessory].join(",") : "");
			if (!sceneCache.has(cacheKey)) { if (sceneCache.size > 64) sceneCache.clear(); sceneCache.set(cacheKey, miraScene(key, L, count)); }
			const draw = rows => rows.map(([x, y, w, c]) => h("rect", { key: x + "-" + y, x, y, width: w + 0.02, height: 1.02, fill: c }));
			const [a, b] = sceneCache.get(cacheKey);
			return h("svg", { className: "mp4-scene mp4-scene-" + key, viewBox: `${center ? sceneShift([a, b]) : 0} 0 32 24`, width: 32 * scale, height: 24 * scale, shapeRendering: "crispEdges", ...(label ? { role: "img", "aria-label": label } : { "aria-hidden": true }) },
				h("g", { className: "mp4-scene-a" }, draw(a)), h("g", { className: "mp4-scene-b" }, draw(b)));
		}
		/**
		 * The whole workspace's scene: a question waiting on the owner comes first (2026-11-18: a fresh delivery's green face
		 * hid a 等你拍板 for three minutes); then a task that just finished is celebrated for 3 minutes, then running, then waiting.
		 */
		function workspaceScene(tasks, personalRunning, now) {
			const asking = tasks.filter(t => !t.resolved && t.state === "blocked").length;
			if (asking) return { state: "waiting", title: `${asking} 项等你拍板` };
			const recent = tasks.filter(t => !t.resolved && (t.state === "review_ready" || t.state === "done") && now - (t.updatedAt || 0) < 3 * 60e3).length;
			const running = tasks.filter(t => t.experience && t.experience.execution && t.experience.execution.active).length;
			const waiting = tasks.filter(isWaiting).length;
			if (recent) return { state: "done", title: "刚做完一件" };
			if (running || personalRunning) return { state: "working", title: running ? `${running} 项进行中` : "个人助理在回复" };
			if (waiting) return { state: "waiting", title: `${waiting} 项待处理` };
			return { state: "sleeping", title: "都歇着" };
		}
		// ---------------------------------------------------------------- the assistant's look: studio and hotkey
		const LOOK_TRAIT_LIST = ["温和", "干脆", "幽默", "严谨", "好奇", "安静", "热情", "冷静", "细心", "直接", "乐观", "有条理"];
		const LOOK_TEMPLATES = [
			{ id: "sprout", name: "Tacit", color: "yellow", shape: "mochi", eyes: "dot", accessory: "sprout", traits: ["温和", "细心"] },
			{ id: "owl", name: "夜读", color: "purple", shape: "round", eyes: "dot", accessory: "glasses", traits: ["安静", "严谨"] },
			{ id: "spark", name: "火花", color: "orange", shape: "mochi", eyes: "star", accessory: "antenna", traits: ["热情", "幽默"] },
			{ id: "tidy", name: "方方", color: "teal", shape: "block", eyes: "dot", accessory: "cap", traits: ["有条理", "干脆"] },
			{ id: "calm", name: "耳机", color: "blue", shape: "round", eyes: "sleepy", accessory: "headphones", traits: ["冷静", "直接"] },
			{ id: "bow", name: "小结", color: "pink", shape: "mochi", eyes: "big", accessory: "bow", traits: ["乐观", "好奇"] },
		];
		const LOOK_LABELS = {
			color: { yellow: "黄", orange: "橙", red: "红", pink: "粉", purple: "紫", blue: "蓝", teal: "青", green: "绿", gray: "灰" },
			shape: { mochi: "团子", round: "圆球", block: "方块" },
			eyes: { dot: "豆豆眼", big: "大眼睛", sleepy: "眯眯眼", star: "亮晶晶" },
			accessory: { sprout: "小芽", bow: "蝴蝶结", cap: "帽子", glasses: "眼镜", headphones: "耳机", antenna: "天线", none: "不戴" },
		};
		const DEFAULT_HOTKEY = { code: "KeyM", alt: true, meta: false, ctrl: false, shift: false };
		const lookOf = t => ({ template: t.id, name: t.name, color: t.color, shape: t.shape, eyes: t.eyes, accessory: t.accessory, traits: [...t.traits], hotkey: { ...DEFAULT_HOTKEY }, pet: { visible: true, size: "small" } });
		/** Where a new assistant starts: the first pixel template (Tacit), the default name, no traits picked yet. */
		const BUDDY_START_OF = () => lookOf(LOOK_TEMPLATES[0]);
		/** A template counts as picked when the figure is exactly its figure. */
		const isTemplate = (look, t) => look.template === t.id || ["color", "shape", "eyes", "accessory"].every(k => look[k] === t[k]);
		/** The template that shares the most traits with the owner's picks (the first on a tie, none when nothing is picked). */
		function bestTemplate(traits) {
			let best = null, score = 0;
			for (const t of LOOK_TEMPLATES) { const n = t.traits.filter(x => traits.includes(x)).length; if (n > score) { best = t; score = n; } }
			return best;
		}
		const IS_MAC = typeof navigator !== "undefined" && /Mac|iPhone|iPad/.test(navigator.platform || navigator.userAgent || "");
		/** A key combination as people read it: "⌥ M" on a Mac, "Alt + M" elsewhere. */
		function hotkeyLabel(k, mac = IS_MAC) {
			if (!k) return "不用快捷键";
			const key = k.code === "Space" ? (mac ? "空格" : "Space") : k.code.startsWith("Key") ? k.code.slice(3) : k.code.startsWith("Digit") ? k.code.slice(5) : ({ Period: ".", Comma: ",", Slash: "/", Semicolon: ";", Quote: "'", Backquote: "`", BracketLeft: "[", BracketRight: "]" })[k.code] || k.code;
			const mods = mac ? [k.ctrl && "⌃", k.alt && "⌥", k.shift && "⇧", k.meta && "⌘"] : [k.ctrl && "Ctrl", k.alt && "Alt", k.shift && "Shift", k.meta && "Win"];
			return [...mods.filter(Boolean), key].join(mac ? " " : " + ");
		}
		const HOTKEY_CODE = /^(Key[A-Z]|Digit[0-9]|Space|Period|Comma|Slash|Semicolon|Quote|Backquote|BracketLeft|BracketRight)$/;
		/** A key press as a combination the summon key may use: a letter, digit or space with ⌘, ⌥ or ⌃; anything else is null. */
		function hotkeyFrom(e) {
			if (!HOTKEY_CODE.test(e.code || "") || !(e.altKey || e.metaKey || e.ctrlKey)) return null;
			return { code: e.code, alt: !!e.altKey, meta: !!e.metaKey, ctrl: !!e.ctrlKey, shift: !!e.shiftKey };
		}
		function hotkeyMatches(k, e) {
			return !!k && !e.repeat && e.code === k.code && !!e.altKey === !!k.alt && !!e.metaKey === !!k.meta && !!e.ctrlKey === !!k.ctrl && !!e.shiftKey === !!k.shift;
		}

		/** Who opens the studio and who answers the summon key: tiny module-level buses. */
		const studioBus = { open: false, subs: new Set(), recording: false };
		function openStudio() { studioBus.open = true; studioBus.subs.forEach(f => f()); }
		function closeStudio() { studioBus.open = false; studioBus.subs.forEach(f => f()); }
		const useStudioOpen = () => useSyncExternalStore(f => { studioBus.subs.add(f); return () => studioBus.subs.delete(f); }, () => studioBus.open, () => false);
		/**
		 * The summon key does one thing: open the assistant's own conversation with the cursor in its composer,
		 * the same box, model picker and streaming replies as always (the owner, 2026-11-17: no separate quick-ask).
		 */
		let assistantSessionId = null;
		async function summon() {
			const onScreen = shell.sidebar?.mounted?.getSnapshot?.();
			assistantSessionId = sharedFeed("api/mira-projects/personal", { loaded: false }).getSnapshot().sessionId || assistantSessionId;
			if (!assistantSessionId || onScreen !== assistantSessionId) await openMira();
			const focus = (tries) => { const editor = document.querySelector('[contenteditable="true"]'); if (editor) editor.focus(); else if (tries) setTimeout(() => focus(tries - 1), 150); };
			focus(10);
		}

		function MiraPortrait({ size = 20, look, face = "ask" }) {
			const personal = usePersonal(), L = look || personal.look || null;
			return h("svg", { viewBox: "0 0 14 12", width: size, height: Math.round(size * 12 / 14), shapeRendering: "crispEdges", "aria-hidden": true, className: "mp4-portrait" },
				lookPortrait(L, face).map(([x, y, w, c]) => h("rect", { key: x + "-" + y, x, y, width: w + 0.02, height: 1.02, fill: c })));
		}

		/** First run, or 助理设置 → 性格与快捷键: character, name and summon key, in two short steps. */
		function LookStudio({ initial, first, onClose }) {
			const BUDDY_START = BUDDY_START_OF();
			// First run starts with no traits picked: the owner's choice, not ours.
			const [draft, setDraft] = useState(() => initial ? { ...BUDDY_START, ...initial } : { ...BUDDY_START, traits: [] });
			const [step, setStep] = useState(0), [touched, setTouched] = useState(!!initial);
			const [sentence, setSentence] = useState(""), [busy, setBusy] = useState(null), [error, setError] = useState(null), [recording, setRecording] = useState(false);
			useEffect(() => {
				if (!recording) return;
				studioBus.recording = true;
				const onKey = e => {
					e.preventDefault(); e.stopPropagation();
					if (e.key === "Escape") { setRecording(false); return; }
					const k = hotkeyFrom(e);
					if (k) { setDraft(d => ({ ...d, hotkey: k })); setRecording(false); }
				};
				document.addEventListener("keydown", onKey, true);
				return () => { studioBus.recording = false; document.removeEventListener("keydown", onKey, true); };
			}, [recording]);
			// Traits are only traits: picking them no longer swaps the look and the name for a template's (it renamed people's assistants).
			const toggleTrait = t => {
				const traits = draft.traits.includes(t) ? draft.traits.filter(x => x !== t) : draft.traits.length >= 3 ? draft.traits : [...draft.traits, t];
				setDraft(d => ({ ...d, traits }));
			};
			const setLook = patch => { setTouched(true); setDraft(d => ({ ...d, template: null, ...patch })); };
			const generate = async () => {
				setBusy("gen"); setError(null);
				try { const r = await act("assistantLookDraft", { sentence }); setDraft(d => ({ ...r.look, hotkey: d.hotkey })); setTouched(true); }
				catch (e) { setError(e.message); } finally { setBusy(null); }
			};
			const save = async look => {
				setBusy("save"); setError(null);
				try { await act("assistantLook", { look }); onClose(); }
				catch (e) { setError(e.message); setBusy(null); }
			};
			// Every option shows what it looks like; its name is the tooltip. One selected style for all of them (DESIGN.md).
			const opt = (on, label, look, onClick, key, big) => h("button", { key, type: "button", className: "mp4-pick" + (big ? " mp4-pick-big" : "") + (on ? " mp4-pick-on" : ""), "aria-pressed": on, "aria-label": label, title: label, onClick }, h(MiraPortrait, { size: big ? 44 : 32, look }));
			const group = (label, body) => h("div", { className: "mp4-studio-grp" }, h("p", { className: "mp4-studio-q" }, label), body);
			const chip = (on, label, onClick, key) => h("button", { key: key || label, type: "button", className: "mp4-chip" + (on ? " mp4-chip-on" : ""), "aria-pressed": on, onClick }, label);
			const steps = [
				h("div", { key: 0, className: "mp4-studio-body" },
					group("样子", h("div", { className: "mp4-picks" }, LOOK_TEMPLATES.map(t => opt(isTemplate(draft, t), t.name, { ...t, template: null }, () => setLook({ template: t.id, color: t.color, shape: t.shape, eyes: t.eyes, accessory: t.accessory, name: draft.name === (LOOK_TEMPLATES.find(x => isTemplate(draft, x))?.name ?? "") ? t.name : draft.name }), t.id, true)))),
					group("颜色", h("div", { className: "mp4-swatches", role: "radiogroup", "aria-label": "颜色" }, Object.keys(LOOK_FILLS).map(c => h("button", { key: c, type: "button", role: "radio", "aria-checked": draft.color === c, "aria-label": LOOK_LABELS.color[c], title: LOOK_LABELS.color[c],
						className: "mp4-swatch" + (draft.color === c ? " mp4-swatch-on" : ""), style: { background: LOOK_FILLS[c], color: LOOK_FILLS[c] }, onClick: () => setLook({ color: c }) })))),
					h("div", { className: "mp4-studio-pair" },
						group("身体", h("div", { className: "mp4-picks" }, Object.keys(LOOK_LABELS.shape).map(k => opt(draft.shape === k, LOOK_LABELS.shape[k], { ...draft, template: null, shape: k, accessory: "none" }, () => setLook({ shape: k }), "s" + k)))),
						group("眼睛", h("div", { className: "mp4-picks" }, Object.keys(LOOK_LABELS.eyes).map(k => opt(draft.eyes === k, LOOK_LABELS.eyes[k], { ...draft, template: null, eyes: k, accessory: "none" }, () => setLook({ eyes: k }), "e" + k))))),
					group("配饰", h("div", { className: "mp4-picks" }, Object.keys(LOOK_LABELS.accessory).map(k => opt(draft.accessory === k, LOOK_LABELS.accessory[k], { ...draft, template: null, accessory: k }, () => setLook({ accessory: k }), "a" + k))))),
				h("div", { key: 1, className: "mp4-studio-body" },
					h("p", { className: "mp4-studio-q" }, "它是什么性格？最多选 3 个"),
					h("div", { className: "mp4-chips" }, LOOK_TRAIT_LIST.map(t => chip(draft.traits.includes(t), t, () => toggleTrait(t)))),
					h("p", { className: "mp4-studio-q" }, "或者一句话描述它"),
					h("form", { className: "mp4-studio-gen", onSubmit: e => { e.preventDefault(); if (sentence.trim()) generate(); } },
						h("input", { value: sentence, onChange: e => setSentence(e.target.value), placeholder: "比如：安静的夜猫子，说话简短", maxLength: 200, "aria-label": "一句话描述助理", disabled: busy === "gen" }),
						h("button", { type: "submit", className: "mp4-sug", disabled: !sentence.trim() || !!busy }, busy === "gen" ? "生成中…" : "生成")),
					h("div", { className: "mp4-studio-row" }, h("span", { className: "mp4-k" }, "呼出"),
						h("div", { className: "mp4-chips" },
							h("button", { type: "button", className: "mp4-key" + (recording ? " mp4-key-on" : ""), onClick: () => setRecording(true) }, recording ? "按下新的组合键…" : hotkeyLabel(draft.hotkey)),
							draft.hotkey ? h("button", { type: "button", className: "mp4-mem-link", onClick: () => setDraft(d => ({ ...d, hotkey: null })) }, "不用") : h("button", { type: "button", className: "mp4-mem-link", onClick: () => setDraft(d => ({ ...d, hotkey: { ...DEFAULT_HOTKEY } })) }, "用默认")))),
			];
			const titles = ["形象", "性格与呼出"];
			return h("div", { className: "mp4-studio-veil", onClick: e => { if (e.target === e.currentTarget && !busy) onClose(); } },
				h("div", { className: "mp4-studio", role: "dialog", "aria-modal": true, "aria-label": "捏一个你的助理" },
					h("div", { className: "mp4-studio-head" },
						h("b", null, first ? "捏一个你的助理" : "形象与快捷键"),
						h("div", { className: "mp4-studio-steps" }, titles.map((t, i) => h("button", { key: t, type: "button", className: "mp4-studio-step" + (i === step ? " mp4-studio-step-on" : ""), onClick: () => setStep(i) }, t))),
						h("button", { type: "button", className: "mp4-dock-x", "aria-label": "关闭", onClick: onClose, disabled: !!busy }, h(Icon, { name: "close", size: 14 }))),
					h("div", { className: "mp4-studio-main" },
						h("div", { className: "mp4-studio-stage" },
							h(MiraPortrait, { look: draft, size: 112 }),
							h("input", { className: "mp4-studio-name", value: draft.name, maxLength: 12, placeholder: "名字", onChange: e => setDraft(d => ({ ...d, name: e.target.value })), "aria-label": "助理的名字" }),
							draft.traits.length ? h("span", { className: "mp4-muted" }, draft.traits.join("、")) : null),
						steps[step]),
					error ? h("p", { className: "mp-error", role: "alert" }, error) : null,
					h("div", { className: "mp4-studio-foot" },
						first ? h("button", { type: "button", className: "mp4-mem-link", disabled: !!busy, onClick: () => save(BUDDY_START) }, "跳过") : h("span"),
						h("span", { className: "mp4-studio-acts" },
							step > 0 ? h("button", { type: "button", className: "mp4-sug", disabled: !!busy, onClick: () => setStep(step - 1) }, "上一步") : null,
							step < steps.length - 1 ? h("button", { type: "button", className: "mp4-sug mp4-sug-strong", disabled: !!busy, onClick: () => setStep(step + 1) }, "下一步")
								: h("button", { type: "button", className: "mp4-sug mp4-sug-strong", disabled: !!busy || !draft.name.trim(), onClick: () => save({ ...draft, name: draft.name.trim() }) }, busy === "save" ? "正在保存…" : "完成")))));
		}
		/** Shows the studio when asked, and once on first run when the assistant has no look yet. */
		/**
		 * The pet in the bottom-right corner (back on the owner's word, 2026-11-18 evening, after the morning's cut): the
		 * assistant's dot, its face telling what is going on; a bubble when a decision has waited over an hour. It opens the
		 * assistant. Hidden on the assistant's own page, and when the owner switches it off in 形象与快捷键.
		 */
		function MiraBall() {
			const hash = useRoute(), onScreen = useOnScreenSession(), nav = useSnapshot(), personal = usePersonal(), today = useToday();
			const [bubbleOff, setBubbleOff] = useState(() => { try { return window.sessionStorage.getItem("mira-projects:ball-bubble") === "off"; } catch (_) { return false; } });
			void hash;
			const visible = !(isHomeRoute() || (onScreen && onScreen === personal.sessionId));
			if (personal.sessionId) assistantSessionId = personal.sessionId;
			const pet = personal.look && personal.look.pet || { visible: true, size: "small" }, who = personal.look && personal.look.name || "个人助理";
			const keyNow = personal.look ? personal.look.hotkey : DEFAULT_HOTKEY;
			// The ball reserves its bottom-right corner in the right panel, so its last row is never under it.
			useEffect(() => {
				const root = document.documentElement;
				if (visible && pet.visible) root.setAttribute("data-mira-ball", pet.size === "large" ? "large" : "live"); else root.removeAttribute("data-mira-ball");
				return () => root.removeAttribute("data-mira-ball");
			}, [visible, pet.visible, pet.size]);
			if (!visible) return null;
			const scene = workspaceScene(allTasks(nav).map(x => x.t), personal.status === "running", Date.now());
			const decisions = (today.waiting?.decisions || []).filter(d => Date.now() - d.since > 60 * 60e3).sort((a, b) => a.since - b.since);
			const bubble = !bubbleOff && decisions[0] ? `「${decisions[0].threadTitle || "一个任务"}」等你拍板，等了 ${ago(decisions[0].since)}` : null;
			return h("div", { className: "mp4-ball-wrap" },
				bubble ? h("div", { className: "mp4-ball-bubble" }, h("button", { type: "button", onClick: () => summon() }, bubble), h("button", { type: "button", className: "mp4-icon", "aria-label": "先不用", onClick: () => { setBubbleOff(true); try { window.sessionStorage.setItem("mira-projects:ball-bubble", "off"); } catch (_) {} } }, "×")) : null,
				pet.visible ? h("button", { type: "button", className: "mp4-ball", "aria-label": `打开${who}`, title: `${who}：${scene.title}${keyNow ? `，${hotkeyLabel(keyNow)} 呼出` : ""}`, onClick: () => summon() },
					h(BuddyDot, { look: personal.look, face: scene.state, size: pet.size === "large" ? 52 : 40 })) : null);
		}
		function LookStudioHost() {
			const open = useStudioOpen(), personal = usePersonal(), hash = useRoute(), onScreen = useOnScreenSession();
			const [skipped, setSkipped] = useState(() => { try { return window.sessionStorage.getItem("mira-projects:studio-later") === "1"; } catch (_) { return false; } });
			void hash;
			const first = personal.loaded && personal.enabled !== false && !personal.error && personal.look === null && (isHomeRoute() || (!!onScreen && onScreen === personal.sessionId)) && !skipped;
			// On a new install the host shows its own notices first (preview note, sign-in). Ours waits its turn instead of stacking under them.
			const [hostBusy, setHostBusy] = useState(true);
			useEffect(() => {
				if (!first) return;
				const check = () => setHostBusy(Array.from(document.querySelectorAll('[role="dialog"], dialog[open]')).some(el => !el.classList.contains("mp4-studio")));
				check();
				const timer = window.setInterval(check, 600);
				return () => window.clearInterval(timer);
			}, [first]);
			if (!open && (!first || hostBusy)) return null;
			const close = () => { if (open) closeStudio(); else { setSkipped(true); try { window.sessionStorage.setItem("mira-projects:studio-later", "1"); } catch (_) {} } };
			return h(LookStudio, { initial: personal.look, first: !personal.look, onClose: close });
		}


		function executionRows(entries) {
			const mask = value => String(value || "").replace(/\bBearer\s+[A-Za-z0-9._~-]+/gi, "Bearer [已隐藏]").replace(/((?:api[_-]?key|password|secret|token|authorization|cookie)["']?\s*[:=]\s*["']?)[^\s"',;}]+/gi, "$1[已隐藏]");
			const text = content => mask((Array.isArray(content) ? content : []).filter(p => p.type === "text").map(p => p.text || "").join("\n"));
			const argumentsText = raw => {
				try { return mask(JSON.stringify(JSON.parse(raw), (key, value) => /^(?:headers?|authorization|cookie|.*(?:token|secret|password)|api[_-]?key)$/i.test(key) ? "[已隐藏]" : value, 2)); }
				catch (_) { return mask(raw); }
			};
			const rows = [];
			for (const entry of entries) {
				if (entry.type !== "event") continue;
				const e = entry.event, data = e.data;
				// Deliberately exclude request headers, provider config, tool-private meta,
				// reasoning streams and uncommitted live chunks from this task view.
				if (e.type === "user/message") rows.push({ id: e.seq, at: e.time, label: "会话收到的输入", text: text(data.content) });
				else if (e.type === "assistant/message") {
					const body = text(data.message?.content);
					if (body) rows.push({ id: e.seq, at: e.time, label: "执行者输出", text: body });
				} else if (e.type === "tool/call") rows.push({ id: e.seq, at: e.time, label: `调用工具，${data.name}`, code: argumentsText(data.arguments) });
				else if (e.type === "tool/result") rows.push({ id: e.seq, at: e.time, label: `工具返回，${data.message?.isError ? "失败" : "已返回"}`, text: text(data.message?.content) });
			}
			return rows;
		}

		function ExecutionTraceContent({ reference }) {
			const binding = reference.binding, source = binding.eventSource;
			const snapshot = useSyncExternalStore(fn => source.subscribe(fn), () => source.getSnapshot());
			const [loading, setLoading] = useState(false), [error, setError] = useState(null);
			const rows = executionRows(snapshot.entries);
			const loadOlder = async () => { setLoading(true); setError(null); try { await binding.session.loadOlder(); } catch (e) { setError(String(e?.message || e)); } finally { setLoading(false); } };
			return h("div", { className: "mp-trace-events" },
				snapshot.hasMore ? h(Btn, { disabled: loading, onClick: loadOlder }, loading ? "正在读取…" : "加载更早记录") : null,
				error ? h("p", { role: "alert", className: "mp-error" }, error) : null,
				rows.length ? rows.map(row => h("details", { key: row.id, className: "mp-trace-event", open: row.label === "会话收到的输入" }, h("summary", null, row.label, h("time", null, clock(row.at))), h("div", { className: "mp-markdown" }, h(P.MarkdownText, { text: row.code === undefined ? row.text : "```json\n" + row.code.replaceAll("```", "` ` `") + "\n```", labels: MARKDOWN_LABELS })))) : h("p", { className: "mp-hint" }, "当前会话还没有可显示的输入、工具操作或输出。"));
		}

		/** Use the public DSH Session feed and Markdown renderer without a second input channel. */
		function NativeExecutionTrace({ target, retainSession, onClose }) {
			const dialog = useRef(null), [reference, setReference] = useState(null), [error, setError] = useState(null);
			useEffect(() => {
				const previousFocus = document.activeElement;
				dialog.current?.showModal();
				let alive = true, ref;
				try {
					ref = retainSession(target);
					ref.ready.then(() => { if (alive) setReference(ref); }, e => { if (alive) setError(String(e?.message || e)); });
				} catch (e) { setError(String(e?.message || e)); }
				return () => { alive = false; ref?.release(); if (previousFocus?.isConnected) previousFocus.focus(); };
			}, []);
			return h("dialog", { ref: dialog, className: "mp-trace-dialog", "aria-label": "执行记录", onCancel: e => { e.preventDefault(); onClose(); } },
				h("div", { className: "mp-trace-header" }, h("div", null, h("strong", null, "执行记录"), h("p", { className: "mp-hint" }, "只读查看，继续任务请回到线程对话")), h(IconButton, { name: "close", label: "关闭执行记录", onClick: onClose })),
				h("div", { className: "mp-trace-body" }, error ? h("p", { role: "alert", className: "mp-error" }, `暂时无法读取记录：${error}`) : reference ? h(ExecutionTraceContent, { reference }) : h("p", { role: "status" }, "正在读取会话记录…")));
		}

		function ProjectsIcon({ size }) {
			const s = size || 20;
			return h("svg", { width: s, height: s, viewBox: "0 0 24 24", fill: "none", stroke: "currentColor", strokeWidth: 1.6, strokeLinecap: "round", strokeLinejoin: "round", "aria-hidden": true },
				h("path", { d: "M4 5.5A2.5 2.5 0 0 1 6.5 3h7A2.5 2.5 0 0 1 16 5.5v4A2.5 2.5 0 0 1 13.5 12H9l-3.2 2.6c-.3.25-.8.03-.8-.37V12.0A2.5 2.5 0 0 1 4 9.5z" }),
				h("path", { d: "M18.5 8.5h.0A1.5 1.5 0 0 1 20 10v4.5a2.5 2.5 0 0 1-2 2.45V19l-2.8-2.1H12a2 2 0 0 1-2-2" }));
		}



		// Raft Design System (raft-ui 0.56) tokens, scoped to the page: surfaces,
		// ink, hairlines, the status family, 4px stickers with a 1.5px ink edge,
		// Space Grotesk for what people say and Space Mono for what machines write.
		const CSS = `
/* The old page's tokens; its dialogs also open from DSH's right panel (Mira 设置, sources), outside .mp-page */
.mp-page,.mp-trace-dialog,.mp-observation-dialog,.mp-handoff-dialog{--sans:var(--dsw-font-family,-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Hiragino Sans GB","Microsoft YaHei","Helvetica Neue",Helvetica,Arial,sans-serif);--mono:var(--ds-font-family-code,"SF Mono","JetBrains Mono",Menlo,Consolas,monospace);
--fg:var(--dsw-alias-label-primary,rgb(15,17,21));--ink:var(--fg);--fg-muted:var(--dsw-alias-label-secondary,rgb(97,102,107));--fg-hint:var(--dsw-alias-label-tertiary,rgb(129,133,140));--fg-disabled:var(--dsw-alias-label-caption,rgb(173,178,184));
--canvas:var(--dsw-alias-bg-base,#fff);--card:var(--dsw-alias-bg-layer-1,#fff);--popover:var(--dsw-alias-bg-layer-3,#fff);--sidebar:rgb(249,250,251);
--fill-muted:var(--dsw-alias-markdown-tag,rgb(241,243,245));--fill-subtle:rgba(38,49,72,.03);--fill-medium:var(--dsw-alias-interactive-bg-hover,rgba(38,49,72,.06));--fill-active:var(--dsw-alias-interactive-bg-active,rgba(38,49,72,.1));
--hair:var(--dsw-alias-border-l2,rgba(0,0,0,.1));--seam:var(--dsw-alias-border-l3,rgba(0,0,0,.12));--strong:var(--dsw-alias-border-l4,rgba(0,0,0,.16));--edge:transparent;
--accent:var(--dsw-alias-state-business-primary,rgb(65,118,230));--accent-soft:var(--dsw-alias-state-business-tertiary,rgb(228,237,253));--on-accent:#fff;
--selected:var(--fill-active);--brand:var(--accent);--attention:var(--accent);--attention-strong:var(--accent);--attention-soft:var(--accent-soft);
--danger-strong:var(--dsw-alias-state-error-primary,rgb(236,19,19));--float:0 0 0 1px var(--hair),0 12px 32px -12px rgba(15,17,21,.24);
--s-cyan:var(--accent);--s-cyan-soft:var(--accent-soft);
--s-pink:var(--dsw-alias-state-warn-primary,rgb(245,158,11));--s-pink-soft:var(--dsw-alias-state-warn-tertiary,rgb(254,245,231));
--s-lav:var(--dsw-alias-state-success-primary,rgb(34,197,94));--s-lav-soft:var(--dsw-alias-state-success-tertiary,rgb(230,250,237));
--s-lime:var(--s-lav);--s-lime-soft:var(--s-lav-soft);
--s-orange:var(--dsw-alias-state-error-secondary,rgb(242,90,90));--s-orange-soft:rgba(242,90,90,.1);
--s-stone:rgb(173,178,184);--s-stone-soft:var(--fill-muted);
--s-yellow:rgb(247,173,49);--s-yellow-soft:var(--s-pink-soft);
--lavender:var(--fill-muted)}
@media (prefers-color-scheme:dark){html:not([data-theme="light"]) :is(.mp-page,.mp-trace-dialog,.mp-observation-dialog,.mp-handoff-dialog){--fg:var(--dsw-alias-label-primary,rgb(249,250,251));--fg-muted:var(--dsw-alias-label-secondary,rgb(207,211,214));--fg-hint:var(--dsw-alias-label-tertiary,rgb(173,178,184));--fg-disabled:var(--dsw-alias-label-caption,rgb(129,133,140));
--canvas:var(--dsw-alias-bg-base,rgb(21,21,23));--card:var(--dsw-alias-bg-layer-1,rgb(35,35,36));--popover:var(--dsw-alias-bg-layer-3,rgb(53,54,56));--sidebar:rgb(27,27,28);
--fill-muted:var(--dsw-alias-markdown-tag,rgb(44,44,46));--fill-subtle:rgba(255,255,255,.03);--fill-medium:var(--dsw-alias-interactive-bg-hover,rgba(255,255,255,.08));--fill-active:var(--dsw-alias-interactive-bg-active,rgba(255,255,255,.14));
--hair:var(--dsw-alias-border-l2,rgba(255,255,255,.12));--seam:var(--dsw-alias-border-l3,rgba(255,255,255,.16));--strong:var(--dsw-alias-border-l4,rgba(255,255,255,.2));
--accent:var(--dsw-alias-state-business-primary,rgb(103,158,254));--accent-soft:var(--dsw-alias-state-business-tertiary,rgb(52,65,91));
--danger-strong:var(--dsw-alias-state-error-primary,rgb(242,90,90));--float:0 0 0 1px var(--hair),0 16px 40px -12px rgba(0,0,0,.8);
--s-pink-soft:var(--dsw-alias-state-warn-tertiary,rgb(39,36,31));--s-lav-soft:var(--dsw-alias-state-success-tertiary,rgb(35,60,44));--s-orange-soft:rgba(242,90,90,.15)}}
html[data-theme="dark"] :is(.mp-page,.mp-trace-dialog,.mp-observation-dialog,.mp-handoff-dialog),html.dark :is(.mp-page,.mp-trace-dialog,.mp-observation-dialog,.mp-handoff-dialog){--fg:var(--dsw-alias-label-primary,rgb(249,250,251));--fg-muted:var(--dsw-alias-label-secondary,rgb(207,211,214));--fg-hint:var(--dsw-alias-label-tertiary,rgb(173,178,184));--fg-disabled:var(--dsw-alias-label-caption,rgb(129,133,140));
--canvas:var(--dsw-alias-bg-base,rgb(21,21,23));--card:var(--dsw-alias-bg-layer-1,rgb(35,35,36));--popover:var(--dsw-alias-bg-layer-3,rgb(53,54,56));--sidebar:rgb(27,27,28);
--fill-muted:var(--dsw-alias-markdown-tag,rgb(44,44,46));--fill-subtle:rgba(255,255,255,.03);--fill-medium:var(--dsw-alias-interactive-bg-hover,rgba(255,255,255,.08));--fill-active:var(--dsw-alias-interactive-bg-active,rgba(255,255,255,.14));
--hair:var(--dsw-alias-border-l2,rgba(255,255,255,.12));--seam:var(--dsw-alias-border-l3,rgba(255,255,255,.16));--strong:var(--dsw-alias-border-l4,rgba(255,255,255,.2));
--accent:var(--dsw-alias-state-business-primary,rgb(103,158,254));--accent-soft:var(--dsw-alias-state-business-tertiary,rgb(52,65,91));
--danger-strong:var(--dsw-alias-state-error-primary,rgb(242,90,90));--float:0 0 0 1px var(--hair),0 16px 40px -12px rgba(0,0,0,.8);
--s-pink-soft:var(--dsw-alias-state-warn-tertiary,rgb(39,36,31));--s-lav-soft:var(--dsw-alias-state-success-tertiary,rgb(35,60,44));--s-orange-soft:rgba(242,90,90,.15)}
.mp-page{display:grid;grid-template-columns:248px minmax(0,1fr);width:100%;height:100%;min-width:0;min-height:0;overflow:hidden;color:var(--fg);background:var(--canvas);font-family:var(--sans);font-size:14px;line-height:24px;position:relative;-webkit-font-smoothing:antialiased}
.mp-page.mp-with-thread{grid-template-columns:minmax(300px,360px) minmax(0,1fr)}
.mp-page.mp-no-list{grid-template-columns:minmax(0,1fr)}
.mp-page.mp-no-list.mp-with-thread{grid-template-columns:minmax(300px,360px) minmax(0,1fr)}
.mp-page.mp-with-thread.mp-single-context{grid-template-columns:minmax(0,1fr)}
.mp-project-pane{display:flex;flex-direction:column;min-width:0;min-height:0}
.mp-project-pane>.mp-chat{flex:1}
.mp-project-pane[hidden],.mp-thread[hidden]{display:none}
.mp-project-context-bar{display:flex;align-items:center;justify-content:space-between;gap:8px;padding:8px 12px;background:var(--fill-subtle);border-bottom:.5px solid var(--hair);font-size:14px;color:var(--fg-muted);line-height:24px}
.mp-project-context-bar>span{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.mp-context-toggle{all:unset;box-sizing:border-box;display:inline-flex;align-items:center;justify-content:center;gap:4px;flex:none;cursor:pointer;min-height:30px;padding:4px 8px;border:.5px solid var(--hair);border-radius:8px;font-size:14px;line-height:24px;color:var(--fg-muted);background:var(--canvas)}
.mp-context-toggle:hover{background:var(--fill-medium);color:var(--fg)}
.mp-context-toggle:focus-visible,.mp-thread-project-link:focus-visible{outline:2px solid var(--accent);outline-offset:2px}
.mp-page *{box-sizing:border-box}
.mp-page button{font-family:inherit}
.mp-center{display:flex;align-items:center;justify-content:center}
.mp-mono{font-family:var(--mono);font-size:14px;line-height:24px;letter-spacing:0;font-variant-numeric:tabular-nums}
.mp-hint{color:var(--fg-hint);font-size:14px;line-height:24px}
.mp-ellipsis{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;min-width:0}
.mp-clamp{display:-webkit-box;-webkit-line-clamp:3;-webkit-box-orient:vertical;overflow:hidden}
.mp-right{margin-left:auto}
.mp-welcome{max-width:480px;width:100%;padding:24px}
.mp-welcome h1{font-size:16px;line-height:24px;margin:0 0 8px;font-weight:600}
.mp-welcome p{color:var(--fg-muted);margin:0 0 16px}
/* Personal Mira keeps the conversation primary and its context available on demand. */
.mp-home{display:flex;flex-direction:column;position:relative;min-width:0;min-height:0}.mp-home-head{gap:12px}.mp-home-head h1,.mp-home-head p{margin:0}.mp-home-loading{padding:28px}.mp-home-context{flex:none;padding:12px 24px;border-bottom:.5px solid var(--hair);font-size:14px;line-height:24px}.mp-home-context>summary{cursor:pointer;color:var(--fg-muted)}.mp-home-context-body{max-height:250px;overflow:auto;padding-top:12px}.mp-home-projects{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(220px,100%),1fr));gap:8px;margin-bottom:8px}.mp-home-projects>button{all:unset;box-sizing:border-box;display:flex;flex-direction:column;gap:8px;padding:12px;border:.5px solid var(--hair);border-radius:8px;cursor:pointer}.mp-home-projects>button:hover{background:var(--fill-medium)}.mp-home-activity{all:unset;box-sizing:border-box;display:flex;gap:16px;padding:8px 0;width:100%;cursor:pointer}.mp-home-activity>span:last-child{flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.mp-home-scroll{flex:1;min-height:0;overflow:auto;overflow-anchor:none}.mp-home-column{max-width:880px;margin:auto;padding:24px 24px 32px}.mp-personal-welcome{padding:40px 0}.mp-personal-welcome h2{font-size:16px;font-weight:600;line-height:1.4}.mp-personal-welcome p{color:var(--fg-hint);font-size:14px;margin:12px 0 24px;line-height:24px}.mp-personal-welcome .mp-btn{margin:0 8px 8px 0}.mp-personal-message{padding:12px 0 24px;line-height:1.65}.mp-personal-message-head{display:flex;gap:12px;align-items:center;margin-bottom:8px;font-size:14px;line-height:24px}.mp-personal-message-head time{font-size:14px;color:var(--fg-hint);line-height:24px}.mp-personal-owner>.mp-markdown{padding:12px 16px;border-radius:8px;background:var(--fill-muted)}.mp-personal-message-actions{display:flex;gap:12px;flex-wrap:wrap;padding-top:8px}.mp-source-link{all:unset;box-sizing:border-box;cursor:pointer;color:var(--accent);font-size:14px;line-height:24px;padding:4px 0}.mp-source-link:hover{text-decoration:underline}.mp-source-link:focus-visible,.mp-workspace-home:focus-visible{outline:2px solid var(--accent);outline-offset:3px}.mp-home-composer{flex:none;width:min(100%,928px);box-sizing:border-box;padding:0 24px 20px;margin:0 auto}.mp-personal-status{display:flex;align-items:center;gap:8px;font-size:14px;color:var(--fg-hint);line-height:24px}.mp-personal-jump{all:unset;cursor:pointer;align-self:center;background:var(--card);border:.5px solid var(--hair);border-radius:8px;padding:8px 16px;margin:0 0 8px;font-size:14px;color:var(--fg-muted);line-height:24px}.mp-home-error{margin:0;padding:8px 20px;font-size:14px;color:var(--danger-strong);line-height:24px}.mp-workspaces .mp-workspace-home{display:flex;align-items:center;gap:8px;padding:8px;width:100%;font-size:14px;line-height:24px}.mp-workspace-home[aria-current=page]{background:var(--dsw-alias-bg-secondary);border-radius:8px}.mp-handoff-receipt{all:unset;box-sizing:border-box;cursor:pointer;display:block;padding:8px 0;font-size:14px;color:var(--fg-muted);line-height:24px}.mp-handoff-receipt:hover{color:var(--accent)}.mp-source-body{overflow:auto;min-height:0;padding:20px 28px;font-size:14px;line-height:1.7}.mp-source-meta{font-weight:500}.mp-source-actions{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-top:24px}.mp-handoff-dialog{box-sizing:border-box;width:min(620px,calc(100vw - 32px));max-width:none;max-height:calc(100dvh - 32px);padding:0;border:.5px solid var(--hair);border-radius:8px;background:var(--canvas);color:var(--fg);overflow:auto}.mp-handoff-dialog::backdrop{background:#0005}.mp-handoff-form{display:flex;flex-direction:column;gap:16px;padding:20px 24px}.mp-handoff-form label{display:flex;flex-direction:column;gap:8px;font-size:14px;line-height:24px}.mp-handoff-form select,.mp-handoff-form textarea{box-sizing:border-box;width:100%;padding:12px 12px;border:.5px solid var(--hair);border-radius:8px;background:var(--canvas);color:var(--fg);font:inherit;line-height:1.6}.mp-handoff-form p{margin:0;font-size:14px;line-height:1.65}.mp-handoff-form>.mp-btn{align-self:flex-end}
.mp-observation-dialog{box-sizing:border-box;width:min(1080px,calc(100vw - 32px));height:min(90dvh,1000px);max-width:none;max-height:90dvh;padding:0;border:.5px solid var(--hair);border-radius:12px;background:var(--canvas);color:var(--fg);overflow:hidden}.mp-observation-dialog[open]{display:flex;flex-direction:column}.mp-observation-dialog::backdrop{background:#0005}.mp-observation-scroll{min-height:0;overflow:auto;padding:20px 24px;flex:1}.mp-observation-filters{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px}.mp-observation-filters label,.mp-observation-policy-grid label,.mp-observation-exclude{display:flex;flex-direction:column;gap:8px;font-size:14px;line-height:24px}.mp-observation-filters select,.mp-observation-filters input,.mp-observation-policy input[type=number],.mp-observation-policy textarea{box-sizing:border-box;width:100%;min-width:0;border:.5px solid var(--hair);border-radius:8px;background:var(--canvas);color:var(--fg);padding:8px;font:inherit}.mp-observation-item{all:unset;box-sizing:border-box;width:100%;display:flex;align-items:center;gap:16px;padding:16px 0;cursor:pointer;border-bottom:.5px solid var(--hair)}.mp-observation-item:focus-visible{outline:2px solid var(--accent);outline-offset:3px}.mp-observation-item>img,.mp-observation-no-image{width:128px;height:82px;flex:none;border-radius:8px;object-fit:contain;background:var(--fill-muted)}.mp-observation-no-image{display:flex;align-items:center;justify-content:center;color:var(--fg-hint);font-size:14px;line-height:24px}.mp-observation-item-main{display:flex;flex-direction:column;gap:8px;min-width:0;flex:1}.mp-observation-item-main>span:last-child{display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden;font-size:14px;line-height:1.6}.mp-observation-item-main strong{font-size:14px;line-height:24px}.mp-observation-device{padding:20px;border:.5px solid var(--hair);border-radius:12px;margin:16px 0}.mp-observation-device-head,.mp-observation-device-actions{display:flex;gap:12px;align-items:center;flex-wrap:wrap}.mp-observation-device-head strong{margin-right:auto}.mp-observation-metrics{display:flex;gap:8px 20px;flex-wrap:wrap;font-size:14px;line-height:1.7;padding:12px 0;color:var(--fg-muted)}.mp-observation-policy{display:flex;flex-direction:column;gap:16px;margin-top:12px}.mp-observation-check{display:flex;align-items:center;gap:8px;font-size:14px;line-height:24px}.mp-observation-policy-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:16px}.mp-observation-picture{margin:20px 0}.mp-observation-picture img{display:block;max-width:100%;max-height:55dvh;object-fit:contain;margin:auto;border:.5px solid var(--hair);border-radius:8px}.mp-observation-picture figcaption{font-size:14px;color:var(--fg-hint);text-align:center;padding-top:12px;line-height:24px}.mp-observation-source-refs{font-size:14px;padding:12px 0;line-height:24px}.mp-observation-source-refs summary{cursor:pointer}.mp-observation-policy p{margin:0;line-height:1.6}
@media(max-width:600px){.mp-observation-dialog{width:100vw;height:100dvh;max-height:100dvh;border-radius:0;border:0}.mp-observation-scroll{padding:16px}.mp-observation-filters,.mp-observation-policy-grid{grid-template-columns:repeat(2,minmax(0,1fr))}.mp-observation-device{padding:12px}.mp-observation-item>img,.mp-observation-no-image{width:84px;height:64px}.mp-observation-item{gap:12px}.mp-home-head{gap:8px}.mp-home-head .mp-head-text{min-width:0}.mp-home-head .mp-head-meta{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
@media(max-width:900px){.mp-home-column{padding:16px}.mp-home-composer{padding:0 12px 12px}.mp-home-context{padding:12px 16px}.mp-home-activity{flex-direction:column;gap:4px}.mp-source-body{padding:16px}.mp-handoff-dialog{width:calc(100vw - 24px)}}

.mp-trace-dialog.mp-connectors-dialog{width:min(820px,calc(100vw - 32px));height:auto;max-height:calc(100dvh - 48px)}.mp-connector-sources{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(290px,100%),1fr));gap:12px;margin-top:20px}.mp-connector-source{padding:16px;border:.5px solid var(--hair);border-radius:12px;min-width:0}.mp-connector-source-head{display:flex;gap:12px;align-items:center;justify-content:space-between}.mp-connector-source p{margin:8px 0}.mp-connector-source .mp-source-actions{margin-top:16px;justify-content:flex-start}.mp-connector-recent-title{font-size:14px;margin:28px 0 12px}.mp-connector-record{all:unset;box-sizing:border-box;display:flex;flex-direction:column;gap:4px;width:100%;padding:16px 0;border-bottom:.5px solid var(--hair);font-size:14px;cursor:pointer;overflow-wrap:anywhere;line-height:24px}.mp-connector-record:hover{color:var(--accent)}.mp-connector-record:focus-visible{outline:2px solid var(--accent);outline-offset:4px}.mp-head.mp-home-head{flex-wrap:wrap;height:auto;min-height:56px;box-sizing:border-box;padding-top:12px;padding-bottom:12px}
/* avatars: pixel slimes for agents (colour = model), Raft's lavender square with initials for people; presence is a still square light */
.mp-av{position:relative;display:inline-grid;place-items:center;flex:none;line-height:1}
.mp-av-svg{display:block;overflow:visible;filter:drop-shadow(0 0 0 var(--edge))}
.mp-av-human{border-radius:50%;background:var(--fill-active);color:var(--fg-muted);font-weight:500}
.mp-light{position:absolute;right:-3px;bottom:-3px;width:10px;height:10px;border-radius:50%;background:var(--s-stone);box-shadow:0 0 0 2px var(--canvas)}
.mp-av[data-size="sm"] .mp-light{width:9px;height:9px;right:-3px;bottom:-3px}
.mp-av[data-size="xs"] .mp-light,.mp-av[data-size="2xs"] .mp-light{width:7px;height:7px;right:-2px;bottom:-2px;border-width:1px}
.mp-light-online{background:var(--s-lav)}.mp-light-busy{background:var(--accent)}.mp-light-error{background:var(--s-orange)}.mp-light-offline{background:var(--s-stone)}.mp-light-external{background:var(--accent)}
.mp-model{font-size:14px;line-height:24px;color:var(--fg-hint);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;min-width:0}
.mp-faces{display:inline-flex;align-items:center}
.mp-faces .mp-av+.mp-av{margin-left:-4px}
.mp-faces-more{margin-left:8px;font-size:14px;color:var(--fg-hint);line-height:24px}
/* Raft Status and status chips */
.mp-status{display:inline-block;flex:none;width:8px;height:8px;border-radius:50%;background:var(--s-stone)}
.mp-status-sm{width:6px;height:6px}
.mp-status-cyan{background:var(--s-cyan)}.mp-status-pink{background:var(--s-pink)}.mp-status-lav{background:var(--s-lav)}.mp-status-lime{background:var(--s-lime)}.mp-status-orange{background:var(--s-orange)}.mp-status-stone{background:var(--s-stone)}.mp-status-yellow{background:var(--s-yellow)}
.mp-chip{display:inline-flex;align-items:center;gap:4px;height:20px;padding:0 8px;border-radius:var(--mp4-r-ctl);white-space:nowrap;font-size:14px;line-height:24px;font-weight:500;color:var(--fg-muted);background:var(--s-stone-soft);flex:none}
.mp-chip .mp-status{width:6px;height:6px}
.mp-tone-cyan{background:var(--s-cyan-soft)}.mp-tone-pink{background:var(--s-pink-soft)}.mp-tone-lav{background:var(--s-lav-soft)}.mp-tone-lime{background:var(--s-lime-soft)}.mp-tone-orange{background:var(--s-orange-soft)}.mp-tone-stone{background:var(--s-stone-soft)}.mp-tone-yellow{background:var(--s-yellow-soft)}
.mp-count{display:inline-grid;place-items:center;min-width:18px;height:18px;padding:0 4px;border-radius:999px;background:var(--accent);color:var(--on-accent);font-size:14px;font-weight:500;line-height:1;flex:none}
/* Raft Button: outline 1.5px ink, 8px radius; ghost; the pink primary only for the one main action */
.mp-btn{all:unset;box-sizing:border-box;display:inline-flex;align-items:center;justify-content:center;gap:8px;height:32px;padding:0 12px;border-radius:8px;border:.5px solid var(--hair);background:var(--card);color:var(--fg);font-family:var(--sans);font-size:14px;font-weight:500;line-height:1;cursor:pointer;white-space:nowrap}
.mp-btn:hover{background:var(--fill-medium)}
.mp-btn:focus-visible{outline:2px solid var(--strong);outline-offset:2px}
.mp-btn:disabled{opacity:.5;cursor:default}
.mp-btn-strong{border-color:var(--accent);color:var(--accent);background:var(--accent-soft)}
.mp-btn-strong:hover{background:color-mix(in srgb,var(--accent-soft) 85%,var(--accent))}
.mp-btn-strong em{font-style:normal;font-size:14px;font-weight:500;line-height:24px;padding:0 4px;border-radius:999px;background:var(--accent);color:var(--on-accent)}
.mp-btn-quiet{border-color:transparent;background:transparent;color:var(--fg-muted)}
.mp-btn-quiet:hover{color:var(--fg)}
.mp-btn-primary{background:var(--accent);border-color:var(--accent);color:var(--on-accent)}
.mp-btn-primary:hover{background:color-mix(in srgb,var(--accent) 90%,#000)}
.mp-ib{all:unset;box-sizing:border-box;display:inline-grid;place-items:center;width:28px;height:28px;border-radius:8px;color:var(--fg-muted);cursor:pointer;flex:none}
.mp-ib:hover{background:var(--fill-medium);color:var(--fg)}
.mp-ib-on{color:var(--fg);background:var(--fill-active)}
.mp-linkish{all:unset;cursor:pointer}
.mp-linkish:hover{text-decoration:underline;text-underline-offset:3px}
.mp-x{all:unset;display:inline-grid;place-items:center;width:20px;height:20px;border-radius:8px;color:var(--fg-hint);cursor:pointer;opacity:0}
.mp-mem-item:hover .mp-x{opacity:1}.mp-x:hover{background:var(--fill-medium);color:var(--fg)}
/* Raft sidebar */
.mp-list{background:var(--sidebar);display:flex;flex-direction:column;min-height:0}
.mp-list-head{display:flex;align-items:center;justify-content:space-between;padding:8px 12px 0;font-size:14px;color:var(--fg-muted);line-height:24px}
.mp-nav-toggle{all:unset;display:inline-flex;align-items:center;gap:4px;flex:none;cursor:pointer;border-radius:8px;padding:8px;color:var(--fg-muted)}
.mp-nav-toggle:hover{background:var(--fill-medium)}
.mp-nav-toggle span{display:none}
.mp-nav-overlay>.mp-list{position:absolute;inset:0 auto 0 0;width:min(288px,calc(100% - 48px));z-index:12;box-shadow:var(--float)}
.mp-nav-scrim{position:absolute;inset:0;border:0;background:rgba(0,0,0,.16);z-index:11;cursor:pointer}
.mp-nav-toggle:focus-visible,.mp-workspaces button:focus-visible,.mp-sidebar-switch:focus-visible{outline:2px solid var(--dsw-alias-state-business-primary,var(--rx-blue-9));outline-offset:1px}
/* Scoped workspace browser uses DSH tokens, with no vertical divider or rail. */
.mp-workspaces{padding:0;overflow:auto;min-height:0;flex:1;font-size:14px;color:var(--dsw-alias-label-primary);font-family:var(--dsw-font-family,inherit);line-height:24px}
.mp-workspaces svg,.mp-sidebar-switch svg{stroke-width:1.6}
.mp-workspaces-rail{display:flex;flex-direction:column;align-items:center}
.mp-workspaces button{all:unset;box-sizing:border-box;cursor:pointer;border-radius:8px;display:flex;align-items:center;gap:8px;min-height:32px;min-width:0}
.mp-workspaces button:hover{background:var(--dsw-alias-interactive-bg-hover)}
.mp-workspaces-head{display:flex;align-items:center;justify-content:space-between;height:36px;color:var(--dsw-alias-label-tertiary);padding:0 0 0 var(--dsh-sidebar-row-padding,12px)}
.mp-workspaces-acts{display:inline-flex;align-items:center;gap:4px}
.mp-workspaces-head button{justify-content:center;width:28px;height:28px;min-height:28px;color:var(--dsw-alias-label-secondary,var(--dsw-alias-label-tertiary))}
.mp-workspaces-head button:hover{color:var(--dsw-alias-label-primary)}
.mp-sidebar-switch{all:unset;box-sizing:border-box;cursor:pointer;display:flex;align-items:center;gap:var(--dsh-sidebar-label-gap,8px);width:100%;height:36px;padding:0 calc(var(--dsh-sidebar-row-padding,12px) - var(--dsh-sidebar-paint-inset,0px));border-inline:var(--dsh-sidebar-paint-inset,0px) solid transparent;border-radius:12px;background-clip:padding-box;color:var(--dsw-alias-label-primary);font-family:var(--dsw-font-family,inherit);font-size:14px;line-height:24px}
.mp-sidebar-switch:hover{background:var(--dsw-alias-interactive-bg-hover)}
.mp-sidebar-switch>span{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.mp-sidebar-switch-rail{width:36px;height:36px;min-height:36px;padding:0;border-inline:0;justify-content:center;color:var(--dsw-alias-label-primary)}
.mp-workspace-row{display:flex;align-items:center;gap:4px}
.mp-workspaces .mp-workspace-disclosure{justify-content:center;width:18px;flex:none;color:var(--dsw-alias-label-tertiary)}
.mp-workspace-disclosure[aria-expanded="true"] svg{transform:rotate(90deg)}
.mp-workspaces .mp-workspace-project{flex:1;padding:4px calc(var(--dsh-sidebar-row-padding,12px) - var(--dsh-sidebar-paint-inset,0px));border-inline:var(--dsh-sidebar-paint-inset,0px) solid transparent;background-clip:padding-box}
.mp-workspaces .mp-workspace-thread{width:100%;padding:4px calc(var(--dsh-sidebar-row-padding,12px) - var(--dsh-sidebar-paint-inset,0px));border-inline:var(--dsh-sidebar-paint-inset,0px) solid transparent;background-clip:padding-box}
.mp-workspace-project>span,.mp-workspace-thread>span{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.mp-workspace-threads{padding-left:24px;margin:4px 0 8px}
.mp-workspaces .mp-workspace-selected{background:var(--dsw-alias-interactive-bg-active);font-weight:500}
.mp-workspaces .mp-workspace-within{background:var(--dsw-alias-interactive-bg-hover);font-weight:500}
.mp-workspace-empty{padding:8px var(--dsh-sidebar-row-padding,12px);font-size:14px;color:var(--dsw-alias-label-tertiary);line-height:24px}
.mp-workspace-group-label,.mp-workspace-archive>summary{padding:8px 8px 4px;font-size:14px;line-height:24px;color:var(--dsw-alias-label-tertiary)}.mp-workspace-archive>summary{cursor:pointer}
.mp-workspace-dot{width:6px;height:6px;background:var(--dsw-alias-label-caption,var(--rx-gray-9));border-radius:50%;flex:none}
.mp-workspace-working{background:var(--dsw-alias-state-business-primary,var(--rx-blue-9))}
.mp-workspace-blocked{background:var(--dsw-alias-state-warn-primary,var(--rx-amber-9))}
.mp-workspace-review_ready,.mp-workspace-done{background:var(--dsw-alias-state-success-primary,var(--rx-green-9))}
.mp-workspaces .mp-workspace-project>span{flex:1}
.mp-workspace-meta{flex:none;font-size:14px;color:var(--dsw-alias-label-tertiary);line-height:24px}
.mp-workspace-meta-wait{color:var(--rx-yellow-11)}
.mp-workspaces .mp-workspace-recent{align-items:flex-start;padding:8px calc(var(--dsh-sidebar-row-padding,12px) - var(--dsh-sidebar-paint-inset,0px))}
.mp-workspace-recent .mp-workspace-dot{margin-top:8px}
.mp-workspace-recent>span{display:flex;flex-direction:column;min-width:0}
.mp-workspace-recent-title,.mp-workspace-recent-meta{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.mp-workspace-recent-meta{font-size:14px;line-height:24px;color:var(--dsw-alias-label-tertiary)}
.mp-workspaces .mp-workspaces-head:not(:first-of-type){margin-top:12px}
.mp-task-tab{padding:16px;display:flex;flex-direction:column;gap:16px;font-size:14px;line-height:24px;color:var(--dsw-alias-label-primary);overflow:auto;height:100%;box-sizing:border-box}
.mp-task-tab-head{display:flex;align-items:center;gap:8px;font-size:14px;line-height:24px}
.mp-task-tab-title{margin:0;font-size:14px;line-height:24px;font-weight:600}
.mp-task-tab-grid{display:flex;flex-direction:column;gap:8px}
.mp-task-tab-row{display:grid;grid-template-columns:48px minmax(0,1fr);gap:8px}
.mp-task-tab-row>span:last-child{white-space:pre-wrap;overflow-wrap:anywhere}
.mp-task-tab-check>div{padding:4px 0}
.mp-task-tab-done{color:var(--dsw-alias-label-tertiary)}
.mp-task-tab-accept{display:flex;flex-direction:column;gap:8px;padding:12px;border:.5px solid var(--dsw-alias-border-primary,rgba(0,0,0,.1));border-radius:8px}
.mp-task-tab-accept p{margin:0}
.mp-task-tab-foot{padding-top:4px}
.mp-list-items{overflow:auto;padding:12px 8px 8px;display:flex;flex-direction:column;gap:0px;flex:1;min-height:0}
.mp-sec{display:flex;align-items:center;gap:4px;padding:0 0 0 4px;margin:4px 0 4px}
.mp-sec-toggle{all:unset;box-sizing:border-box;display:flex;align-items:center;gap:8px;flex:1;min-width:0;height:28px;padding:0 8px;border-radius:8px;cursor:pointer;font-size:14px;font-weight:500;color:var(--fg-muted)}
.mp-sec-toggle:hover{background:var(--fill-medium);color:var(--fg)}
.mp-sec-count{color:var(--fg-hint)}
.mp-sec-chev{display:inline-grid;place-items:center;color:var(--fg-hint);transition:transform .15s}
.mp-sec-chev-on{transform:rotate(90deg)}
.mp-row{all:unset;box-sizing:border-box;display:flex;align-items:center;gap:8px;width:100%;min-height:32px;padding:4px 8px;border-radius:8px;cursor:pointer;color:var(--fg);font-size:14px;line-height:24px}
.mp-row:hover{background:var(--fill-medium)}
.mp-row-on,.mp-row-on:hover{background:var(--fill-active);color:var(--fg)}
.mp-glyph{display:inline-grid;place-items:center;width:19px;height:19px;color:var(--fg-muted);flex:none}
.mp-row-on .mp-glyph{color:var(--fg)}
.mp-row-agent{min-height:30px}
.mp-row-name-agent{flex:0 1 auto;font-weight:500}
.mp-row-side,.mp-row-agent .mp-model{flex:1;min-width:0;text-align:left;font-size:14px;color:var(--fg-hint);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;line-height:24px}
.mp-row-name{flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.mp-row-on .mp-row-name{font-weight:500}
.mp-row-review{margin-top:8px}
.mp-tile{display:inline-grid;place-items:center;width:19px;height:19px;border-radius:8px;background:var(--fill-active);color:var(--fg-muted);flex:none}
.mp-dock{border-top:.5px solid var(--hair);padding:8px 8px 12px;display:flex;flex-direction:column;gap:8px}
.mp-dock .mp-sec{margin:0}
.mp-dock-body{display:flex;flex-direction:column;gap:8px;max-height:42vh;overflow:auto}
.mp-dock-empty,.mp-dock-more{padding:4px 8px}
.mp-fab{all:unset;box-sizing:border-box;position:absolute;right:12px;top:64px;display:flex;align-items:center;gap:8px;padding:8px 12px 8px 8px;border-radius:8px;background:var(--popover);box-shadow:var(--float);cursor:pointer;z-index:4}
.mp-project-settings{margin:12px;max-height:68vh;overflow:auto;flex-shrink:0;width:auto}.mp-member-settings{border:.5px solid var(--hair);border-radius:8px;padding:12px;margin:12px 0}.mp-member-settings legend{font-size:14px;color:var(--fg-hint);line-height:24px}.mp-member-settings select{width:100%;padding:8px;border:.5px solid var(--hair);border-radius:8px;background:var(--card);color:inherit}
/* Raft PanelHeader: 56px, seam under, 27px yellow tile, headline + caption */
.mp-chat{display:flex;flex-direction:column;min-width:0;min-height:0;background:var(--canvas)}
.mp-head{display:flex;align-items:center;gap:12px;height:56px;flex:none;padding:0 20px;border-bottom:.5px solid var(--hair)}
.mp-head-tile{display:inline-grid;place-items:center;width:28px;height:28px;border-radius:8px;background:var(--fill-muted);color:var(--fg-muted);flex:none}
.mp-head-text{display:flex;flex-direction:column;justify-content:center;min-width:0;flex:1}
.mp-head-title{font-size:14px;line-height:24px;font-weight:600;margin:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.mp-head-meta{font-size:14px;line-height:24px;color:var(--fg-hint);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;display:flex;align-items:center;min-width:0}
.mp-head-working{display:inline-flex;align-items:center;gap:8px;color:var(--fg-muted);min-width:0;overflow:hidden;text-overflow:ellipsis}
.mp-head-actions{display:flex;align-items:center;gap:4px;flex:none}
.mp-members{gap:8px}
.mp-member{all:unset;display:inline-flex;cursor:pointer;border-radius:8px;padding:4px}
.mp-member:hover{background:var(--fill-medium)}
.mp-chat-scroll{flex:1;min-height:0;overflow:auto;padding:16px 20px 8px}
.mp-column{max-width:820px;margin:0 auto;width:100%}
.mp-empty-chat{margin:32px auto 24px;max-width:460px;text-align:center;color:var(--fg-muted)}
.mp-empty-chat b{display:block;color:var(--fg);font-size:16px;margin-bottom:8px}
/* Raft timeline: message rows with a 35px avatar, 12px gap, rows inset 4px, turns 16px apart */
.mp-timeline{display:flex;flex-direction:column}
.mp-turn{margin-top:8px}
.mp-msg{display:flex;gap:12px;padding:4px 8px;border-radius:0;position:relative}
.mp-msg:hover{background:var(--fill-subtle)}
.mp-msg-main{min-width:0;flex:1}
.mp-msg-head{display:flex;align-items:baseline;gap:8px;min-width:0;margin-bottom:0px;overflow:hidden}
.mp-msg-head b{font-size:14px;line-height:24px;font-weight:500;white-space:nowrap}
.mp-msg-head time{color:var(--fg-hint);white-space:nowrap;flex:none}
.mp-msg-text{white-space:pre-wrap;word-break:break-word;font-size:14px;line-height:24px}
.mp-markdown{white-space:normal;min-width:0;overflow-wrap:anywhere}
.mp-markdown blockquote{border-left:0;border-inline-start:0;border-right:0;padding:12px 12px;border-radius:8px;background:var(--fill-muted)}
.mp-markdown pre{max-width:100%;overflow:auto}
.mp-mention{font-weight:500;color:var(--accent)}
.mp-mention-you{background:var(--accent-soft);color:var(--accent);border-radius:4px;padding:0 4px}
.mp-reacts{display:flex;gap:8px;margin-top:8px}
.mp-reacts span{display:inline-flex;align-items:center;gap:4px;height:24px;padding:0 8px;border-radius:8px;background:var(--fill-muted);font-size:14px}
/* Raft system message: Space Mono hint between hairlines; folds open */
.mp-sys{display:flex;align-items:center;gap:8px;padding:16px 16px 4px;font-size:14px;line-height:24px;color:var(--fg-hint);min-width:0}
.mp-sys::before,.mp-sys::after{content:"";min-width:12px;flex:1;border-top:.5px solid var(--hair)}
.mp-sys>span,.mp-sys>button{min-width:0;max-width:80%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.mp-sys-group .mp-sys+.mp-sys{padding-top:8px}
.mp-sys-toggle{all:unset;display:inline-flex;align-items:center;gap:4px;height:28px;padding:0 8px;border-radius:8px;cursor:pointer}
.mp-sys-toggle:hover{background:var(--fill-medium);color:var(--fg)}
.mp-sys-chev{display:inline-grid;place-items:center;transition:transform .15s}
.mp-sys-open .mp-sys-chev{transform:rotate(90deg)}
/* task chip under a message ("#2 @Koda") */
.mp-chips{display:flex;flex-wrap:wrap;gap:8px;margin-top:8px}
.mp-task-chip{all:unset;box-sizing:border-box;display:inline-flex;align-items:center;gap:4px;height:22px;padding:0 8px;border-radius:var(--mp4-r-ctl);font-size:14px;font-weight:500;line-height:24px;color:var(--fg-muted);cursor:pointer;white-space:nowrap}
.mp-task-chip:hover{filter:brightness(.96)}
/* Raft MessageReplies: a muted block, "N replies" and the latest three */
.mp-replies{all:unset;box-sizing:border-box;display:flex;flex-direction:column;gap:4px;width:100%;max-width:480px;margin-top:8px;padding:8px 12px;border-radius:12px;border:.5px solid var(--hair);background:var(--card);color:var(--fg-muted);cursor:pointer;text-align:left}
.mp-replies:hover{background:var(--fill-medium)}
.mp-replies:focus-visible{outline:2px solid var(--strong);outline-offset:2px}
.mp-replies-on{border-color:var(--accent);box-shadow:0 0 0 1px var(--accent)}
.mp-replies-archived{opacity:.72}
.mp-replies-sum{display:flex;align-items:center;gap:8px;min-width:0}
.mp-replies-title{flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:14px;line-height:24px;font-weight:600;color:var(--fg)}
.mp-replies-row{display:flex;align-items:center;gap:8px;min-width:0;font-size:14px;line-height:24px}
.mp-replies-row b{font-weight:500;color:var(--fg-muted);white-space:nowrap;flex:none}
.mp-replies-text{flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--fg-hint)}
.mp-replies-row time{color:var(--fg-hint);flex:none}
.mp-replies-count{font-size:14px;line-height:24px;font-weight:500;color:var(--accent);white-space:nowrap}
.mp-pending-time{flex:none;white-space:nowrap}
.mp-pending-open{all:unset;box-sizing:border-box;flex:none;height:24px;padding:0 12px;border-radius:var(--mp4-r-ctl);border:.5px solid var(--hair);font-size:14px;font-weight:500;color:var(--fg);cursor:pointer;background:var(--card)}
.mp-pending-open:hover{background:var(--fill-medium)}
.mp-spin{display:inline-block;flex:none;width:12px;height:12px;border-radius:50%;border:1.5px solid var(--hair);border-top-color:var(--accent);animation:mp-spin .8s linear infinite}
.mp-spin-sm{width:10px;height:10px}
@keyframes mp-spin{to{transform:rotate(360deg)}}
@media (prefers-reduced-motion:reduce){.mp-spin{animation:none}}
.mp-working{display:inline-flex;align-items:center;gap:8px;color:var(--fg-muted);min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.mp-working-err{color:var(--danger-strong)}
/* channel tabs */
.mp-library{padding:16px 0}.mp-library-upload{display:flex;flex-wrap:wrap;align-items:end;gap:12px;margin:20px 0}.mp-library-upload label{display:flex;flex-direction:column;gap:8px;max-width:100%}.mp-library-upload input{max-width:100%;box-sizing:border-box;padding:8px;border:.5px solid var(--hair);border-radius:8px;background:var(--fill-medium);color:var(--fg)}.mp-library-row{padding:16px 0;border-top:.5px solid var(--hair);overflow-wrap:anywhere}.mp-library-row>strong{display:block}.mp-file-links{display:flex;align-items:center;flex-wrap:wrap;gap:12px;margin-top:8px}.mp-file-links a{color:var(--fg-accent,var(--fg));text-decoration:underline}.mp-library-editor textarea{box-sizing:border-box;width:100%;padding:12px;background:var(--fill-medium);color:var(--fg);border:.5px solid var(--hair);border-radius:8px;font-family:var(--mono)}
/* 共享文件:右侧栏里一项一行、标签在上;用 mp4 面板的变量,按钮和输入框才有边和底 */
.mp4-filepane{display:flex;flex-direction:column;gap:20px;min-width:0;--hair:var(--mp4-hair2);--card:var(--mp4-bg);--fill-medium:var(--mp4-hover);--fg:var(--mp4-fg);--fg-muted:var(--mp4-fg2);--fg-hint:var(--mp4-fg3);--strong:var(--mp4-fg3);--accent:var(--mp4-fg2);--accent-soft:var(--dsw-alias-interactive-bg-active,#2631481a);--on-accent:var(--mp4-bg)}
.mp4-filepane .mp4-sec{gap:8px}
.mp4-filepane .mp4-note{margin:0}
.mp4-form{display:flex;flex-direction:column;gap:12px;min-width:0}
.mp4-field{display:flex;flex-direction:column;gap:8px;min-width:0;font-size:14px;font-weight:500;color:var(--mp4-fg2);line-height:24px}
.mp4-input{box-sizing:border-box;width:100%;min-width:0;font:inherit;font-size:14px;line-height:24px;font-weight:400;color:var(--mp4-fg);background:var(--mp4-bg);border:.5px solid var(--mp4-hair2);border-radius:8px;padding:8px 12px;outline:none}
.mp4-input:focus{border-color:var(--mp4-fg3);box-shadow:0 0 0 3px var(--dsw-alias-interactive-bg-hover,#2631480f)}
.mp4-input:disabled{opacity:.5}
.mp4-input[type=file]{padding:4px 8px;font-size:14px;line-height:24px}
.mp4-input[type=file]::file-selector-button{font:inherit;font-size:14px;line-height:24px;margin:0 8px 0 0;padding:0 12px;border-radius:8px;border:.5px solid var(--mp4-hair2);background:var(--dsw-alias-button-ghost-active-fill,var(--rx-gray-4));color:var(--mp4-fg);cursor:pointer}
.mp4-input[type=file]::file-selector-button:hover{background:var(--mp4-hover)}
.mp4-form-actions{display:flex;justify-content:flex-end;gap:8px;padding-top:4px}
.mp4-form-actions .mp-btn{height:30px;padding:0 12px;border-radius:8px;font-size:14px}
.mp4-filepane .mp-library-row{display:flex;flex-direction:column;gap:8px;padding:12px 0 12px;border-top:.5px solid var(--mp4-hair);overflow-wrap:anywhere}
.mp4-filepane .mp-library-row:first-of-type{border-top:0;padding-top:4px}
.mp4-file-head{display:flex;align-items:baseline;justify-content:space-between;gap:12px;min-width:0}
.mp4-file-head strong{font-size:14px;font-weight:500;min-width:0;overflow-wrap:anywhere;line-height:24px}
.mp4-file-head span{flex:none}
.mp4-filepane .mp-file-links{gap:12px;margin-top:4px}
.mp4-filepane .mp-file-links a{font-size:14px;color:var(--mp4-fg2);text-decoration:none;line-height:24px}
.mp4-filepane .mp-file-links a:hover{color:var(--mp4-fg);text-decoration:underline}
.mp4-filepane .mp-file-links .mp-btn{height:26px;padding:0 12px;border-radius:8px;font-size:14px}
.mp4-editor{box-sizing:border-box;width:100%;resize:vertical;padding:8px 12px;background:var(--mp4-bg);color:var(--mp4-fg);border:.5px solid var(--mp4-hair2);border-radius:8px;font:12px/1.6 "SF Mono",Menlo,monospace}
.mp4-filepane .mp-error{padding:0;color:var(--dsw-alias-state-error-primary,var(--rx-red-11));font-size:14px;line-height:24px}
.mp-tabs{display:flex;align-items:center;gap:4px;height:44px;flex:none;padding:0 20px;border-bottom:.5px solid var(--hair)}
.mp-tab{all:unset;box-sizing:border-box;display:inline-flex;align-items:center;gap:8px;height:30px;padding:0 12px;border-radius:8px;font-size:14px;font-weight:500;color:var(--fg-hint);cursor:pointer}
.mp-tab:hover{background:var(--fill-medium);color:var(--fg)}
.mp-tab-on,.mp-tab-on:hover{background:var(--fill-active);color:var(--fg)}
/* task board */
.mp-board{display:flex;flex-direction:column;gap:24px;min-width:0;padding-bottom:16px}
.mp-board-col{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(220px,100%),1fr));gap:8px;min-width:0}
.mp-board-col h3{grid-column:1/-1;display:flex;align-items:center;gap:8px;margin:4px 0;font-size:14px;line-height:24px;font-weight:600}
.mp-board-archive{padding-top:12px;border-top:.5px solid var(--hair)}.mp-board-archive summary{cursor:pointer;color:var(--fg-hint);font-size:14px;line-height:24px}.mp-board-archive .mp-tcard{margin-top:8px;width:100%}
.mp-task-preview{display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden;font-size:14px;line-height:24px}
.mp-task-reason{font-size:14px;line-height:24px;color:var(--fg-muted)}
.mp-task-summary{margin:4px 0 12px;padding:12px 16px;border:.5px solid var(--hair);border-radius:12px;background:var(--fill-muted)}
.mp-summary-toggle{all:unset;box-sizing:border-box;display:flex;align-items:center;gap:12px;width:100%;cursor:pointer;font-size:14px;line-height:24px}.mp-summary-toggle strong{margin-right:auto}
.mp-task-summary dl{display:grid;grid-template-columns:44px minmax(0,1fr);gap:8px 12px;margin:12px 0 0;font-size:14px;line-height:24px}.mp-task-summary dt{color:var(--fg-hint)}.mp-task-summary dd{margin:0;white-space:pre-wrap;overflow-wrap:anywhere}
.mp-execution{margin:0 0 16px;padding:4px 4px 12px;font-size:14px;line-height:24px;border-bottom:.5px solid var(--hair)}.mp-execution-head{display:flex;align-items:center;justify-content:space-between;gap:8px}.mp-execution-body{margin-top:8px}.mp-execution-row{display:flex;align-items:center;justify-content:space-between;gap:8px;padding:8px 0;overflow-wrap:anywhere}.mp-execution-row>div{min-width:0}.mp-execution-row .mp-btn{flex:none}
.mp-thread-closed{display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px;color:var(--fg-hint);font-size:14px;line-height:24px}
.mp-trace-dialog{box-sizing:border-box;width:min(1200px,calc(100vw - 32px));height:calc(100dvh - 48px);max-width:none;max-height:none;padding:0;border:.5px solid var(--hair);border-radius:12px;background:var(--canvas);color:var(--fg);box-shadow:0 20px 80px #0003}.mp-trace-dialog[open]{display:flex;flex-direction:column}.mp-trace-dialog::backdrop{background:#0005}.mp-trace-header{display:flex;align-items:center;justify-content:space-between;gap:16px;padding:16px 20px;border-bottom:.5px solid var(--hair)}.mp-trace-header p{margin:4px 0 0;font-size:14px;line-height:24px}.mp-trace-body{display:flex;flex:1;min-height:0;min-width:0;overflow:hidden;padding:8px}.mp-trace-body>div{flex:1;min-width:0;min-height:0}.mp-trace-body>p{padding:16px;font-size:14px;line-height:24px}
.mp-trace-events{overflow:auto;padding:12px 20px;min-width:0}.mp-trace-event{padding:12px 0;border-bottom:.5px solid var(--hair);font-size:14px;line-height:24px}.mp-trace-event>summary{cursor:pointer;font-weight:500}.mp-trace-event time{margin-left:16px;color:var(--fg-hint);font-weight:400;font-size:14px;line-height:24px}.mp-trace-event>.mp-markdown{padding:8px 0;overflow-wrap:anywhere}
.mp-tcard{all:unset;box-sizing:border-box;display:flex;flex-direction:column;gap:8px;padding:12px;border-radius:12px;border:.5px solid var(--hair);background:var(--card);cursor:pointer}
.mp-tcard:hover{background:var(--fill-medium)}
.mp-tcard-on{border-color:var(--accent);box-shadow:0 0 0 1px var(--accent)}
.mp-tcard-title{font-size:14px;line-height:24px;font-weight:500;word-break:break-word}
.mp-tcard-foot{display:flex;align-items:center;gap:8px;min-width:0}
/* thread panel original + details */
.mp-thread-original{display:flex;flex-direction:column;padding-bottom:8px;border-bottom:.5px solid var(--hair);margin:0 -12px;padding-left:12px;padding-right:12px}
.mp-thread-original-chip{padding:0 8px 0 56px;margin-top:-4px}
.mp-details{margin:8px 8px 0 56px;border-radius:8px;background:var(--fill-muted)}
.mp-details-head{all:unset;box-sizing:border-box;display:flex;align-items:center;gap:8px;width:100%;padding:8px 12px;cursor:pointer;min-width:0}
.mp-details-title{flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:14px;font-weight:600;line-height:24px}
.mp-details-body{display:flex;flex-direction:column;gap:8px;padding:0 12px 12px}
.mp-details .mp-handoff{border-top-color:var(--hair)}
.mp-details .mp-children,.mp-details .mp-notice{margin:0}
.mp-followers{display:inline-flex;align-items:center;gap:8px;margin-right:4px}
.mp-followers b{font-size:14px;line-height:24px}
/* Raft MessageActionCard */
.mp-action{display:flex;flex-direction:column;width:100%;max-width:440px;margin-top:8px;border-radius:12px;border:.5px solid var(--hair);background:var(--card)}
.mp-action-head{display:flex;align-items:flex-start;gap:8px;padding:12px 12px 8px;font-size:14px;line-height:24px;font-weight:600}
.mp-action-icon{display:inline-grid;place-items:center;width:27px;height:27px;border-radius:8px;background:var(--fill-muted);color:var(--fg-muted);flex:none}
.mp-action-icon-av{background:transparent}
.mp-action-title{flex:1;min-width:0;padding-top:4px;word-break:break-word}
.mp-action-desc{display:block;margin-top:4px;font-size:14px;line-height:24px;font-weight:400;color:var(--fg-muted)}
.mp-action-status{display:inline-flex;align-items:center;gap:8px;margin-left:auto;padding-top:4px;flex:none;font-size:14px;line-height:24px;font-weight:500;color:var(--fg-muted);white-space:nowrap}
.mp-action-body{display:flex;flex-direction:column;gap:8px;padding:0 12px 12px 48px}
.mp-action-note,.mp-action-text{padding:0 12px 8px 48px;font-size:14px;line-height:24px;color:var(--fg-muted)}
.mp-action-acts{display:flex;flex-wrap:wrap;align-items:center;gap:8px;padding:0 12px 12px 48px}
.mp-action-why{padding:0 12px 12px 48px;color:var(--fg-hint)}
.mp-action-compact{max-width:none;margin-top:0}
.mp-action-compact .mp-action-head{padding:12px 12px 8px;font-size:14px;line-height:24px}
.mp-action-compact .mp-action-text,.mp-action-compact .mp-action-acts{padding-left:12px;padding-right:12px}
.mp-action-compact .mp-action-acts .mp-btn{height:28px;padding:0 12px;font-size:14px}
.mp-action-closed{background:var(--fill-muted);border-color:transparent}
.mp-opt{display:flex;align-items:center;gap:12px;flex-wrap:wrap}
.mp-opt-note{font-size:14px;line-height:24px;color:var(--fg-hint)}
.mp-opt-done{display:grid;grid-template-columns:16px auto minmax(0,1fr);gap:8px;align-items:baseline;font-size:14px;line-height:24px;color:var(--fg-hint)}
.mp-opt-done.mp-opt-chosen{color:var(--fg);font-weight:500}
.mp-opt-mark{display:inline-grid;place-items:center;align-self:center}
/* Raft Composer */
.mp-composer-wrap{padding:0 20px 16px;flex:none}
.mp-column.mp-composer-wrap{padding:0 20px 16px}
.mp-composer{position:relative;display:flex;flex-direction:column;gap:8px;border:.5px solid var(--hair);border-radius:12px;background:var(--card);padding:16px 16px 12px;box-shadow:0 2px 12px -4px rgba(15,17,21,.08)}
.mp-composer:focus-within{border-color:var(--strong)}
.mp-composer textarea{all:unset;box-sizing:border-box;width:100%;resize:none;font-family:var(--sans);font-size:14px;line-height:24px;min-height:24px;max-height:128px;overflow:auto;white-space:pre-wrap;color:var(--fg);padding:0 4px}
.mp-composer textarea::placeholder{color:var(--fg-hint)}
.mp-composer-bar{display:flex;align-items:center;justify-content:space-between;gap:8px;margin-right:-8px}
.mp-composer-hint{font-size:14px;line-height:24px;color:var(--fg-hint);min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.mp-send{all:unset;box-sizing:border-box;display:inline-grid;place-items:center;width:32px;height:32px;border-radius:50%;background:var(--accent);color:var(--on-accent);cursor:pointer;flex:none}
.mp-send:hover{background:color-mix(in srgb,var(--accent) 88%,#000)}
.mp-send:disabled{background:color-mix(in srgb,var(--accent) 40%,var(--canvas));color:var(--on-accent);cursor:default}
.mp-switch{display:block;max-width:100%;font:inherit;font-size:16px;font-weight:500;color:inherit;background:transparent;border:0;padding:0 4px 0 0;margin:0;cursor:pointer}
.mp-menu{position:absolute;left:8px;bottom:calc(100% + 6px);min-width:280px;max-width:420px;background:var(--popover);border-radius:8px;padding:4px;box-shadow:var(--float);display:flex;flex-direction:column;z-index:5}
.mp-menu-label{padding:8px 8px 4px;font-size:14px;line-height:24px;font-weight:500;color:var(--fg-hint)}
.mp-menu-item{all:unset;box-sizing:border-box;display:grid;grid-template-columns:19px auto auto minmax(0,1fr);gap:8px;align-items:center;padding:8px 8px;border-radius:8px;cursor:pointer;font-size:14px;line-height:24px}
.mp-menu-item>span:last-child{color:var(--fg-hint);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:14px;line-height:24px}
.mp-menu-on{background:var(--fill-medium)}
.mp-pending{min-height:8px}
.mp-pending-on{display:flex;align-items:center;gap:8px;min-height:30px;padding:0 4px 8px;font-size:14px;line-height:24px;color:var(--fg-muted);background:linear-gradient(to top,var(--canvas) 70%,transparent)}
/* thread panel */
.mp-thread{display:flex;flex-direction:column;min-height:0;min-width:0;background:var(--canvas)}
.mp-thread .mp-head-title{font-size:16px}
.mp-thread-scroll{flex:1;min-height:0;overflow:auto;padding:20px 20px 8px;display:flex;flex-direction:column}
.mp-thread-content{max-width:860px;width:100%;margin:0 auto}
.mp-thread .mp-composer-wrap{width:100%;max-width:900px;margin:0 auto;padding:8px 20px 16px}
.mp-thread-project-link{all:unset;cursor:pointer;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:60%}
.mp-thread-project-link:hover{color:var(--accent);text-decoration:underline}
.mp-thread-head{min-height:64px}
@media(max-width:520px){.mp-thread .mp-followers{display:none}.mp-thread-head{gap:8px;padding:0 12px}.mp-thread-scroll{padding:16px 12px 8px}.mp-thread .mp-composer-wrap{padding:8px 12px 12px}}
.mp-task{display:flex;flex-direction:column;gap:8px;margin:0 8px;padding:12px;border-radius:8px;border:.5px solid var(--hair);background:var(--card)}
.mp-task-meta{display:flex;align-items:center;gap:8px;min-width:0;flex-wrap:wrap}
.mp-task-who{display:inline-flex;align-items:center;gap:8px;min-width:0;font-size:14px;line-height:24px;color:var(--fg-muted);margin-left:auto}
.mp-task-title{font-size:16px;line-height:24px;font-weight:600;word-break:break-word}
.mp-task-desc{font-size:14px;line-height:24px;color:var(--fg-muted);white-space:pre-wrap;word-break:break-word}
.mp-task-live{display:flex;align-items:center;gap:8px;font-size:14px;line-height:24px;color:var(--fg-hint)}
.mp-task-check{display:flex;flex-direction:column;gap:4px;margin-top:4px}
.mp-task-acts{display:flex;gap:8px;flex-wrap:wrap;margin-top:4px}
.mp-task-acts:empty{display:none}
.mp-check{list-style:none;margin:0;padding:0;display:flex;flex-direction:column;gap:4px;font-size:14px;line-height:24px}
.mp-check li{display:flex;align-items:flex-start;gap:8px;line-height:24px}
.mp-check li i{display:inline-grid;place-items:center;width:16px;height:16px;border-radius:4px;border:.5px solid var(--strong);margin-top:4px;flex:none;color:var(--on-accent);background:var(--card)}
.mp-check li.mp-done{color:var(--fg-hint)}
.mp-check li.mp-done i{background:var(--accent);border-color:var(--accent)}
.mp-handoff{display:flex;flex-direction:column;gap:8px;font-size:14px;margin-top:4px;padding-top:8px;border-top:.5px solid var(--hair);line-height:24px}
.mp-handoff-head{all:unset;display:flex;align-items:center;gap:8px;cursor:pointer;font-size:14px;font-weight:500;color:var(--fg-muted);height:24px}
.mp-handoff-head:hover{color:var(--fg)}
.mp-handoff-row{display:grid;grid-template-columns:64px minmax(0,1fr);gap:8px;line-height:24px}
.mp-handoff-row blockquote{margin:0;padding:8px 8px;border-radius:8px;background:var(--fill-muted);white-space:pre-wrap;color:var(--fg-muted)}
.mp-k{font-size:14px;color:var(--fg-hint);font-weight:500;line-height:24px}
.mp-children{display:flex;flex-direction:column;gap:4px;font-size:14px;margin:0 8px;line-height:24px}
.mp-child{display:flex;align-items:center;gap:8px;padding:4px 0}
.mp-notice{font-size:14px;line-height:24px;color:var(--fg-muted);padding:8px 12px;margin:0 8px;border-radius:8px;background:var(--fill-muted)}
.mp-warn{color:var(--fg);background:var(--s-pink-soft)}
.mp-sys-replies{padding-top:8px}
/* review */
.mp-review-grid{display:grid;grid-template-columns:minmax(0,1.1fr) minmax(0,1fr);gap:28px;max-width:1080px;margin:0 auto;padding:8px 0 24px}
.mp-review-col{display:flex;flex-direction:column;gap:12px;min-width:0}
.mp-review-col h2{font-size:14px;line-height:24px;font-weight:600;margin:4px 0 0}
.mp-review-col h2 .mp-hint{font-weight:400}
.mp-review-col .mp-action{max-width:none;margin-top:0}
.mp-mem{display:flex;flex-direction:column;gap:4px;padding:12px 12px;border:.5px solid var(--hair);border-radius:8px;background:var(--card)}
.mp-mem h3{display:flex;align-items:center;gap:8px;font-size:14px;line-height:24px;font-weight:600;margin:0 0 4px}
.mp-mem p{margin:0;line-height:24px}
.mp-mem-item{display:grid;grid-template-columns:minmax(0,1fr) auto 20px;gap:8px;align-items:center;padding:4px 0;border-top:.5px solid var(--hair);font-size:14px;line-height:24px}
.mp-mem-item:first-of-type{border-top:0}
.mp-mem-ev{color:var(--fg-hint);white-space:nowrap}
.mp-closed summary{cursor:pointer;color:var(--fg-hint);font-size:14px;margin:4px 0 8px;line-height:24px}
.mp-closed{display:flex;flex-direction:column;gap:8px}
/* create */
.mp-create{display:flex;flex-direction:column;gap:12px}
.mp-create label{display:flex;flex-direction:column;gap:8px;font-size:14px;font-weight:500;color:var(--fg-muted);line-height:24px}
.mp-create label.mp-create-check{flex-direction:row;align-items:center;justify-content:flex-start;gap:8px}
.mp-create label.mp-create-check input{flex:none;width:auto;margin:0;padding:0}
.mp-create input,.mp-create textarea,.mp-create select{font:inherit;font-weight:400;color:var(--fg);background:var(--card);border:.5px solid var(--hair);border-radius:8px;padding:8px 12px;outline:none;resize:vertical}
.mp-create input:focus,.mp-create textarea:focus{box-shadow:0 0 0 3px var(--fill-medium)}
.mp-create-note{margin:0;font-size:14px;color:var(--fg-hint);line-height:24px}
.mp-create-bar{display:flex;justify-content:flex-end;gap:8px}
.mp-error{color:var(--danger-strong);font-size:14px;padding:0 12px 8px;line-height:24px}
.mp-composer .mp-error{padding:0}
.mp-offline{position:absolute;left:50%;bottom:12px;transform:translateX(-50%);font-size:14px;padding:8px 12px;border-radius:8px;background:oklch(0.2634 0.0093 294.92);color:oklch(0.9647 0.0027 286.35);line-height:24px}
/* ---------------------------------------------------------------- v4: DSH-session frame */
:root{--rx-gray-1:#fcfcfc;--rx-gray-2:#f9f9f9;--rx-gray-3:#f0f0f0;--rx-gray-4:#e8e8e8;--rx-gray-5:#e0e0e0;--rx-gray-6:#d9d9d9;--rx-gray-7:#cecece;--rx-gray-8:#bbbbbb;--rx-gray-9:#8d8d8d;--rx-gray-10:#838383;--rx-gray-11:#646464;--rx-gray-12:#202020;--rx-blue-3:#e6f4fe;--rx-blue-6:#acd8fc;--rx-blue-9:#0090ff;--rx-blue-11:#0d74ce;--rx-green-3:#e6f6eb;--rx-green-6:#adddc0;--rx-green-9:#30a46c;--rx-green-11:#218358;--rx-red-3:#feebec;--rx-red-6:#fdbdbe;--rx-red-9:#e5484d;--rx-red-11:#ce2c31;--rx-orange-3:#ffefd6;--rx-orange-6:#ffc182;--rx-orange-9:#f76b15;--rx-orange-11:#cc4e00;--rx-amber-3:#fff7c2;--rx-amber-6:#f3d673;--rx-amber-9:#ffc53d;--rx-amber-11:#ab6400;--rx-yellow-11:#9e6c00;--rx-teal-3:#e0f8f3;--rx-teal-6:#a1ded2;--rx-teal-9:#12a594;--rx-teal-11:#008573;--rx-purple-3:#f7edfe;--rx-purple-6:#e0c4f4;--rx-purple-9:#8e4ec6;--rx-purple-11:#8145b5;--rx-pink-3:#fee9f5;--rx-pink-6:#efbfdd;--rx-pink-9:#d6409f;--rx-pink-11:#c2298a;--rx-violet-3:#f4f0fe;--rx-violet-6:#d4cafe;--rx-violet-9:#6e56cf;--rx-violet-11:#6550b9}
@media (prefers-color-scheme:dark){html:not([data-theme="light"]){--rx-gray-1:#111111;--rx-gray-2:#191919;--rx-gray-3:#222222;--rx-gray-4:#2a2a2a;--rx-gray-5:#313131;--rx-gray-6:#3a3a3a;--rx-gray-7:#484848;--rx-gray-8:#606060;--rx-gray-9:#6e6e6e;--rx-gray-10:#7b7b7b;--rx-gray-11:#b4b4b4;--rx-gray-12:#eeeeee;--rx-blue-3:#0d2847;--rx-blue-6:#104d87;--rx-blue-9:#0090ff;--rx-blue-11:#70b8ff;--rx-green-3:#132d21;--rx-green-6:#20573e;--rx-green-9:#30a46c;--rx-green-11:#3dd68c;--rx-red-3:#3b1219;--rx-red-6:#72232d;--rx-red-9:#e5484d;--rx-red-11:#ff9592;--rx-orange-3:#331e0b;--rx-orange-6:#66350c;--rx-orange-9:#f76b15;--rx-orange-11:#ffa057;--rx-amber-3:#302008;--rx-amber-6:#5c3d05;--rx-amber-9:#ffc53d;--rx-amber-11:#ffca16;--rx-yellow-11:#f5e147;--rx-teal-3:#0d2d2a;--rx-teal-6:#145750;--rx-teal-9:#12a594;--rx-teal-11:#0bd8b6;--rx-purple-3:#301c3b;--rx-purple-6:#54346b;--rx-purple-9:#8e4ec6;--rx-purple-11:#d19dff;--rx-pink-3:#37172f;--rx-pink-6:#692955;--rx-pink-9:#d6409f;--rx-pink-11:#ff8dcc;--rx-violet-3:#291f43;--rx-violet-6:#473876;--rx-violet-9:#6e56cf;--rx-violet-11:#baa7ff}}
html[data-theme="dark"],html.dark{--rx-gray-1:#111111;--rx-gray-2:#191919;--rx-gray-3:#222222;--rx-gray-4:#2a2a2a;--rx-gray-5:#313131;--rx-gray-6:#3a3a3a;--rx-gray-7:#484848;--rx-gray-8:#606060;--rx-gray-9:#6e6e6e;--rx-gray-10:#7b7b7b;--rx-gray-11:#b4b4b4;--rx-gray-12:#eeeeee;--rx-blue-3:#0d2847;--rx-blue-6:#104d87;--rx-blue-9:#0090ff;--rx-blue-11:#70b8ff;--rx-green-3:#132d21;--rx-green-6:#20573e;--rx-green-9:#30a46c;--rx-green-11:#3dd68c;--rx-red-3:#3b1219;--rx-red-6:#72232d;--rx-red-9:#e5484d;--rx-red-11:#ff9592;--rx-orange-3:#331e0b;--rx-orange-6:#66350c;--rx-orange-9:#f76b15;--rx-orange-11:#ffa057;--rx-amber-3:#302008;--rx-amber-6:#5c3d05;--rx-amber-9:#ffc53d;--rx-amber-11:#ffca16;--rx-yellow-11:#f5e147;--rx-teal-3:#0d2d2a;--rx-teal-6:#145750;--rx-teal-9:#12a594;--rx-teal-11:#0bd8b6;--rx-purple-3:#301c3b;--rx-purple-6:#54346b;--rx-purple-9:#8e4ec6;--rx-purple-11:#d19dff;--rx-pink-3:#37172f;--rx-pink-6:#692955;--rx-pink-9:#d6409f;--rx-pink-11:#ff8dcc;--rx-violet-3:#291f43;--rx-violet-6:#473876;--rx-violet-9:#6e56cf;--rx-violet-11:#baa7ff}
.mp4-frame,.mp4-index,.mp4-tabpane,.mp4-ask,.mp4-toolline,.mp4-inbox,.mp4-trig,.mp4-posted,.mp4-ball-wrap,.mp4-sugs,.mp4-dock-asks,.mp4-dock-notes,.mp4-filepane,.mp4-crumb,.mp4-studio-veil{--mp4-fg:var(--rx-gray-12);--mp4-fg2:var(--rx-gray-11);--mp4-fg3:var(--rx-gray-11);--mp4-hair:var(--mp4-t-line);--mp4-hair2:var(--mp4-t-border);--mp4-bg:var(--dsw-alias-bg-base,#FFFFFF);--mp4-fill:var(--dsw-alias-bg-secondary,var(--rx-gray-2));--mp4-hover:var(--mp4-t-hover);--mp4-on:var(--dsw-alias-interactive-bg-active,rgba(38,49,72,.1));font-family:var(--dsw-font-family,-apple-system,"PingFang SC",sans-serif);color:var(--mp4-fg)}
.mp4-frame{display:flex;flex-direction:row;width:100%;height:100%;min-height:0;background:var(--mp4-bg)}
.mp4-main{flex:1 1 auto;min-width:0;display:flex;flex-direction:column;min-height:0}
.mp4-head{flex:none;height:52px;display:flex;align-items:center;gap:12px;padding:0 20px;border-bottom:.5px solid var(--mp4-hair)}
.mp4-head-text{flex:1;min-width:0;display:flex;flex-direction:column;justify-content:center}
.mp4-head h1{margin:0;font-size:14px;line-height:24px;font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.mp4-crumb{display:flex;align-items:center;gap:8px;font-size:14px;line-height:24px;color:var(--mp4-fg3)}
.mp4-crumb button{all:unset;cursor:pointer;color:var(--mp4-fg3)}.mp4-crumb button:hover{color:var(--mp4-fg)}
.mp4-where{display:inline-flex;align-items:center;gap:8px;font-size:14px;color:var(--mp4-fg3);white-space:nowrap;line-height:24px}
.mp4-icon{all:unset;box-sizing:border-box;display:inline-flex;align-items:center;justify-content:center;min-width:28px;height:28px;border-radius:8px;color:var(--mp4-fg3);cursor:pointer;font-size:14px}
.mp4-icon:hover{background:var(--mp4-hover);color:var(--mp4-fg)}.mp4-icon:disabled{opacity:.4;cursor:default}
.mp4-icon:focus-visible,.mp4-row:focus-visible,.mp4-card:focus-visible,.mp4-sug:focus-visible,.mp4-opt:focus-visible{outline:2px solid var(--mp4-fg3);outline-offset:1px}
.mp4-more{position:relative}
.mp4-menu{position:absolute;right:0;top:32px;z-index:20;min-width:180px;padding:4px;background:var(--mp4-bg);border-radius:8px;box-shadow:0 0 0 .5px var(--mp4-hair2),0 12px 32px -12px rgba(0,0,0,.3);display:flex;flex-direction:column}
.mp4-menu button{all:unset;cursor:pointer;padding:8px 12px;border-radius:8px;font-size:14px;line-height:24px}.mp4-menu button:hover{background:var(--mp4-hover)}
.mp4-scroll{flex:1;min-height:0;overflow:auto;overflow-anchor:none}
.mp4-column{max-width:760px;margin:0 auto;padding:20px 24px 24px;display:flex;flex-direction:column;gap:16px;font-size:14px;line-height:1.7}
.mp4-composer{flex:none;width:min(100%,808px);box-sizing:border-box;margin:0 auto;padding:0 24px 16px;display:flex;flex-direction:column;gap:8px}
.mp4-hint{font-size:14px;color:var(--mp4-fg3);text-align:center;line-height:24px}
.mp4-empty{padding:28px 0;color:var(--mp4-fg3)}.mp4-empty b{display:block;color:var(--mp4-fg);font-size:16px;margin-bottom:8px;line-height:24px}.mp4-empty p{margin:0}
.mp4-user{align-self:flex-end;max-width:86%;display:flex;flex-direction:column;align-items:flex-end;gap:4px}
.mp4-bubble{background:var(--mp4-fill);border-radius:8px;padding:8px 16px;white-space:pre-wrap;overflow-wrap:anywhere}
.mp4-assistant{line-height:1.75}
.mp4-srcs{display:flex;gap:8px;flex-wrap:wrap;margin-top:8px}
.mp4-src{all:unset;cursor:pointer;font-size:14px;line-height:24px;padding:0 8px;border-radius:var(--mp4-r-ctl);background:var(--mp4-fill);color:var(--mp4-fg2)}.mp4-src:hover{background:var(--mp4-on)}
.mp4-act{display:flex;gap:12px;align-items:flex-start;border:.5px solid var(--mp4-hair);border-radius:8px;padding:12px 12px;font-size:14px;line-height:24px}
.mp4-act-mark{color:var(--rx-green-11);font-weight:500}.mp4-act small{display:block;color:var(--mp4-fg3);font-size:14px;line-height:24px}
.mp4-live{display:flex;gap:8px;align-items:center;font-size:14px;color:var(--mp4-fg2);line-height:24px}
.mp4-sugs{display:flex;gap:8px;flex-wrap:wrap}
/* Above the composer the row sits in DSH's composer seat, whose fade is still see-through at its top: give it its own solid ground. DSH caps the composer card's width and centres it, so the side inset follows the card (16px only when the column is narrower than the cap); a fixed 16px left the row at the far left on a wide window. */
.mp4-dock-notes{display:flex;flex-direction:column;gap:4px;padding:10px max(var(--dsh-composer-side-clearance,16px),calc((100% - var(--dsh-composer-card-max-width,712px))/2)) 2px;background:linear-gradient(to bottom,transparent,var(--mp4-bg) 10px);font-size:14px;line-height:1.6;color:var(--mp4-fg2)}
.mp4-dock-note{display:flex;align-items:center;gap:4px;min-width:0}
.mp4-shot-q{flex:none;margin-right:4px}.mp4-shot-alt{all:unset;box-sizing:border-box;cursor:pointer;min-width:0;max-width:240px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;padding:0 8px;height:22px;line-height:24px;border:.5px solid var(--mp4-hair2);border-radius:8px;color:var(--mp4-fg);background:var(--mp4-bg);transition:background 250ms ease-out}.mp4-shot-alt:hover{background:var(--mp4-hover)}.mp4-shot-alt:focus-visible{outline:2px solid var(--mp4-fg3);outline-offset:1px}.mp4-dock-note .mp4-dock-x{margin-left:auto}.mp4-dock-note-text{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.mp4-dock-note .mp4-mem-link{flex:none}
.mp4-dock-sugs{padding:12px max(var(--dsh-composer-side-clearance,16px),calc((100% - var(--dsh-composer-card-max-width,712px))/2)) 8px;background:linear-gradient(to bottom,transparent,var(--mp4-bg) 12px)}
.mp4-dock-sugs .mp4-sug{background:var(--mp4-bg)}.mp4-dock-sugs .mp4-sug:hover{background:linear-gradient(var(--mp4-hover),var(--mp4-hover)),var(--mp4-bg)}
.mp4-sug{all:unset;box-sizing:border-box;cursor:pointer;font-size:14px;line-height:24px;padding:4px 12px;border-radius:var(--mp4-r-ctl);border:.5px solid var(--mp4-hair2);color:var(--mp4-fg)}.mp4-sug:hover{background:var(--mp4-hover)}.mp4-sug:disabled{opacity:.5;cursor:default}.mp4-sug-on{border-color:var(--mp4-fg2);background:var(--mp4-fill);font-weight:500}
.mp4-link-btn{all:unset;cursor:pointer;font-size:14px;color:var(--mp4-fg2);padding:4px 0;align-self:flex-start;line-height:24px}.mp4-link-btn:hover{color:var(--mp4-fg);text-decoration:underline}
.mp4-muted{color:var(--mp4-fg3)}.mp4-mono{font-family:"SF Mono",Menlo,monospace;font-size:14px;line-height:24px}
.mp4-ell{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;min-width:0}
.mp4-note{font-size:14px;line-height:1.6;margin:8px 0 0}
.mp4-merge{display:flex;align-items:flex-start;gap:12px}.mp4-merge>code{flex:1;min-width:0;overflow-wrap:anywhere;padding:8px 8px;border-radius:8px;background:var(--mp4-fill)}.mp4-merge>button{padding:8px 0}
.mp4-agent{display:grid;gap:4px;padding:8px 0;border-bottom:.5px solid var(--mp4-hair)}
.mp4-agent:last-child{border-bottom:0}
.mp4-agent-top{display:flex;justify-content:space-between;gap:8px;font-size:14px;line-height:24px}
.mp4-agent-select{width:100%;height:28px;box-sizing:border-box;font:inherit;font-size:14px;padding:0 24px 0 8px;border:.5px solid var(--mp4-hair2);border-radius:8px;color:var(--mp4-fg);-webkit-appearance:none;appearance:none;background:var(--mp4-bg) url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 24 24' fill='none' stroke='%238d8d8d' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'%3E%3Cpath d='M6 9l6 6 6-6'/%3E%3C/svg%3E") no-repeat right 8px center;cursor:pointer}.mp4-agent-select:hover{border-color:var(--rx-gray-8)}
/* beside the Session title: up to the project, and who works on the task */
.mp4-crumb{display:inline-flex;align-items:center;gap:8px;min-width:0;font-size:14px;line-height:24px}
.mp4-crumb .mp4-crumb-up{all:unset;box-sizing:border-box;cursor:pointer;display:inline-flex;align-items:center;gap:4px;min-width:0;max-width:220px;height:20px;padding:0 8px;border-radius:8px;font-size:14px;line-height:24px;color:var(--mp4-fg2);background:var(--rx-gray-3);transition:background 250ms ease-out,color 250ms ease-out}
.mp4-crumb .mp4-crumb-up:hover{background:var(--rx-gray-4);color:var(--mp4-fg)}
.mp4-crumb-up:focus-visible{outline:2px solid var(--dsw-alias-state-business-primary,var(--rx-blue-9));outline-offset:1px}
.mp4-crumb-name{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.mp4-crumb-icon{flex:none}
.mp4-crumb-key{flex:none;font:inherit;font-size:14px;line-height:24px;color:var(--mp4-fg3);margin-left:4px}
.mp4-crumb-who{flex:none;box-sizing:border-box;max-width:120px;height:20px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;padding:0 8px;border-radius:8px;line-height:24px;color:var(--mp4-fg2);background:var(--rx-gray-3)}
/* state: one semantic colour channel */
.mp4-pill{display:inline-flex;align-items:center;font-size:14px;line-height:24px;font-weight:500;padding:0 8px;border-radius:var(--mp4-r-ctl);white-space:nowrap}
.mp4-wait{color:var(--rx-yellow-11);background:var(--rx-amber-3)}.mp4-run{color:var(--rx-blue-11);background:var(--rx-blue-3)}.mp4-done{color:var(--rx-green-11);background:var(--rx-green-3)}.mp4-fail{color:var(--rx-red-11);background:var(--rx-red-3)}.mp4-idle{color:var(--rx-gray-11);background:var(--rx-gray-3)}
.mp4-dot{display:inline-block;flex:none;width:6px;height:6px;border-radius:4px;background:var(--rx-gray-8)}
.mp4-dot-wait{background:var(--rx-amber-9)}.mp4-dot-run{background:var(--rx-blue-9)}.mp4-dot-done{background:var(--rx-green-9)}.mp4-dot-fail{background:var(--rx-red-9)}.mp4-dot-idle{background:var(--rx-gray-8)}
/* right panel */
.mp4-panel{flex:none;width:420px;max-width:46%;border-left:.5px solid var(--mp4-hair);display:flex;flex-direction:column;min-height:0;background:var(--mp4-bg)}
.mp4-panel-wide{width:640px;max-width:60%}
.mp4-tabs{flex:none;height:44px;display:flex;align-items:center;gap:4px;padding:0 8px;border-bottom:.5px solid var(--mp4-hair);overflow:hidden} .mp4-tabs-list{display:flex;align-items:center;gap:4px;min-width:0;overflow-x:auto;scrollbar-width:none} .mp4-tabs-list::-webkit-scrollbar{display:none} .mp4-tabs > .mp4-icon{flex:none}
.mp4-tab{display:inline-flex;align-items:center;border-radius:8px;flex:none}
.mp4-tab button{all:unset;cursor:pointer;display:inline-flex;align-items:center;gap:8px;font-size:14px;line-height:24px;padding:0 4px 0 12px;color:var(--mp4-fg3)}
.mp4-tab .mp4-tab-x{padding:0 8px 0 4px;opacity:.5}.mp4-tab .mp4-tab-x:hover{opacity:1}
.mp4-tab-on{background:var(--mp4-on)}.mp4-tab-on button{color:var(--mp4-fg);font-weight:500}
.mp4-tabs-end{margin-left:auto;display:inline-flex;gap:4px}
.mp4-panel-body{flex:1;min-height:0;overflow:auto;padding:16px 16px 24px;display:flex;flex-direction:column;gap:16px;font-size:14px;line-height:24px}
.mp4-start{display:flex;flex-direction:column;gap:8px;padding-top:20px}
.mp4-guide{all:unset;box-sizing:border-box;cursor:pointer;display:flex;gap:12px;align-items:center;padding:12px 12px;border:.5px solid var(--mp4-hair);border-radius:8px}.mp4-guide:hover{background:var(--mp4-hover)}
.mp4-guide>span:last-child{display:flex;flex-direction:column}.mp4-guide small{color:var(--mp4-fg3);font-size:14px;line-height:24px}
.mp4-guide-icon{width:32px;height:32px;border-radius:8px;background:var(--mp4-fill);display:inline-flex;align-items:center;justify-content:center;color:var(--mp4-fg2)}
.mp4-sec{display:flex;flex-direction:column;gap:4px}
.mp4-k{margin:0 0 4px;font-size:14px;font-weight:500;color:var(--mp4-fg3);line-height:24px}
.mp4-grid{display:grid;grid-template-columns:52px minmax(0,1fr);gap:8px 12px}.mp4-grid .mp4-k{margin:0;line-height:24px}
.mp4-grid>span{overflow-wrap:anywhere}.mp4-grid>span:not(.mp4-k){font-size:14px;line-height:24px;color:var(--mp4-fg)}
.mp4-files-link{display:inline-flex;align-items:center;gap:4px;margin-top:8px}.mp4-prow-desc{display:flex;align-items:center;gap:8px;min-width:0}.mp4-prow-state{flex:none;margin-left:4px;font-size:14px;line-height:24px;color:var(--mp4-fg3)}
.mp4-check{display:grid;grid-template-columns:16px minmax(0,1fr);gap:8px;padding:4px 0}.mp4-check>span:first-child{color:var(--mp4-fg3)}.mp4-check-done>span:first-child{color:var(--rx-green-11)}.mp4-check-done>span:last-child{color:var(--mp4-fg3)}
.mp4-li{padding:4px 0}
.mp4-row{all:unset;box-sizing:border-box;cursor:pointer;display:flex;gap:12px;align-items:flex-start;width:100%;padding:8px 8px;border-radius:8px}.mp4-row:hover{background:var(--mp4-hover)}.mp4-row-on{background:var(--mp4-on)}
.mp4-row-static{cursor:default}.mp4-row-static:hover{background:transparent}
.mp4-row .mp4-dot{margin-top:8px}
.mp4-row-body{flex:1;min-width:0;display:flex;flex-direction:column}
.mp4-row-top{display:flex;justify-content:space-between;gap:8px;min-width:0}
.mp4-time-row{align-items:flex-start}.mp4-time-row>.mp4-mono{width:40px;flex:none;line-height:24px}
.mp4-tree{display:flex;flex-direction:column}.mp4-tree-sum{margin:0 0 8px;font-size:14px;line-height:24px}
.mp4-tree-row{display:flex;align-items:center}.mp4-tree-row .mp4-row{align-items:center;flex:1;min-width:0;width:auto}.mp4-tree-row .mp4-row .mp4-dot{margin-top:0}
.mp4-tree-child{padding-left:20px}.mp4-tree-row .mp4-count,.mp4-tree-row .mp4-more{flex:none}.mp4-tree-row .mp4-link-btn{flex:none;align-self:center}
.mp4-tree-chev{all:unset;cursor:pointer;flex:none;width:20px;text-align:center;color:var(--mp4-fg3)}.mp4-tree-chev:focus-visible{outline:2px solid var(--mp4-fg3);outline-offset:1px;border-radius:4px}
.mp4-tree-title{flex:1}.mp4-tree-meta{flex:none;max-width:45%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:14px;line-height:24px}
.mp4-tree-fold{margin:0 0 0 44px;font-size:14px;color:var(--mp4-fg3);line-height:24px}
.mp4-tree-archive{display:none;padding:4px 8px;font-size:14px;line-height:24px}.mp4-tree-can-archive:hover .mp4-tree-archive,.mp4-tree-can-archive:focus-within .mp4-tree-archive{display:inline}
.mp4-tree-can-archive:hover .mp4-tree-when,.mp4-tree-can-archive:focus-within .mp4-tree-when{display:none}
.mp4-tree-confirm{display:flex;flex-direction:column;gap:8px;margin:4px 0 8px 28px;padding:12px 12px;border-radius:8px;background:var(--mp4-fill)}
.mp4-tree-confirm-actions{display:flex;gap:8px;margin-top:4px}.mp4-sug-danger{color:var(--rx-red-11);border-color:var(--rx-red-6)}
.mp4-tree-error{margin:4px 0 0;font-size:14px;color:var(--rx-red-11);line-height:24px}
.mp4-tree-trash{display:flex;flex-direction:column;margin-top:16px;padding-top:12px;border-top:.5px solid var(--mp4-hair)}
.mp4-tree-gone{gap:12px;padding:4px 0 4px 28px}
.mp4-fold>summary{cursor:pointer;font-size:14px;font-weight:500;color:var(--mp4-fg3);padding:4px 0;line-height:24px}
.mp4-fold-cards{display:flex;flex-direction:column;gap:8px;padding-top:8px}
.mp4-thread{gap:12px;--fill-subtle:var(--mp4-hover);--canvas:var(--mp4-bg);--fill-muted:var(--mp4-fill);--popover:var(--mp4-bg);--float:0 0 0 1px var(--mp4-hair2),0 12px 32px -12px rgba(15,17,21,.24)}
.mp4-setbox{display:flex;flex-direction:column;gap:8px;padding:4px 12px 8px}.mp4-setbox p{margin:0}.mp4-setbox a{color:var(--mp4-fg);text-decoration:underline;overflow-wrap:anywhere}
.mp4-err{margin:0;font-size:14px;color:var(--dsw-alias-state-error-primary,var(--rx-red-9));line-height:24px}
/* 记忆 */
.mp4-mem{display:flex;flex-direction:column;gap:8px;min-width:0}.mp4-mem>p{margin:0}
.mp4-mem-add{display:flex;gap:8px;align-items:center;min-width:0}.mp4-mem-add .mp4-input{flex:1}
.mp4-mem-item{display:flex;flex-direction:column;gap:4px;padding:8px 0;border-top:.5px solid var(--mp4-hair);min-width:0}.mp4-mem-item p{margin:0}
.mp4-mem-text{overflow-wrap:anywhere;color:var(--mp4-fg)}
.mp4-mem-sec{margin:8px 0 0;font-size:14px;font-weight:500;color:var(--mp4-fg2);line-height:24px}
.mp4-mem-foot{display:flex;justify-content:space-between;gap:12px;align-items:baseline;font-size:14px;min-width:0;line-height:24px}.mp4-mem-foot>span:first-child{min-width:0;overflow-wrap:anywhere}
.mp4-mem-acts{display:flex;gap:12px;flex:none}
.mp4-mem-link{all:unset;cursor:pointer;font-size:14px;color:var(--mp4-fg2);line-height:24px}.mp4-mem-link:hover{color:var(--mp4-fg);text-decoration:underline;text-underline-offset:2px}.mp4-mem-link:disabled{opacity:.5;cursor:default}.mp4-mem-link:focus-visible{outline:2px solid var(--mp4-fg3);outline-offset:2px}
.mp4-judge{margin:0;line-height:1.7}
/* 时间线: ChatGPT Computer History's rail — time | 1px rail with a dot | title, description, chips; dashed rail across a break */
.mp4-tl-top{display:flex;flex-direction:column;gap:12px;margin:0 0 20px}
.mp4-tl-top .mp4-live{margin:0;padding:8px 8px;border-radius:8px;background:var(--mp4-fill)}.mp4-live-recent{margin:-8px 0 0 8px;font-size:14px;line-height:24px;color:var(--mp4-fg3)}.mp4-tl-top .mp4-live .mp4-dot{flex:none}
.mp4-tl{display:grid;grid-template-columns:40px 1px minmax(0,1fr);column-gap:16px}
.mp4-tl-time{padding-top:12px;text-align:right;font-size:14px;line-height:24px;font-weight:500;color:var(--mp4-fg3);font-variant-numeric:tabular-nums}
.mp4-tl-rail{position:relative;min-height:100%}
.mp4-tl-rail::before,.mp4-tl-rail::after{content:"";position:absolute;left:0;width:1px;background:var(--mp4-hair2)}
.mp4-tl-rail::before{top:0;height:calc(15px + var(--tl-gap,0px))}.mp4-tl-rail::after{top:calc(25px + var(--tl-gap,0px));bottom:0}
.mp4-tl-gap-before::before,.mp4-tl-gap-after::after{background:repeating-linear-gradient(to bottom,var(--mp4-hair2) 0 2px,transparent 2px 5px)}
.mp4-tl:first-child .mp4-tl-rail::before,.mp4-tl:last-child .mp4-tl-rail::after{display:none}
.mp4-tl-dot{position:absolute;left:50%;top:calc(17px + var(--tl-gap,0px));width:6px;height:6px;border-radius:50%;transform:translateX(-50%)}
.mp4-tl-same .mp4-tl-dot{width:4px;height:4px;opacity:.7}.mp4-tl-until{display:block;font-size:14px;font-weight:400;opacity:.8;line-height:24px}
.mp4-tl-time{padding-top:calc(12px + var(--tl-gap,0px)) !important}
.mp4-tl-body,.mp4-tl-main{padding-top:calc(10px + var(--tl-gap,0px)) !important;padding-bottom:8px !important}
.mp4-tl-body{min-width:0;padding:12px 0 24px}.mp4-tl:last-child .mp4-tl-body{padding-bottom:8px}
.mp4-tl-head{display:flex;align-items:center;gap:8px;min-width:0}
.mp4-tl-title{display:block;margin:0;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:14px;line-height:24px;font-weight:500;color:var(--mp4-fg)}
.mp4-tl-desc{display:block;margin:4px 0 0;font-size:14px;line-height:24px;color:var(--mp4-fg2)}
.mp4-tl-chips{display:flex;flex-wrap:wrap;gap:8px;margin-top:8px}
.mp4-tl-chip{all:unset;box-sizing:border-box;font-size:14px;line-height:24px;padding:0px 8px;border-radius:8px;background:var(--mp4-fill);color:var(--mp4-fg2)}.mp4-tl-len{background:none;padding-left:0;color:var(--mp4-fg3)}
button.mp4-tl-chip{cursor:pointer;color:var(--mp4-fg)}button.mp4-tl-chip:hover{background:var(--mp4-hover)}
.mp4-tl-act{all:unset;cursor:pointer;flex:none;margin-left:auto;font-size:14px;line-height:24px;padding:0 8px;border-radius:8px;color:var(--mp4-fg3);opacity:0;transition:opacity 250ms ease-out}
.mp4-tl:hover .mp4-tl-act,.mp4-tl:focus-within .mp4-tl-act{opacity:1}.mp4-tl-act:hover{background:var(--mp4-hover);color:var(--mp4-fg)}
@media (hover:none){.mp4-tl-act{opacity:1}}
.mp4-tl-main{all:unset;box-sizing:border-box;cursor:pointer;display:block;min-width:0;padding:12px 0 24px}.mp4-tl-main:hover .mp4-tl-title{text-decoration:underline;text-underline-offset:2px}.mp4-tl-main:focus-visible{outline:2px solid var(--rx-blue-9);outline-offset:2px;border-radius:4px}
.mp4-source-text{padding:12px;border-radius:8px;background:var(--mp4-fill);line-height:1.7}
.mp4-card{all:unset;box-sizing:border-box;cursor:pointer;display:flex;gap:12px;align-items:flex-start;padding:12px 16px;border:.5px solid var(--mp4-hair);border-radius:8px;background:var(--mp4-bg)}.mp4-card:hover{background:var(--mp4-hover)}
.mp4-card-body{flex:1;min-width:0;display:flex;flex-direction:column;gap:4px}
.mp4-card-top{display:flex;justify-content:space-between;gap:12px}
.mp4-wait-card .mp4-dot{margin-top:8px}
.mp4-app{display:inline-flex;flex:none;align-items:center;justify-content:center;overflow:hidden;border-radius:27%;font-weight:500;font-style:normal;line-height:1;box-shadow:inset 0 0 0 .5px rgba(0,0,0,.14)}.mp4-app svg{display:block}
.mp4-tl-app{display:inline-flex;align-items:center;gap:4px;padding-left:4px}
.mp4-k-row{display:flex;align-items:center;justify-content:space-between;gap:8px}.mp4-k-row .mp4-k{margin:0}.mp4-k-row .mp4-link-btn{align-self:auto;padding:0;font-size:14px;line-height:24px}
.mp4-wgroup{flex:none;display:flex;flex-direction:column;gap:4px}
.mp4-wgroup-head{display:flex;align-items:center;gap:8px;padding:0 8px 4px;font-size:14px;line-height:24px;color:var(--mp4-fg3)}
.mp4-wgroup-name{all:unset;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-weight:500;color:var(--mp4-fg2)}button.mp4-wgroup-name{cursor:pointer}button.mp4-wgroup-name:hover{color:var(--mp4-fg);text-decoration:underline;text-underline-offset:3px}
.mp4-wgroup-n{flex:none;font-variant-numeric:tabular-nums}
.mp4-wgroup-note{margin:0 8px 4px;font-size:14px;line-height:24px;color:var(--mp4-fg3)}
.mp4-wrow{all:unset;box-sizing:border-box;cursor:pointer;display:flex;gap:12px;align-items:flex-start;width:100%;padding:8px 8px;border-radius:8px}.mp4-wrow:hover{background:var(--mp4-hover)}.mp4-wrow:focus-visible{outline:2px solid var(--rx-blue-9);outline-offset:-2px}
.mp4-wrow-lead{flex:none;display:inline-flex;align-items:center;justify-content:center;width:16px;height:20px}.mp4-wrow-lead .mp4-app{border-radius:4px}
.mp4-wmark{display:inline-flex}.mp4-status{display:block;flex:none}.mp4-row>.mp4-status{margin-top:4px}.mp4-tree-row .mp4-row>.mp4-status{margin-top:0}.mp-workspace-recent>.mp4-status{margin-top:4px}
.mp4-pmark{display:inline-flex;align-items:center;justify-content:center;flex:none;border-radius:8px;line-height:1;font-style:normal}.mp4-pmark-plain{border-radius:0;background:none;box-shadow:none}
.mp-workspaces .mp-workspace-project>.mp4-pmark{flex:none;overflow:visible}.mp4-pmark svg{display:block}.mp4-project-card .mp4-pmark{border-radius:8px}
.mp4-wgroup-name{display:inline-flex;align-items:center;gap:8px}.mp4-crumb-up .mp4-pmark{margin-right:4px}
.mp4-iconpick{display:flex;flex-direction:column;gap:8px;padding:12px 12px;border:.5px solid var(--mp4-hair,var(--rx-gray-6));border-radius:8px}
.mp4-iconpick-head{display:flex;align-items:center;gap:12px}.mp4-iconpick-head>span{display:flex;flex-direction:column}
.mp4-iconpick-grid{display:grid;grid-template-columns:repeat(8,1fr);gap:4px}
.mp4-iconpick-emoji{all:unset;cursor:pointer;display:flex;align-items:center;justify-content:center;height:30px;border-radius:8px}.mp4-iconpick-emoji:hover{background:var(--mp4-hover,var(--rx-gray-3))}
.mp4-iconpick-colors{display:flex;flex-wrap:wrap;gap:8px}.mp4-iconpick-color{all:unset;cursor:pointer;width:18px;height:18px;border-radius:50%}
.mp4-iconpick-on{box-shadow:0 0 0 2px var(--mp4-bg,#fff),0 0 0 4px var(--rx-blue-9)}.mp4-iconpick-emoji.mp4-iconpick-on{background:var(--mp4-fill,var(--rx-gray-3));box-shadow:none}
.mp4-wrow-body{flex:1;min-width:0;display:flex;flex-direction:column;gap:4px}
.mp4-wrow-top{display:flex;align-items:baseline;gap:8px}.mp4-wrow-top b{flex:1;min-width:0;font-weight:500;color:var(--mp4-fg)}
.mp4-wrow-when{flex:none;font-size:14px;color:var(--mp4-fg3);font-variant-numeric:tabular-nums;line-height:24px}
.mp4-wrow-sub{display:flex;align-items:center;gap:4px;min-width:0;font-size:14px;line-height:24px;color:var(--mp4-fg3)}.mp4-wrow-sub .mp-av,.mp4-wrow-sub .mp4-app{flex:none}


.mp4-wmore{all:unset;cursor:pointer;margin:0 8px;font-size:14px;line-height:24px;color:var(--mp4-fg3)}.mp4-wmore:hover{color:var(--mp4-fg)}
/* 待你决定, sizes only go down: count 12 · project 14 strong · task 14 regular · detail 12 · time 11. A project and its
   tasks share a size (as Linear and Multica group lists do); weight, the emoji and the space above tell them apart. */
.mp4-k-row .mp4-k{font-size:14px;line-height:24px;font-weight:500;color:var(--mp4-fg3)}
.mp4-wgroup-head{padding:4px 8px 4px;font-size:14px;line-height:24px}.mp4-wgroup-name{font-weight:500;color:var(--mp4-fg)}button.mp4-wgroup-name:hover{color:var(--mp4-fg)}
.mp4-wgroup-name .mp4-pmark{font-size:14px !important;width:16px !important;height:16px !important}.mp4-wgroup-n{font-size:14px;line-height:24px}
.mp4-wrow-top b{font-size:14px;line-height:24px;font-weight:500}.mp4-wrow-when{font-size:14px;line-height:24px}.mp4-wrow-sub{font-size:14px;line-height:24px}
.mp4-wmark svg{width:15px;height:15px}
.mp4-part+.mp4-part{margin-left:.75em}
.mp4-fallback{display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin:0 0 12px;padding:8px 12px;border-radius:8px;font-size:14px;line-height:24px;color:var(--mp4-fg2);background:var(--mp4-fill)}.mp4-fallback .mp4-link-btn{padding:0;font-size:14px;line-height:24px}.mp4-wmore{margin:4px 0 0 36px}
.mp4-app-sticker{box-shadow:none;border-radius:0;background:none}

.mp-workers-fold{margin:4px 0}.mp-workers-fold>summary{cursor:pointer;font-size:14px;color:var(--fg-muted);line-height:24px}.mp-workers-fold[open]>summary{margin-bottom:8px}
.mp4-follow-card .mp4-app{margin-top:0px}.mp4-adv-k{margin:16px 0 0;padding:0 12px}
.mp4-follow-k{margin-top:20px}
.mp4-follow-card{cursor:default}.mp4-follow-card:hover{background:var(--mp4-bg)}.mp4-follow-card .mp4-dot{margin-top:8px}
.mp4-follow-quote{margin:4px 0;padding-left:8px;font-size:14px;line-height:24px;color:var(--mp4-fg2);overflow-wrap:anywhere}
button.mp4-dock-note-text{all:unset;box-sizing:border-box;cursor:pointer;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;border-radius:4px}button.mp4-dock-note-text:hover{color:var(--mp4-fg)}button.mp4-dock-note-text:focus-visible,.mp4-dock-x:focus-visible{outline:2px solid var(--mp4-fg3);outline-offset:1px}
.mp4-dock-x{all:unset;box-sizing:border-box;flex:none;display:inline-flex;align-items:center;justify-content:center;width:20px;height:20px;border-radius:4px;color:var(--mp4-fg3);cursor:pointer}.mp4-dock-x:hover{background:var(--mp4-hover);color:var(--mp4-fg)}.mp4-dock-x:disabled{opacity:.5;cursor:default}
.mp4-exec{display:flex;gap:8px;align-items:center;min-width:0}.mp4-exec .mp4-agent-select{flex:1;min-width:0;width:auto}.mp4-exec .mp4-exec-harness{flex:0 1 auto;max-width:42%}
.mp4-exec-foot{display:flex;align-items:center;gap:8px;margin-top:8px;font-size:14px;min-width:0;line-height:24px}.mp4-exec-foot .mp4-mem-link{flex:none;margin-left:auto}
.mp4-trig-row{box-sizing:border-box;display:flex;align-items:center;gap:8px;width:100%;padding:4px 0;font-size:14px;line-height:24px;color:var(--mp4-fg3);min-width:0}.mp4-trig-row svg{flex:none}
.mp4-trig-said{display:flex;flex-direction:column;align-items:flex-end;gap:4px;margin:4px 0}.mp4-trig-from{display:inline-flex;align-items:center;gap:4px;font-size:14px;color:var(--mp4-fg3);line-height:24px}
.mp4-trig-bubble{max-width:77%;min-width:0;overflow-wrap:anywhere;padding:8px 12px;border-radius:8px;background:var(--rx-gray-3);color:var(--mp4-fg);font-size:var(--dsh-content-font-size,14px);line-height:1.6}.mp4-trig-bubble p{margin:0}
.mp4-trig-divider{display:flex;align-items:center;gap:12px;margin:8px 0;font-size:14px;color:var(--mp4-fg3);line-height:24px}.mp4-trig-divider::before,.mp4-trig-divider::after{content:"";flex:1;border-top:.5px solid var(--mp4-hair)}
.mp4-trig-title{flex:none;color:var(--mp4-fg2)}.mp4-trig-detail{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.mp4-trig-time{flex:none;margin-left:auto;font-size:14px;line-height:24px}
.mp4-scene{display:block;flex:none}.mp4-scene-b{visibility:hidden}
@keyframes mp4-scene-a{0%,49.99%{visibility:visible}50%,100%{visibility:hidden}}@keyframes mp4-scene-b{0%,49.99%{visibility:hidden}50%,100%{visibility:visible}}
.mp4-scene .mp4-scene-a{animation:mp4-scene-a 1.2s steps(1,end) infinite}.mp4-scene .mp4-scene-b{animation:mp4-scene-b 1.2s steps(1,end) infinite}
.mp4-scene-sleeping .mp4-scene-a,.mp4-scene-sleeping .mp4-scene-b{animation-duration:2.4s}
@media (prefers-reduced-motion:reduce){.mp4-scene .mp4-scene-a{animation:none}.mp4-scene .mp4-scene-b{animation:none;visibility:hidden}}
.mp4-kanban-name{cursor:default;display:inline-flex;align-items:center;gap:8px;flex:none!important}.mp4-wrow-state.mp4-krow-lane{color:var(--mp4-fg3)}.mp4-kanban-head-acts{margin-left:auto;display:inline-flex;gap:12px}.mp4-krow{cursor:default}.mp4-krow-acts{margin-left:auto;flex:none;display:inline-flex;gap:12px;font-size:14px;color:var(--mp4-fg3)}.mp4-krow-done .mp4-wrow-top b{color:var(--mp4-fg3);font-weight:400}
.mp4-skill-diff{display:flex;flex-direction:column;gap:8px;padding:12px 0 12px 60px}.mp4-skills .mp4-skill-diff{padding-left:0}.mp4-skill-why{margin:0;font-size:14px;line-height:24px;color:var(--mp4-fg)}.mp4-skill-lines{display:flex;flex-direction:column;gap:4px;font-size:14px;line-height:24px;background:var(--mp4-fill);border-radius:8px;padding:8px 12px}.mp4-skill-gone{color:var(--rx-red-11);text-decoration:line-through;text-decoration-color:color-mix(in srgb,currentColor 40%,transparent)}.mp4-skill-came{color:var(--rx-green-11)}.mp4-skill-kept{color:var(--mp4-fg3);font-size:14px}.mp4-skill-k{margin:0;font-size:14px;line-height:24px;color:var(--mp4-fg3)}.mp4-skill-ref{margin:0;font-size:14px;line-height:24px;color:var(--mp4-fg2)}.mp4-setup.mp4-skills{display:flex;flex-direction:column;align-items:stretch;gap:0}.mp4-skill-head,.mp4-skill-row,.mp4-skill-open{width:100%;box-sizing:border-box}.mp4-skill-head,.mp4-skill-row{display:grid;grid-template-columns:minmax(0,1fr) 48px 48px 96px;gap:8px;align-items:center;font-size:14px;line-height:24px}.mp4-skill-head{font-size:14px;color:var(--mp4-fg3);padding:0 0 8px}.mp4-skill-row{all:unset;box-sizing:border-box;display:grid;grid-template-columns:minmax(0,1fr) 48px 48px 96px;gap:8px;padding:8px 0;border-top:.5px solid var(--mp4-hair);cursor:pointer;color:var(--mp4-fg)}.mp4-skill-flag{color:var(--rx-yellow-11);font-weight:500}.mp4-skill-open{display:flex;flex-direction:column;gap:8px;padding:4px 0 12px}.mp4-skill-now{margin:0;font-size:14px;line-height:24px;color:var(--mp4-fg);white-space:pre-wrap}.mp4-skill-ver{display:flex;justify-content:space-between;gap:12px;font-size:14px;line-height:24px;color:var(--mp4-fg2);padding:4px 0}
.mp4-banner{display:flex;align-items:center;gap:12px;padding:12px;border:.5px solid var(--mp4-hair2);border-radius:8px;background:var(--mp4-fill)}.mp4-banner-icon{flex:none;display:inline-flex;width:20px;height:20px}
.mp4-banner-line{margin:0;display:flex;align-items:center;gap:8px;min-width:0;font-size:14px;line-height:24px;color:var(--mp4-fg)}.mp4-banner-line>svg{flex:none}.mp4-banner-line>.mp4-muted{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.mp4-banner-text{flex:1;min-width:0;display:flex;flex-direction:column;gap:4px}.mp4-banner-text b{font-size:14px;line-height:24px;font-weight:500;color:var(--mp4-fg)}.mp4-banner-text span{font-size:14px;line-height:24px;color:var(--mp4-fg3);overflow-wrap:anywhere}
.mp4-banner .mp4-btn{flex:none;height:32px;padding:0 12px;border-radius:8px;font-weight:500}.mp4-btn.mp4-btn-strong{background:var(--rx-gray-12);color:var(--rx-gray-1);border-color:var(--rx-gray-12)}.mp4-btn.mp4-btn-strong:hover{background:var(--rx-gray-11);border-color:var(--rx-gray-11)}.mp4-btn[disabled]{opacity:.5;cursor:default}
.mp4-quote{margin:8px 0 0;display:flex;flex-direction:column;gap:4px}.mp4-quote blockquote{margin:0;padding:0 0 0 12px;border-left:2px solid var(--mp4-hair2);font-size:14px;line-height:24px;color:var(--mp4-fg)}.mp4-quote figcaption{padding-left:12px;font-size:14px;line-height:24px;color:var(--mp4-fg3)}
.mp4-now{margin:0 0 8px;font-size:14px;line-height:24px;color:var(--mp4-fg)}.mp4-fold-body{margin:4px 0 0}
.mp4-agents-top{display:flex;justify-content:flex-end}.mp4-btn.mp4-askmira,.mp4-link-btn.mp4-askmira{align-self:flex-start;display:inline-flex;align-items:center;gap:4px}.mp4-strategy-k{display:inline-flex;gap:8px}.mp4-strategy-links{display:inline-flex;align-items:center;gap:16px}
.mp4-tabpane{--hair:var(--mp4-hair2);--card:var(--mp4-bg);--canvas:var(--mp4-bg);--fill-medium:var(--mp4-hover);--fill-muted:var(--mp4-fill);--fg:var(--mp4-fg);--fg-muted:var(--mp4-fg2);--fg-hint:var(--mp4-fg3);--strong:var(--mp4-fg3);--accent:var(--mp4-fg2);--accent-soft:var(--mp4-hover);--on-accent:var(--mp4-bg)}
.mp4-tabpane .mp-create>h2{display:none}.mp4-tabpane .mp-create label{font-size:14px;line-height:24px;font-weight:500;color:var(--mp4-fg2)}
.mp4-tabpane .mp-create input,.mp4-tabpane .mp-create textarea,.mp4-tabpane .mp-create select{border:.5px solid var(--rx-gray-7);border-radius:8px;font-size:14px;line-height:24px;background:var(--rx-gray-1,#fff)}
.mp4-tabpane .mp-btn,.mp-handoff-dialog .mp-btn{height:28px;padding:0 12px;border:.5px solid var(--rx-gray-7);border-radius:8px;font-size:14px;font-weight:500;background:transparent;color:var(--mp4-fg,var(--fg))}
.mp4-tabpane .mp-btn.mp-btn-primary,.mp4-tabpane .mp-btn.mp-btn-strong,.mp-handoff-dialog .mp-btn.mp-btn-strong{background:var(--rx-gray-12);color:var(--rx-gray-1);border-color:var(--rx-gray-12)}
.mp4-tabpane .mp-btn[disabled],.mp-handoff-dialog .mp-btn[disabled]{opacity:.45}
.mp-handoff-form select{padding:8px 12px;border-color:var(--rx-gray-7);border-radius:8px;font-size:14px;line-height:24px}
.mp-trace-dialog.mp-connectors-dialog[open]{height:auto}.mp-connectors-dialog .mp-source-body{flex:none}
.mp-observation-item:has(.mp-observation-no-image) .mp-observation-no-image{display:none}
.mp4-file-item+.mp4-file-item{margin-top:12px}.mp4-file-acts{display:flex;align-items:center;gap:12px;padding:4px 4px 0;font-size:14px}.mp4-file-versions summary{cursor:pointer;color:var(--mp4-fg2);list-style:none}.mp4-file-versions summary::-webkit-details-marker{display:none}.mp4-file-versions summary::after{content:" ›"}.mp4-file-versions[open] summary::after{content:" ‹"}.mp4-file-versions a{display:block;color:var(--mp4-fg2);margin-top:4px}
.mp4-setbox .mp4-sug{border-color:var(--rx-gray-7);border-radius:8px;padding:4px 12px}.mp4-setbox .mp4-sug-on{background:var(--rx-gray-4);border-color:var(--rx-gray-8);font-weight:500}
.mp4-iconpick-closed{padding-bottom:12px}.mp4-iconpick-head{display:flex;align-items:center;gap:12px}.mp4-iconpick-toggle{margin-left:auto}
.mp4-tabpane .mp-btn{width:auto;align-self:flex-end}.mp4-tabpane .mp-create>.mp-btn,.mp4-tabpane section>.mp-btn{align-self:flex-start}
.mp4-file-pick{position:relative;display:flex;align-items:center;gap:8px;min-width:0;font-weight:400}.mp4-file-input{position:absolute;inset:0;opacity:0;cursor:pointer}.mp4-agent-head{display:flex;align-items:baseline;gap:8px}.mp4-back{display:inline-flex;align-items:center;gap:4px;align-self:flex-start}
.mp4-strategy{display:flex;flex-direction:column;align-items:flex-start;gap:8px;margin-bottom:8px}.mp4-strategy-head{display:flex;justify-content:space-between;align-items:center;align-self:stretch}.mp4-strategy-k{font-size:14px;line-height:24px;color:var(--mp4-fg3)}
.mp4-strategy-input{align-self:stretch;box-sizing:border-box;font:inherit;font-size:14px;line-height:24px;padding:8px 8px;border-radius:8px;border:.5px solid var(--rx-gray-7);background:var(--rx-gray-1,#fff);color:var(--mp4-fg);resize:vertical}.mp4-strategy-acts{display:flex;gap:8px;align-self:flex-end}
.mp4-wakes{margin:0;padding-left:20px;font-size:14px;line-height:24px;color:var(--mp4-fg)}.mp4-wakes li+li{margin-top:4px}
.mp4-prompt{margin:8px 0 0;max-height:320px;overflow:auto;white-space:pre-wrap;overflow-wrap:anywhere;font:12px/18px "SF Mono",Menlo,monospace;color:var(--mp4-fg2);background:var(--mp4-fill);border-radius:8px;padding:12px}
.mp4-setup{display:flex;flex-direction:column;align-items:flex-start;gap:8px}.mp4-inputs{list-style:none;margin:0;padding:0;font-size:14px;line-height:24px;color:var(--mp4-fg2)}
.mp4-tline{list-style:none;margin:0;padding:0;display:flex;flex-direction:column}.mp4-tline li{position:relative;display:grid;grid-template-columns:14px minmax(0,1fr) auto;gap:8px;padding:4px 0}
.mp4-tline li+li::before{content:"";position:absolute;left:6.5px;top:-6px;height:15px;width:1px;background:var(--mp4-hair2)}
.mp4-tline-mark{display:flex;align-items:center;justify-content:center;height:20px}.mp4-tline-mark i{width:6px;height:6px;border-radius:999px;background:var(--rx-gray-8)}.mp4-tline-you .mp4-tline-mark i{background:var(--rx-gray-12)}
.mp4-tline-text{min-width:0;font-size:14px;line-height:24px;color:var(--mp4-fg2);overflow-wrap:anywhere}.mp4-tline-said .mp4-tline-text{color:var(--mp4-fg)}
.mp4-tline time{font-size:14px;line-height:24px;color:var(--mp4-fg3);white-space:nowrap}.mp4-tline-more{margin-bottom:4px;font-size:14px}.mp4-says{margin:0;font-size:14px;line-height:24px;color:var(--mp4-fg);overflow-wrap:anywhere}
.mp4-clampbox{display:flex;flex-direction:column;align-items:flex-start;min-width:0}.mp4-clamp{display:-webkit-box;-webkit-box-orient:vertical;-webkit-line-clamp:3;overflow:hidden}.mp4-clamp-more{margin-top:4px;font-size:14px;line-height:24px}
.mp4-dfile{border:.5px solid var(--mp4-hair2);border-radius:8px;overflow:hidden}.mp4-dfile+.mp4-dfile{margin-top:8px}.mp4-dfile-row{display:flex;align-items:center}
.mp4-doc{display:flex;flex-direction:column;height:100%;min-height:0}.mp4-doc-bar{flex:none;display:flex;align-items:center;gap:8px;padding:8px 12px 8px 16px;border-bottom:.5px solid var(--mp4-hair)}.mp4-doc-meta{flex:1;min-width:0;font-size:14px;color:var(--mp4-fg2)}.mp4-doc-bar .mp4-dfile-dl{margin-right:0}.mp4-doc-body{flex:1;min-height:0;overflow:auto;padding:20px 24px 40px}.mp4-doc-body .mp4-dfile-md{max-width:760px;margin:0 auto;font-size:14px;line-height:24px}.mp4-doc-body-wide{padding:0}.mp4-doc-body-wide .mp4-dfile-frame{height:100%;min-height:520px}
.mp4-dfile-main{all:unset;box-sizing:border-box;flex:1;min-width:0;display:flex;align-items:center;gap:12px;padding:8px 12px;cursor:pointer;color:var(--mp4-fg2)}.mp4-dfile-main:hover{background:var(--mp4-hover)}.mp4-dfile-main[disabled]{cursor:default;background:none}
.mp4-dfile-text{display:flex;flex-direction:column;min-width:0}.mp4-dfile-text b{font-size:14px;line-height:24px;font-weight:500;color:var(--mp4-fg)}.mp4-dfile-text>span{font-size:14px;line-height:24px;color:var(--mp4-fg3)}
.mp4-dfile-dl{flex:none;display:inline-flex;align-items:center;justify-content:center;width:32px;height:32px;margin-right:4px;border-radius:8px;color:var(--mp4-fg2)}.mp4-dfile-dl:hover{background:var(--mp4-hover);color:var(--mp4-fg)}
.mp4-dfile-preview{border-top:.5px solid var(--mp4-hair);max-height:360px;overflow:auto;padding:12px;background:var(--mp4-bg)}.mp4-dfile-md{font-size:14px;line-height:24px;color:var(--mp4-fg)}.mp4-dfile-md :is(h1,h2,h3,h4){font-size:14px;line-height:24px;font-weight:600;margin:12px 0 4px}.mp4-dfile-md>:first-child,.mp4-dfile-md>*>:first-child{margin-top:0}.mp4-dfile-md :is(p,ul,ol,blockquote,table){margin:0 0 8px}
.mp4-dfile-preview pre{margin:0;white-space:pre-wrap;overflow-wrap:anywhere;font:13px/20px var(--dsw-font-family,-apple-system,"PingFang SC",sans-serif);color:var(--mp4-fg)}
.mp4-meta-row{display:flex;align-items:center;gap:12px;padding-top:12px;border-top:.5px solid var(--mp4-hair);font-size:14px;line-height:24px}.mp4-meta-row .mp4-k{margin:0;flex:none}.mp4-meta-row .mp4-ell{flex:1}.mp4-meta-row .mp4-files-link{margin:0;flex:none}
.mp4-check-evidence.mp4-check-done>span:last-child{color:var(--mp4-fg)}
.mp4-headline{font-size:14px;line-height:24px;font-weight:600;color:var(--mp4-fg)}.mp4-note-soft{margin:4px 0 0;font-size:14px;line-height:24px;color:var(--mp4-fg)}
.mp4-grid .mp4-k{font-weight:400}
.mp4-studio-veil{position:fixed;inset:0;z-index:60;background:rgba(0,0,0,.32);display:flex;align-items:center;justify-content:center;padding:16px;pointer-events:auto}
.mp4-studio{width:min(720px,100%);max-height:calc(100vh - 32px);overflow:auto;display:flex;flex-direction:column;gap:12px;padding:16px;border-radius:8px;background:var(--rx-gray-1,#fff);color:var(--mp4-fg);box-shadow:0 0 0 .5px rgba(0,0,0,.1),0 24px 64px -16px rgba(0,0,0,.4);font-size:14px;line-height:24px}
.mp4-studio-head{display:flex;align-items:center;gap:12px}.mp4-studio-head>b{font-size:14px;line-height:24px}.mp4-studio-steps{display:flex;gap:4px;flex:1}
.mp4-studio-step{all:unset;cursor:pointer;padding:4px 8px;border-radius:8px;font-size:14px;color:var(--mp4-fg3);line-height:24px}.mp4-studio-step-on{background:var(--rx-gray-3);color:var(--mp4-fg)}
.mp4-studio-main{display:flex;gap:16px;align-items:flex-start;flex-wrap:wrap}
.mp4-studio-stage{flex:none;width:176px;display:flex;flex-direction:column;align-items:center;gap:4px;padding:8px;border-radius:8px;background:var(--rx-gray-2)}.mp4-studio-stage .mp4-scene{width:160px;height:120px}.mp4-studio-stage>span{font-size:14px;text-align:center;line-height:24px}
.mp4-studio-stage{gap:8px;padding:20px 8px 16px}.mp4-studio-stage>b{font-size:14px;line-height:24px}
.mp4-swatches{display:flex;flex-wrap:wrap;gap:8px}.mp4-swatch{all:unset;box-sizing:border-box;cursor:pointer;width:24px;height:24px;border-radius:50%;box-shadow:0 0 0 .5px rgba(0,0,0,.12);transition:box-shadow 250ms ease-out}.mp4-swatch-on{box-shadow:0 0 0 2px var(--rx-gray-1,#fff),0 0 0 3.5px currentColor}.mp4-swatch:focus-visible{outline:2px solid var(--rx-gray-8);outline-offset:3px}
.mp4-entry-dot{display:inline-grid;place-items:center;flex:none}
.mp4-head-avatar{display:inline-flex;align-items:center;margin-right:8px}.mp4-portrait{display:block;flex:none}
/* A task link opens the task here, not a web page: no globe in front of it. */
a[href^="https://task.local/"]>svg:first-child{display:none}
/* DESIGN.md: one set of tokens and components for 待你决定, 时间线 and 项目与任务 (direction 2: white, hairline cards). Lines and hover are mixed from the text colour, so dark mode follows by itself. */
:root{--mp4-t-line:color-mix(in srgb,var(--rx-gray-12) 8%,transparent);--mp4-t-border:color-mix(in srgb,var(--rx-gray-12) 14%,transparent);--mp4-t-hover:color-mix(in srgb,var(--rx-gray-12) 4%,transparent);--mp4-r-card:12px;--mp4-r-ctl:8px;--mp4-r-mark:4px}
.mp4-bd{display:flex;flex-direction:column;gap:24px;padding:4px 0 24px;color:var(--mp4-fg)}
.mp4-bd-sec{display:flex;flex-direction:column;gap:12px}
.mp4-bd-head{display:flex;align-items:center;gap:8px;padding:0 4px}.mp4-bd-head>b{font-size:16px;line-height:24px;font-weight:600}.mp4-bd-count{margin-left:auto;font-size:14px;line-height:24px;color:var(--mp4-fg2)}.mp4-bd-head .mp4-kb-tool{margin-left:4px}
.mp4-bd-proj,.mp4-bd-card{border:.5px solid var(--mp4-t-border);border-radius:var(--mp4-r-card)}.mp4-bd-proj{padding:16px 16px 4px}.mp4-bd-card{padding:4px}
.mp4-bd-proj-h{all:unset;box-sizing:border-box;cursor:pointer;display:flex;align-items:center;gap:8px;width:100%;margin-bottom:4px}.mp4-bd-proj-h>b{flex:1;min-width:0;font-size:14px;line-height:24px;font-weight:600}.mp4-bd-proj-h time,.mp4-bd-row time,.mp4-bd-msg time{flex:none;font-size:14px;line-height:24px;color:var(--mp4-fg2)}.mp4-bd-proj-h:hover>b{text-decoration:underline;text-underline-offset:3px;text-decoration-thickness:.5px}
.mp4-bd-story{margin:0 0 12px 24px;font-size:14px;line-height:24px;color:var(--mp4-fg)}
.mp4-bd-rows{display:flex;flex-direction:column;margin:0 -12px;padding:4px 0;border-top:.5px solid var(--mp4-t-line)}
.mp4-bd-row{all:unset;box-sizing:border-box;cursor:pointer;display:flex;align-items:center;gap:8px;width:100%;min-height:40px;padding:0 12px;border-radius:var(--mp4-r-ctl);font-size:14px;line-height:24px;transition:background 250ms ease-out}.mp4-bd-row:hover,.mp4-bd-msg:hover,.mp4-bd-more:hover{background:var(--mp4-t-hover)}
.mp4-bd-verb{flex:none;color:var(--mp4-fg2)}.mp4-bd-line{flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.mp4-bd-more{all:unset;box-sizing:border-box;cursor:pointer;display:block;width:100%;min-height:40px;padding:8px 12px 8px 36px;border-radius:var(--mp4-r-ctl);font-size:14px;line-height:24px;color:var(--mp4-fg2);transition:background 250ms ease-out}.mp4-bd-more:hover{color:var(--mp4-fg)}
.mp4-bd-msg{position:relative;display:flex;align-items:flex-start;gap:8px;padding:12px;border-radius:var(--mp4-r-ctl);cursor:pointer;transition:background 250ms ease-out}
.mp4-bd-msg+.mp4-bd-msg::before,.mp4-bd-card>.mp4-bd-more::before{content:"";position:absolute;left:12px;right:12px;top:0;border-top:.5px solid var(--mp4-t-line)}.mp4-bd-card>.mp4-bd-more{position:relative}
.mp4-bd-msg-ic{flex:none;width:16px;height:24px;display:grid;place-items:center}
.mp4-kanban-rows{margin:8px 4px 0;padding-top:12px;border-top:.5px solid var(--mp4-hair2)}.mp4-kanban-row{display:flex;align-items:baseline;gap:8px;padding:4px 0;font-size:14px;line-height:24px}.mp4-kanban-lane{flex:none;color:var(--mp4-fg2)}.mp4-kanban-item{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.mp4-kanban-row-acts{flex:none;margin-left:auto;display:flex;gap:12px}.mp4-kanban-row .mp4-link-btn{padding:0;font-size:14px;line-height:24px}
.mp4-bd-msg-b{flex:1;min-width:0;display:flex;flex-direction:column}.mp4-bd-msg-b>b{font-size:14px;line-height:24px;font-weight:500}.mp4-bd-msg-b>span{font-size:14px;line-height:24px;color:var(--mp4-fg2);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.mp4-bd-msg:hover>time{visibility:hidden}.mp4-bd-msg>.mp4-kb-x{opacity:0}.mp4-bd-msg:hover>.mp4-kb-x,.mp4-bd-msg>.mp4-kb-x:focus-visible{opacity:1}
.mp4-bd-msg>.mp4-kb-x{top:12px;right:12px;background:var(--mp4-bg)}
.mp4-tabpane.mp4-core{padding:16px 16px 24px;gap:16px;font-size:14px;line-height:24px;background:var(--mp4-bg)}.mp4-panel-body.mp4-core{gap:16px;font-size:14px;line-height:24px}
.mp4-core .mp4-sec{gap:0}.mp4-core .mp4-sec>.mp4-k{margin:0 0 12px;padding:0 4px;font-size:16px;line-height:24px;font-weight:600;color:var(--mp4-fg)}
.mp4-core .mp4-judge{font-size:14px;line-height:24px;padding:0 4px}
.mp4-core .mp4-tl-top{gap:12px;margin:0}.mp4-core .mp4-tl-top .mp4-live{padding:8px 12px;border-radius:var(--mp4-r-ctl);background:none;border:.5px solid var(--mp4-t-border);font-size:14px;line-height:24px}.mp4-core .mp4-live-recent{margin:-4px 0 0 12px;font-size:14px;line-height:24px;color:var(--mp4-fg2)}
.mp4-core .mp4-sugs{gap:8px}.mp4-core .mp4-sug{font-size:14px;line-height:24px;padding:4px 12px;border-radius:var(--mp4-r-ctl);border-color:var(--mp4-t-border)}.mp4-core .mp4-sug:hover{background:var(--mp4-t-hover)}
.mp4-core .mp4-tl-list{border:.5px solid var(--mp4-t-border);border-radius:var(--mp4-r-card);padding:4px 16px 4px 8px}
.mp4-core .mp4-tl{grid-template-columns:48px 1px minmax(0,1fr);column-gap:12px}
.mp4-core .mp4-tl-time,.mp4-core .mp4-tl-until{font-size:14px;line-height:24px;font-weight:400;color:var(--mp4-fg2)}
.mp4-core .mp4-tl-title{font-size:14px;line-height:24px;font-weight:500}.mp4-core .mp4-tl-desc{font-size:14px;line-height:24px;color:var(--mp4-fg2)}
.mp4-core .mp4-tl-chips{gap:12px;margin-top:4px}.mp4-core .mp4-tl-chip,.mp4-core .mp4-tl-act{font-size:14px;line-height:24px;border-radius:var(--mp4-r-mark)}.mp4-core .mp4-tl-act{padding:0 8px}
.mp4-core .mp4-tree{gap:12px}
.mp4-core .mp4-tree-project{border:.5px solid var(--mp4-t-border);border-radius:var(--mp4-r-card);padding:4px}
.mp4-core .mp4-tree-project>.mp4-tree-row:first-child:has(~ .mp4-tree-child){margin-bottom:4px;box-shadow:0 .5px 0 var(--mp4-t-line)}
.mp4-core .mp4-tree-row{min-height:40px;position:relative}.mp4-core .mp4-tree-row>.mp4-more{position:absolute;right:8px;top:50%;transform:translateY(-50%);background:var(--mp4-bg);border-radius:var(--mp4-r-ctl)}.mp4-core .mp4-tree-row:hover .mp4-count,.mp4-core .mp4-tree-row:focus-within .mp4-count{visibility:hidden}.mp4-core .mp4-tree-row .mp4-row{min-height:40px;padding:0 8px;gap:8px;align-items:center;border-radius:var(--mp4-r-ctl);font-size:14px;line-height:24px}.mp4-core .mp4-tree-row .mp4-row:hover{background:var(--mp4-t-hover)}
.mp4-core .mp4-tree-chev{width:20px;height:24px}.mp4-core .mp4-tree-title{font-weight:400}.mp4-core .mp4-tree-project>.mp4-tree-row:first-child .mp4-tree-title{font-weight:600}
.mp4-core .mp4-count,.mp4-core .mp4-tree-meta,.mp4-core .mp4-tree-fold,.mp4-core .mp4-tree-archive,.mp4-core .mp4-tree-error,.mp4-core .mp4-tree-trash .mp4-link-btn{font-size:14px;line-height:24px;font-weight:400}.mp4-core .mp4-count,.mp4-core .mp4-tree-meta,.mp4-core .mp4-tree-fold{color:var(--mp4-fg2)}
.mp4-core .mp4-tree-child{padding-left:20px}.mp4-core .mp4-tree-fold{margin:0 0 0 52px;padding:8px 0}
.mp4-core .mp4-tree-confirm{margin:4px 8px 8px 28px;padding:12px;border-radius:var(--mp4-r-ctl);background:none;border:.5px solid var(--mp4-t-border);font-size:14px;line-height:24px}
.mp4-core .mp4-tree-trash{margin-top:0;padding:4px 8px;border:.5px solid var(--mp4-t-border);border-radius:var(--mp4-r-card)}.mp4-core .mp4-tree-trash>.mp4-link-btn{min-height:40px;display:flex;align-items:center;padding:0 4px;color:var(--mp4-fg2)}.mp4-core .mp4-tree-gone{gap:8px;min-height:40px;padding:0 4px 0 28px}
/* DESIGN.md: the studio uses the same tokens (radius 12/8/4, one selected style, 16/14 type) */
.mp4-studio{width:min(800px,100%);gap:16px;padding:20px 24px;border-radius:var(--mp4-r-card);box-shadow:0 0 0 .5px var(--mp4-t-border),0 24px 64px rgba(0,0,0,.18);font-size:14px;line-height:24px}
.mp4-studio-head>b{font-size:16px;line-height:24px;font-weight:600}.mp4-studio-steps{justify-content:flex-end}
.mp4-studio-step{padding:0 8px;border-radius:var(--mp4-r-ctl);font-size:14px;line-height:24px;color:var(--mp4-fg2)}.mp4-studio-step-on{background:var(--mp4-t-hover);color:var(--mp4-fg)}
.mp4-studio-main{gap:24px;flex-wrap:nowrap}
.mp4-studio-stage{width:208px;padding:24px 12px 12px;gap:12px;border-radius:var(--mp4-r-card);background:none;border:.5px solid var(--mp4-t-border)}
.mp4-studio-name{width:100%;box-sizing:border-box;font:inherit;font-size:14px;line-height:24px;text-align:center;padding:4px 8px;border-radius:var(--mp4-r-ctl);border:.5px solid var(--mp4-t-border);background:var(--mp4-bg);color:var(--mp4-fg)}.mp4-studio-name:focus{outline:none;border-color:var(--mp4-fg2)}
.mp4-studio-stage>span{font-size:14px;line-height:24px;color:var(--mp4-fg2)}
.mp4-studio-body{gap:16px}.mp4-studio-grp{display:flex;flex-direction:column;gap:8px}.mp4-studio-pair{display:flex;gap:24px;flex-wrap:wrap}
.mp4-studio-q{font-size:14px;line-height:24px;color:var(--mp4-fg2)}
.mp4-picks{display:flex;flex-wrap:wrap;gap:8px}
.mp4-pick{all:unset;box-sizing:border-box;cursor:pointer;width:48px;height:48px;display:grid;place-items:center;border-radius:var(--mp4-r-ctl);border:.5px solid var(--mp4-t-border);transition:background 250ms ease-out}.mp4-pick:hover{background:var(--mp4-t-hover)}.mp4-pick-big{width:64px;height:64px}
.mp4-pick-on,.mp4-pick-on:hover{border-color:transparent;box-shadow:0 0 0 1.5px var(--mp4-fg)}.mp4-pick:focus-visible{outline:2px solid var(--rx-blue-9);outline-offset:2px}
.mp4-studio .mp4-swatches{gap:12px;padding:4px}.mp4-studio .mp4-swatch{width:24px;height:24px}.mp4-studio .mp4-swatch-on{box-shadow:0 0 0 2px var(--mp4-bg),0 0 0 3.5px var(--mp4-fg)}
.mp4-studio .mp4-chip{padding:4px 12px;border-radius:var(--mp4-r-ctl);border:.5px solid var(--mp4-t-border);font-size:14px;line-height:24px}.mp4-studio .mp4-chip-on{background:none;border-color:transparent;box-shadow:0 0 0 1.5px var(--mp4-fg);color:var(--mp4-fg)}
.mp4-studio .mp4-studio-gen input{font-size:14px;line-height:24px;padding:4px 12px;border-radius:var(--mp4-r-ctl);border-color:var(--mp4-t-border)}
.mp4-studio .mp4-key{padding:4px 12px;border-radius:var(--mp4-r-ctl);font-size:14px;line-height:24px}.mp4-studio .mp4-studio-row{grid-template-columns:48px minmax(0,1fr)}
.mp4-studio .mp4-sug{font-size:14px;line-height:24px;padding:4px 16px;border-radius:var(--mp4-r-ctl)}.mp4-studio .mp4-sug:not(.mp4-sug-strong){border-color:transparent;color:var(--mp4-fg2)}
.mp4-studio .mp4-mem-link{font-size:14px;line-height:24px;color:var(--mp4-fg2)}
@media (max-width:640px){.mp4-studio-main{flex-wrap:wrap}.mp4-studio-stage{width:100%}}
.mp4-buddy{display:block;flex:none}.mp4-buddy-blink .mp4-buddy-eyes{transform-box:fill-box;transform-origin:center;animation:mp4-buddy-blink 4.2s steps(1) infinite}
@keyframes mp4-buddy-blink{0%,93%,100%{transform:scaleY(1)}95%{transform:scaleY(.1)}}
.mp4-buddy-glance .mp4-buddy-eyes{animation:mp4-buddy-glance 2.4s steps(1) infinite}
@keyframes mp4-buddy-glance{0%,100%{transform:translateX(0)}25%{transform:translateX(1px)}50%{transform:translateX(0)}75%{transform:translateX(-1px)}}
@media (prefers-reduced-motion:reduce){.mp4-buddy-blink .mp4-buddy-eyes,.mp4-buddy-glance .mp4-buddy-eyes{animation:none}}
html[data-mira-ball=large]{--mira-pet-room:136px}
/* the pet: opens the assistant */
.mp4-ball-wrap{position:fixed;right:22px;bottom:22px;z-index:40;display:flex;flex-direction:column;align-items:flex-end;gap:12px;pointer-events:none}
/* DSH's overlay layer turns pointer events off for everything inside it; the pet and its bubble take them back. */
.mp4-ball-wrap>*,.mp4-ball-wrap>* *{pointer-events:auto!important}
.mp4-ball{all:unset;cursor:pointer;position:relative;display:inline-flex;align-items:center;justify-content:center;border-radius:50%;filter:drop-shadow(0 6px 12px rgba(0,0,0,.18));transition:transform 250ms ease-out}.mp4-ball:focus-visible{outline:2px solid var(--mp4-fg3);outline-offset:2px}
.mp4-ball:hover{transform:translateY(-1px)}
.mp4-ball-bubble{max-width:300px;display:flex;align-items:flex-start;gap:4px;background:var(--rx-gray-1,#FFFFFF);color:var(--mp4-fg);border-radius:8px;box-shadow:0 0 0 .5px rgba(0,0,0,.1),0 12px 32px -12px rgba(0,0,0,.3);padding:8px 8px 8px 12px;font-size:14px;line-height:1.6}
.mp4-ball-bubble>button:first-child{all:unset;cursor:pointer;flex:1}
/* While the pet is on screen it owns the bottom-right corner of the right panel: end the tab's scroll
   area above it, so no row can ever slide under it (padding alone still lets content scroll past). */
html[data-mira-ball] .mp4-tabpane{height:max(0px,calc(100% - var(--mira-pet-room,112px)));-webkit-mask-image:linear-gradient(to bottom,#000 calc(100% - 22px),transparent);mask-image:linear-gradient(to bottom,#000 calc(100% - 22px),transparent)}
html[data-mira-ball] .mp4-panel-body{margin-bottom:var(--mira-pet-room,112px);-webkit-mask-image:linear-gradient(to bottom,#000 calc(100% - 22px),transparent);mask-image:linear-gradient(to bottom,#000 calc(100% - 22px),transparent)}
.mp4-studio-body{flex:1;min-width:260px;display:flex;flex-direction:column;gap:12px}.mp4-studio-q{margin:0;color:var(--mp4-fg2)}
.mp4-studio-row{display:grid;grid-template-columns:40px minmax(0,1fr);gap:8px;align-items:center}.mp4-studio-row .mp4-k{font-weight:400;margin:0}
.mp4-chips{display:flex;flex-wrap:wrap;gap:8px;align-items:center}
.mp4-chip{all:unset;cursor:pointer;padding:4px 12px;border-radius:var(--mp4-r-ctl);border:.5px solid var(--mp4-hair2);font-size:14px;line-height:24px}.mp4-chip:hover{background:var(--mp4-hover)}.mp4-chip-on{background:var(--rx-gray-4);color:var(--rx-gray-12);border-color:var(--rx-gray-8);font-weight:500}
.mp4-swatch{all:unset;cursor:pointer;width:20px;height:20px;border-radius:999px;box-shadow:inset 0 0 0 .5px rgba(0,0,0,.2)}.mp4-swatch-on{box-shadow:0 0 0 2px var(--rx-gray-1,#fff),0 0 0 3px var(--rx-gray-9)}
.mp4-tpls{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:8px}.mp4-tpl{all:unset;cursor:pointer;display:flex;flex-direction:column;align-items:center;gap:4px;padding:8px 4px;border-radius:8px;border:.5px solid var(--mp4-hair);font-size:14px;line-height:24px}.mp4-tpl:hover{background:var(--mp4-hover)}.mp4-tpl-on{border-color:var(--rx-gray-12);background:var(--rx-gray-3)}
.mp4-studio-gen{display:flex;gap:8px}.mp4-studio-gen input,.mp4-studio-input{flex:1;min-width:0;font:inherit;padding:4px 8px;border-radius:8px;border:.5px solid var(--mp4-hair2);background:var(--rx-gray-1,#fff);color:var(--mp4-fg)}
.mp4-key{all:unset;cursor:pointer;padding:4px 12px;border-radius:8px;border:.5px solid var(--mp4-hair2);font-family:"SF Mono",Menlo,monospace;font-size:14px;line-height:24px}.mp4-key-on{border-color:var(--rx-amber-9);color:var(--rx-yellow-11)}
.mp4-studio-foot{display:flex;align-items:center;justify-content:space-between;gap:8px}.mp4-studio-acts{display:flex;gap:8px}
.mp4-sug-strong{background:var(--rx-gray-12);color:var(--rx-gray-1);border-color:var(--rx-gray-12)}.mp4-sug-strong:hover{background:var(--rx-gray-11)}
.mp4-portrait{display:block}
.mp4-index-col{max-width:760px;padding:32px 24px;gap:16px}.mp4-index-head{align-items:center;margin-bottom:0}.mp4-index-head h1{font-size:16px;line-height:24px;font-weight:600}
.mp4-btn{all:unset;box-sizing:border-box;cursor:pointer;display:inline-flex;align-items:center;gap:8px;height:28px;padding:0 12px;border-radius:8px;border:.5px solid var(--mp4-hair2);font-size:14px;color:var(--mp4-fg)}.mp4-btn:hover{background:var(--mp4-hover)}
.mp4-plist{display:flex;flex-direction:column;border-top:.5px solid var(--mp4-hair)}
.mp4-prow{all:unset;box-sizing:border-box;cursor:pointer;display:grid;grid-template-columns:16px minmax(0,1fr) auto;column-gap:12px;align-items:start;padding:12px 8px;border-bottom:.5px solid var(--mp4-hair)}.mp4-prow:hover{background:var(--mp4-hover)}.mp4-prow .mp4-pmark{margin-top:4px}
.mp4-prow-main{display:flex;flex-direction:column;gap:4px;min-width:0}.mp4-prow-name{font-size:14px;line-height:24px;font-weight:500;color:var(--mp4-fg)}
.mp4-prow-desc{font-size:14px;line-height:24px;color:var(--mp4-fg3);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.mp4-prow-meta{display:flex;align-items:center;gap:12px;font-size:14px;line-height:24px;color:var(--mp4-fg3);white-space:nowrap}.mp4-prow-meta>span{display:inline-flex;align-items:center;gap:4px}.mp4-prow-wait{color:var(--rx-yellow-11)}
.mp4-time{font-variant-numeric:tabular-nums;font-size:14px;line-height:24px}
.mp4-check-mark{display:inline-flex;align-items:center;justify-content:center;height:20px}.mp4-check-todo{width:8px;height:8px;border-radius:50%;box-shadow:inset 0 0 0 1px var(--mp4-fg3)}
.mp4-setlist{display:flex;flex-direction:column;border:.5px solid var(--mp4-hair2);border-radius:8px;overflow:hidden}.mp4-setlist>*+*{border-top:.5px solid var(--mp4-hair)}.mp4-setlist,.mp4-setlist>*,.mp4-setlist>.mp4-setrow{flex-shrink:0}.mp4-setlist>:not(.mp4-setrow){padding:12px}
.mp4-setrow{all:unset;box-sizing:border-box;cursor:pointer;display:flex;align-items:center;gap:12px;padding:12px 12px;color:var(--mp4-fg3)}.mp4-setrow:hover{background:var(--mp4-hover)}
.mp4-setrow-text{flex:1;min-width:0;display:flex;flex-direction:column;gap:4px}.mp4-setrow-title{font-size:14px;line-height:24px;font-weight:500;color:var(--mp4-fg)}.mp4-setrow-desc{font-size:14px;line-height:24px;color:var(--mp4-fg3);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.mp4-send:not(:disabled){color:var(--mp4-fg)}
.mp4-agent-select,.mp4-sug,.mp4-guide,.mp4-card,.mp4-btn,.mp4-chip,.mp4-key,.mp4-tpl{border-width:.5px}
.mp4-k,.mp4-sec>.mp4-k{font-weight:500}
.mp4-tree-chev svg,.mp4-disclose svg{transition:transform 250ms ease-out}.mp4-tree-chev[aria-expanded=true] svg,[aria-expanded=true]>.mp4-disclose svg{transform:rotate(90deg)}.mp4-tree-chev{display:inline-flex;align-items:center;justify-content:center;height:20px}
.mp4-disclose{display:inline-flex;align-items:center;gap:4px}.mp4-tree-title{font-weight:500}
.mp4-fold>summary{list-style:none;display:flex;align-items:center;gap:4px;font-weight:500}.mp4-fold>summary::-webkit-details-marker{display:none}
.mp4-fold>summary::before{content:"";flex:none;width:12px;height:12px;background:currentColor;-webkit-mask:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' fill='none' stroke='black' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'%3E%3Cpath d='M9 6l6 6-6 6'/%3E%3C/svg%3E") center/12px no-repeat;mask:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' fill='none' stroke='black' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'%3E%3Cpath d='M9 6l6 6-6 6'/%3E%3C/svg%3E") center/12px no-repeat;transition:transform 250ms ease-out}.mp4-fold[open]>summary::before{transform:rotate(90deg)}
.mp4-agent-select,.mp4-studio-gen input,.mp4-studio-input,.mp4-btn,.mp4-key{border-color:var(--rx-gray-7)}
.mp4-wrow-state{flex:none;font-weight:500;color:var(--rx-yellow-11);margin-right:8px}.mp4-wrow-state-failed{color:var(--rx-red-11)}
.mp4-wgroup-n{margin-left:auto}.mp4-wgroup-head .mp4-wgroup-name{font-weight:500}
.mp4-plist{border-top:none}
.mp4-tree-row .mp4-more{opacity:0;transition:opacity 250ms ease-out}.mp4-tree-row:hover .mp4-more,.mp4-tree-row:focus-within .mp4-more,.mp4-more:has([aria-expanded=true]){opacity:1}
.mp4-chip{border-color:var(--rx-gray-7)}
.mp4-wrow-go{flex:none;margin-left:auto;display:inline-flex;align-items:center;gap:4px;padding-left:8px;color:var(--mp4-fg3)}.mp4-wrow:hover .mp4-wrow-go{color:var(--mp4-fg)}

.mp4-follow-acts{display:flex;flex-wrap:wrap;gap:8px;margin-top:8px}.mp4-runner{align-items:center;margin-top:0}.mp4-runner .mp4-agent-select{width:auto;flex:1;min-width:0}
/* 项目 index */
.mp4-index{width:100%;height:100%;overflow:auto;background:var(--mp4-bg)}
.mp4-index-col{max-width:880px;margin:0 auto;padding:40px 24px;display:flex;flex-direction:column;gap:12px}
.mp4-index-head{display:flex;align-items:flex-end;justify-content:space-between;gap:16px;margin-bottom:8px}
.mp4-index-head h1{margin:0;font-size:16px;line-height:24px}.mp4-index-head p{margin:4px 0 0;color:var(--mp4-fg3);font-size:14px;line-height:24px}
.mp4-tile{flex:none;width:34px;height:34px;border-radius:8px;background:var(--mp4-fill);display:inline-flex;align-items:center;justify-content:center;color:var(--mp4-fg2)}
.mp4-count{font-size:14px;line-height:24px;color:var(--mp4-fg3)}.mp4-count-wait{color:var(--rx-yellow-11);font-weight:500}
/* inside a task's DSH Session */
.mp4-tabpane{padding:16px 16px 24px;display:flex;flex-direction:column;gap:16px;font-size:14px;line-height:24px;height:100%;overflow:auto;box-sizing:border-box}
.mp4-tab-title{margin:0;font-size:16px;line-height:24px;font-weight:600}
.mp4-toolline{display:flex;align-items:center;gap:8px;font-size:14px;line-height:24px;color:var(--mp4-fg3);min-width:0}
.mp4-inbox-head{background:none;border:0;padding:0;margin:0;cursor:pointer;font-family:inherit;text-align:left;max-width:100%;transition:color 250ms ease-out}.mp4-inbox-head:hover{color:var(--mp4-fg)}.mp4-inbox-chev{flex:none;display:inline-flex;transition:transform 250ms ease-out}.mp4-inbox-head[aria-expanded="true"] .mp4-inbox-chev{transform:rotate(90deg)}
.mp4-inbox-list{list-style:none;margin:8px 0 4px 8px;padding:0 0 0 12px;border-left:.5px solid var(--mp4-hair2);display:flex;flex-direction:column;gap:8px;font-size:14px;line-height:24px;color:var(--mp4-fg)}.mp4-inbox-list>li{display:-webkit-box;-webkit-line-clamp:3;-webkit-box-orient:vertical;overflow:hidden}.mp4-inbox-who{color:var(--mp4-fg2);margin-right:8px}
.mp4-posted{font-size:14px;line-height:1.75;padding:4px 0}
.mp4-ask{border:.5px solid var(--mp4-hair2);border-radius:8px;padding:12px;display:flex;flex-direction:column;gap:8px;max-width:560px;font-size:14px;background:var(--mp4-bg);line-height:24px}
.mp4-ask-done{border-color:var(--mp4-hair);font-size:14px;color:var(--mp4-fg2);display:block;line-height:24px}
.mp4-dock-asks{display:flex;flex-direction:column;gap:8px;padding:12px max(var(--dsh-composer-side-clearance,16px),calc((100% - var(--dsh-composer-card-max-width,712px))/2)) 8px;background:linear-gradient(to bottom,transparent,var(--mp4-bg) 12px)}.mp4-dock-asks .mp4-ask{max-width:none;box-shadow:none}
.mp4-ask-k{font-size:14px;font-weight:500;color:var(--rx-yellow-11);line-height:24px}.mp4-ask-q{font-weight:500}
.mp4-opt{all:unset;box-sizing:border-box;cursor:pointer;display:flex;gap:12px;align-items:flex-start;padding:8px 12px;border:.5px solid var(--mp4-hair);border-radius:8px;font-size:14px;line-height:24px}.mp4-opt:hover{background:var(--mp4-hover)}.mp4-opt:disabled{opacity:.6;cursor:default}
.mp4-opt-mark{flex:none;width:12px;height:12px;margin-top:4px;border:1.5px solid var(--mp4-fg3);border-radius:50%}
/* left column additions */
.mp4-entry{position:relative;display:inline-flex}

.mp-workspace-recent .mp4-dot{margin-top:8px}
/* Our cards on DSH's 开始 page: DSH's own guide card, and the cell of a card that belongs to another level hidden */
.mp4-guide-entry{box-sizing:border-box;width:100%;min-height:40px;display:flex;align-items:center;gap:12px;padding:8px 20px;color:var(--dsw-alias-label-primary,var(--rx-gray-12));font:inherit;text-align:left;background:var(--dsw-alias-bg-layer-1,#fff);border:.5px solid var(--dsw-alias-border-l3,var(--rx-gray-6));border-radius:var(--dsl-guide-entry-radius,12px);cursor:pointer}
.mp4-guide-entry:hover{background:var(--dsw-alias-interactive-bg-hover,var(--rx-gray-3))}
.mp4-guide-entry-icon{width:26px;height:26px;flex:none;display:flex;align-items:center;justify-content:center;color:var(--dsw-alias-label-tertiary,var(--rx-gray-11))}
.mp4-guide-entry-text{flex:1 1 0%;min-width:0;display:flex;flex-direction:column;gap:4px}
.mp4-guide-entry-title{font-size:14px;line-height:1.4;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.mp4-guide-entry-desc{font-size:14px;line-height:1.4;color:var(--dsw-alias-label-tertiary,var(--rx-gray-11));white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.mp4-page-head{display:flex;flex-direction:column;gap:4px}.mp4-page-head>p{margin:0;font-size:14px;line-height:24px;color:var(--mp4-fg3);overflow-wrap:anywhere}.mp4-page-head>p:empty{display:none}
.mp4-wgroup-head>.mp4-wgroup-name{flex:0 1 auto}.mp4-wgroup-head>.mp4-wgroup-n{margin-left:0;margin-right:auto;color:var(--mp4-fg3)}.mp4-kanban-name>.mp4-wgroup-n{margin-left:0}
.mp4-setlist>.mp4-dfile{border:0;border-radius:0;margin:0}.mp4-setlist>.mp4-quote{margin:0}.mp4-setlist>.mp4-now{margin:0}.mp4-setlist>.mp4-tline-more{margin:0;text-align:left}.mp4-setlist>.mp4-checks,.mp4-setfold>.mp4-checks{display:flex;flex-direction:column;gap:4px}.mp4-setfold>.mp4-checks{padding:12px}.mp4-setline>.mp4-setline-k.mp4-muted{color:var(--mp4-fg3)}.mp4-setline>.mp4-mem-link{flex:none}.mp4-setlist>.mp4-note{margin:0}
.mp4-setlist>.mp4-strategy{margin:0}.mp4-kanban-name>.mp4-wgroup-n{font-weight:400;color:var(--mp4-fg3)}
.mp4-setlist .mp4-strategy-k{font-size:14px;line-height:24px;font-weight:500;color:var(--mp4-fg)}.mp4-setlist .mp4-strategy-k .mp4-muted{font-weight:400}
.mp4-wrow .mp4-wrow-go{opacity:0;transition:opacity 250ms ease-out}.mp4-wrow:hover .mp4-wrow-go,.mp4-wrow:focus-visible .mp4-wrow-go{opacity:1}@media (hover:none){.mp4-wrow .mp4-wrow-go{opacity:1}}
.mp4-setgroup>h4{display:flex;align-items:baseline;gap:8px}.mp4-setgroup-n{font-weight:400;color:var(--mp4-fg3);font-variant-numeric:tabular-nums}.mp4-setgroup-when{margin-left:auto;font-size:14px;font-weight:400;color:var(--mp4-fg3)}.mp4-setgroup-desc{margin:-4px 0 0;font-size:14px;line-height:24px;color:var(--mp4-fg3)}.mp4-setgroup .mp4-mem-sec{margin:4px 0 0}
.mp4-setlist>.mp4-memrow{display:flex;align-items:flex-start;gap:12px;padding:12px 12px}.mp4-memrow-body{flex:1;min-width:0;display:flex;flex-direction:column;gap:4px}.mp4-memrow-body p{margin:0}.mp4-memrow .mp4-mem-text{font-size:14px;line-height:24px}.mp4-memrow-meta{font-size:14px;line-height:24px;color:var(--mp4-fg3);overflow-wrap:anywhere}.mp4-memrow-acts{flex:none;display:inline-flex;gap:12px;opacity:0;transition:opacity 250ms ease-out}.mp4-memrow:hover .mp4-memrow-acts,.mp4-memrow:focus-within .mp4-memrow-acts{opacity:1}@media (hover:none){.mp4-memrow-acts{opacity:1}}.mp4-setlist>form.mp4-memrow{align-items:center;padding:8px 12px}
.mp4-setgroup{display:flex;flex-direction:column;gap:8px;min-width:0}.mp4-setgroup>h4{margin:0;font-size:14px;line-height:24px;font-weight:600;color:var(--mp4-fg)}.mp4-setgroup>.mp4-askmira{align-self:flex-start}
.mp4-setlist>.mp4-setline,.mp4-setlist>.mp4-setfold>summary{display:flex;align-items:center;justify-content:space-between;gap:12px;min-height:44px;box-sizing:border-box;padding:8px 12px}
.mp4-setline-k{min-width:0;font-size:14px;line-height:24px;font-weight:500;color:var(--mp4-fg);overflow-wrap:anywhere}.mp4-setline-plain{font-weight:400}
.mp4-setline-v{flex:none;max-width:60%;text-align:right;font-size:14px;line-height:24px;color:var(--mp4-fg3);overflow-wrap:anywhere}
.mp4-setline-select.mp4-agent-select{width:auto;max-width:100%;min-width:180px}
.mp4-setlist>.mp4-setfold{padding:0}.mp4-setfold>summary{list-style:none;cursor:pointer}.mp4-setfold>summary::-webkit-details-marker{display:none}.mp4-setfold>summary:hover{background:var(--mp4-hover)}.mp4-setfold[open]>summary{border-bottom:.5px solid var(--mp4-hair)}.mp4-setfold>.mp4-prompt{margin:0;padding:12px;max-height:320px;overflow:auto;border:0;border-radius:0}
div:has(> [data-slot="sidebar.right.tab.guide.entry"] [data-mira-guide-skip]){display:none}
/* Below 768px DSH always fills the window with its right panel (RightbarSeat's autoFullscreen): its "exit fullscreen" button can only close the panel, like the collapse button, and a split leaves two columns too narrow to use, while its icon is all but the collapse one. Keep the one way out. Wider windows keep all three; a split stays on its shortcut and on dragging a tab. */
@media (max-width:767.98px){[data-sidebar-right-mode],[data-dockkit-split-button]{display:none!important}}
@media (max-width:900px){.mp4-panel{position:absolute;right:0;top:0;bottom:0;max-width:92%;z-index:10;box-shadow:-12px 0 32px -16px rgba(0,0,0,.3)}.mp4-frame{position:relative}.mp4-column,.mp4-composer{padding-left:16px;padding-right:16px}.mp4-head{padding:0 12px}}
@media (max-width:1180px){.mp-review-grid{grid-template-columns:minmax(0,1fr)}}
@media (max-width:900px){.mp-trace-dialog{width:100vw;height:100dvh;border:0;border-radius:0}.mp-tabs{padding:0 12px}.mp-board{grid-template-columns:repeat(5,minmax(220px,1fr))}.mp-details,.mp-thread-original-chip{margin-left:0;padding-left:0}.mp-page,.mp-page.mp-with-thread,.mp-page.mp-no-list.mp-with-thread{grid-template-columns:minmax(0,1fr)}.mp-head{padding:0 16px}.mp-chat-scroll{padding:12px 12px 8px}.mp-column.mp-composer-wrap,.mp-composer-wrap{padding:0 12px 12px}.mp-members{gap:4px}.mp-members .mp-member:nth-child(n+4){display:none}}
@media (max-width:520px){.mp-replies,.mp-action{max-width:none}.mp-handoff-row{grid-template-columns:1fr}.mp-msg{padding:4px 4px}.mp-action-body,.mp-action-note,.mp-action-text,.mp-action-acts,.mp-action-why{padding-left:12px}}

/* Older forms and dialogs, brought onto the same spec as the new pages (Codex settings): one bordered card, 12px labels over 32px fields, 13px text, 6px control radius, a strong button in gray-12. */
.mp4-tabpane form.mp-create{padding:16px;border:.5px solid var(--rx-gray-6);border-radius:8px;gap:16px;background:var(--rx-gray-1,#fff)}
.mp4-tabpane .mp-create input,.mp4-tabpane .mp-create textarea,.mp4-tabpane .mp-create select,.mp-handoff-form input,.mp-handoff-form select,.mp-handoff-form textarea{min-height:32px;box-sizing:border-box;border:.5px solid var(--rx-gray-7);border-radius:8px;font-size:14px;line-height:24px;padding:4px 12px;background:var(--rx-gray-1,#fff);color:var(--rx-gray-12)}
.mp-handoff-form label{display:flex;flex-direction:column;gap:8px;font-size:14px;line-height:24px;font-weight:500;color:var(--rx-gray-11)}
.mp4-tabpane .mp-btn,.mp-handoff-dialog .mp-btn{height:32px;padding:0 12px;border-radius:8px;font-size:14px;line-height:24px;font-weight:500}
.mp4-tabpane .mp-btn-primary,.mp4-tabpane .mp-btn-strong,.mp-handoff-dialog .mp-btn-strong{background:var(--rx-gray-12);border-color:var(--rx-gray-12);color:var(--rx-gray-1)}
.mp4-tabpane .mp-create-bar{display:flex;flex-wrap:wrap;gap:8px;align-items:center}.mp4-tabpane .mp-create-bar .mp-btn{align-self:auto}
.mp4-tabpane .mp-member-settings{display:flex;flex-direction:column;gap:4px;padding:12px 16px;border:.5px solid var(--rx-gray-6);border-radius:8px;font-size:14px;line-height:24px}.mp4-tabpane .mp-member-settings p{margin:0}
.mp-handoff-dialog .mp-hint{color:var(--rx-gray-11)}.mp-handoff-form{gap:16px}

.mp-trace-dialog.mp-connectors-dialog{height:auto}
.mp4-krow .mp4-wrow-sub{display:flex;align-items:baseline;gap:8px;min-width:0}.mp4-krow .mp4-wrow-sub>.mp4-ell{flex:1;min-width:0}.mp4-krow .mp4-krow-acts{padding-left:16px;gap:12px}
.mp4-setline-k>small{display:block;font-size:14px;line-height:24px;font-weight:400;color:var(--mp4-fg3)}.mp4-setline-k>.mp4-askmira{display:flex;margin-top:4px;font-size:14px;line-height:24px;font-weight:400}
.mp4-tl-chips{gap:12px;margin-top:8px}.mp4-tl-chip{background:none;padding:0;border-radius:0;color:var(--mp4-fg3)}.mp4-tl-chip:hover{color:var(--mp4-fg);text-decoration:underline;text-underline-offset:3px}
.mp4-krow .mp4-wrow-top{display:flex;align-items:baseline;gap:16px;min-width:0}.mp4-krow .mp4-wrow-top>b{flex:1;min-width:0}.mp4-krow .mp4-wrow-top>.mp4-krow-acts{padding-left:0;font-weight:400}
.mp-observation-item-main>:nth-child(n+3){display:-webkit-box;-webkit-line-clamp:1;-webkit-box-orient:vertical;overflow:hidden}
.mp4-dfile-preview-wide{max-height:none;padding:0}.mp4-dfile-frame{display:block;width:100%;height:520px;border:0;background:#fff}.mp4-dfile-img{display:block;max-width:100%;height:auto;margin:0 auto}
.mp4-setlist>.mp4-file-item{padding:0 0 12px}.mp4-setlist .mp4-file-item .mp4-dfile{border:0;margin:0;border-radius:0}.mp4-setlist .mp4-file-item .mp4-file-acts{display:flex;flex-wrap:wrap;align-items:center;gap:12px;margin:0;padding-left:40px;font-size:14px;line-height:24px}
.mp-retention-dialog .mp-handoff-form>label{flex-direction:row;align-items:center;justify-content:space-between;gap:12px;font-size:14px;line-height:24px;color:var(--rx-gray-12)}.mp-retention-dialog .mp-handoff-form>label>select{width:160px;flex:none}

/* 1:1 with Codex settings (measured from its screen and its stylesheet): page title 28/36, group title 14/20 semibold, row title 13/20 medium, row padding 12/16, card radius 12. */
.mp4-page-head .mp4-tab-title{font-size:16px;line-height:36px;font-weight:600}.mp4-page-head{margin-bottom:16px}
.mp4-setgroup{gap:12px}.mp4-setgroup+.mp4-setgroup{margin-top:12px}.mp4-setgroup>h4{font-size:14px;line-height:24px;font-weight:600}
.mp4-setlist{border-radius:12px}.mp4-setlist>.mp4-setline,.mp4-setlist>.mp4-setfold>summary,.mp4-setlist>.mp4-memrow,.mp4-setlist>.mp4-setrow{padding:12px 16px;min-height:0}.mp4-setlist>.mp4-setline .mp4-setline-k{font-weight:500}
/* 1:1 with Codex's file preview: a toolbar of pill buttons above the preview. */
.mp4-dfile-bar{display:flex;justify-content:flex-end;gap:8px;padding:8px 12px;border-bottom:.5px solid var(--mp4-hair);background:var(--mp4-bg)}
.mp4-pill-btn{all:unset;box-sizing:border-box;cursor:pointer;display:inline-flex;align-items:center;gap:8px;height:28px;padding:0 12px;border-radius:var(--mp4-r-ctl);border:.5px solid var(--rx-gray-7);font-size:14px;line-height:24px;font-weight:500;color:var(--mp4-fg)}.mp4-pill-btn:hover{background:var(--mp4-hover)}.mp4-pill-btn:focus-visible{outline:2px solid var(--rx-blue-9);outline-offset:1px}
.mp4-empty{display:flex;flex-direction:column;align-items:center;gap:8px;padding:72px 16px;text-align:center;color:var(--mp4-fg3)}.mp4-empty>b{font-size:14px;line-height:24px;font-weight:600;color:var(--mp4-fg)}.mp4-empty>p{margin:0;font-size:14px;line-height:24px;color:var(--mp4-fg3)}
.mp4-pill-strong{all:unset;box-sizing:border-box;cursor:pointer;display:inline-flex;align-items:center;height:32px;padding:0 16px;margin-top:16px;border-radius:var(--mp4-r-ctl);background:var(--rx-gray-12);color:var(--rx-gray-1);font-size:14px;line-height:24px;font-weight:500}.mp4-pill-strong:hover{opacity:.88}
.mp4-new-row{all:unset;box-sizing:border-box;cursor:pointer;display:inline-flex;align-items:center;gap:8px;align-self:flex-start;padding:8px 0;font-size:14px;line-height:24px;font-weight:500;color:var(--mp4-fg)}.mp4-new-row:hover{color:var(--mp4-fg2)}
.mp4-page-head.mp4-page-head-row{flex-direction:row;align-items:center;justify-content:space-between;gap:12px}

/* Inbox actions as Codex / Multica do them: small ghost buttons that appear on the row you point at; a done item shows a quiet checked state instead, never something that looks clickable. */
.mp4-ghost-btn{all:unset;box-sizing:border-box;cursor:pointer;display:inline-flex;align-items:center;height:24px;padding:0 8px;border-radius:8px;font-size:14px;line-height:24px;font-weight:500;color:var(--mp4-fg2);white-space:nowrap}
.mp4-ghost-btn:hover{background:var(--mp4-hover);color:var(--mp4-fg)}.mp4-ghost-btn:focus-visible{outline:2px solid var(--rx-blue-9);outline-offset:1px}
.mp4-ghost-btn.mp4-ghost-main{color:var(--mp4-fg);box-shadow:inset 0 0 0 .5px var(--rx-gray-7)}
.mp4-krow .mp4-krow-acts{gap:4px;opacity:0;transition:opacity 150ms ease-out}.mp4-krow:hover .mp4-krow-acts,.mp4-krow:focus-within .mp4-krow-acts{opacity:1}@media (hover:none){.mp4-krow .mp4-krow-acts{opacity:1}}
.mp4-krow-done-mark{flex:none;display:inline-flex;align-items:center;gap:4px;font-size:14px;line-height:24px;color:var(--mp4-fg3)}
.mp4-kanban-head-acts{gap:4px}
.mp4-loading{display:flex;align-items:center;justify-content:center;gap:8px;padding:72px 16px;font-size:14px;line-height:24px;color:var(--mp4-fg3)}.mp4-spin{width:12px;height:12px;border-radius:50%;border:1.5px solid var(--rx-gray-6);border-top-color:var(--rx-gray-11);animation:mp4-spin 800ms linear infinite}@keyframes mp4-spin{to{transform:rotate(360deg)}}@media (prefers-reduced-motion:reduce){.mp4-spin{animation:none}}
.mp4-mark{display:block;image-rendering:pixelated}.mp4-mark-working rect:nth-child(n+46){animation:mp4-mark-blink 960ms steps(1,end) infinite}@keyframes mp4-mark-blink{50%{opacity:.25}}@media (prefers-reduced-motion:reduce){.mp4-mark-working rect{animation:none}}
.mp4-letter-av{display:inline-flex;align-items:center;justify-content:center;box-sizing:border-box;border-radius:8px;font-weight:600;line-height:1;color:color-mix(in srgb,var(--av-tint) 70%,var(--rx-gray-12));background-color:color-mix(in srgb,var(--av-tint) 16%,var(--rx-gray-1));background-image:repeating-conic-gradient(color-mix(in srgb,var(--av-tint) 10%,transparent) 0 25%,transparent 0 50%);background-size:4px 4px}
.mp4-file-filter{height:32px;border-radius:8px;background:var(--mp4-fill);border-color:transparent}.mp4-file-filter:focus{background:var(--rx-gray-1);border-color:var(--rx-gray-7);box-shadow:none}.mp4-file-filter::-webkit-search-cancel-button{filter:grayscale(1);opacity:.45;cursor:pointer}
.mp4-row.mp4-row-line{display:flex;align-items:center;gap:8px;min-height:32px;padding:8px 8px;border-radius:8px}.mp4-row-line .mp4-row-title{flex:1;min-width:0;font-size:14px;line-height:24px;font-weight:400;color:var(--mp4-fg)}.mp4-row-line .mp4-time{flex:none;font-size:14px;line-height:24px}.mp4-row-line.mp4-row-on{background:var(--rx-gray-3)}
.mp-retention-dialog .mp-handoff-form>label{margin:0;padding:12px 16px;border:.5px solid var(--rx-gray-6);background:var(--rx-gray-1)}.mp-retention-dialog .mp-handoff-form>label+label{margin-top:-16px;border-top-width:0}.mp-retention-dialog .mp-handoff-form>label:first-of-type{border-radius:12px 12px 0 0}.mp-retention-dialog .mp-handoff-form>label:last-of-type{border-radius:0 0 12px 12px}.mp-retention-dialog .mp-handoff-form>label:first-of-type:last-of-type{border-radius:12px}
.mp4-seg{flex:none;display:inline-flex;gap:4px;padding:4px;border-radius:8px;background:var(--rx-gray-3)}.mp4-seg>button{all:unset;cursor:pointer;padding:4px 12px;border-radius:8px;font-size:14px;line-height:24px;color:var(--mp4-fg3)}.mp4-seg>button.mp4-seg-on{background:var(--rx-gray-1,#fff);color:var(--mp4-fg);font-weight:500;box-shadow:0 0 0 .5px var(--rx-gray-6)}.mp4-seg>button:disabled{opacity:.45;cursor:default}.mp4-seg>button:focus-visible{outline:2px solid var(--rx-blue-9);outline-offset:1px}
.mp4-kb{display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:12px;align-items:start}
.mp4-kb-lane{min-width:0;display:flex;flex-direction:column;gap:8px;padding:8px;border-radius:12px;background:var(--rx-gray-2)}
.mp4-kb-head{margin:0;padding:4px 8px;display:flex;align-items:center;gap:8px;font-size:14px;line-height:24px;font-weight:600;color:var(--mp4-fg)}.mp4-kb-n{font-weight:400;color:var(--mp4-fg3);font-variant-numeric:tabular-nums}
.mp4-kb-tool{all:unset;box-sizing:border-box;cursor:pointer;margin-left:auto;display:inline-flex;align-items:center;justify-content:center;width:24px;height:24px;border-radius:8px;color:var(--mp4-fg3);transition:background 250ms ease-out,color 250ms ease-out}.mp4-kb-tool:hover{background:var(--rx-gray-4);color:var(--mp4-fg)}
.mp4-kb-cards{display:flex;flex-direction:column;gap:8px}
.mp4-kb-card{position:relative;box-sizing:border-box;cursor:pointer;display:flex;flex-direction:column;gap:4px;padding:12px 12px;border-radius:8px;background:var(--mp4-bg);box-shadow:0 0 0 .5px var(--rx-gray-6),0 1px 2px rgba(0,0,0,.04);outline:none;transition:box-shadow 250ms ease-out}
.mp4-kb-card:hover{box-shadow:0 0 0 .5px var(--rx-gray-8),0 2px 8px rgba(0,0,0,.06)}
.mp4-kb-card:focus-visible,.mp4-kb-x:focus-visible,.mp4-kb-tool:focus-visible,.mp4-kb-more:focus-visible{outline:2px solid var(--rx-blue-9);outline-offset:-2px}
.mp4-kb-top{display:flex;align-items:flex-start;gap:8px;min-width:0}
.mp4-kb-icon{flex:none;display:inline-flex;align-items:center;justify-content:center;width:16px;height:20px}.mp4-kb-icon>.mp4-app{border-radius:4px}
.mp4-kb-title{flex:1;min-width:0;font-size:14px;line-height:24px;font-weight:500;color:var(--mp4-fg);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.mp4-kb-when{flex:none;font-size:14px;line-height:24px;color:var(--mp4-fg3);font-variant-numeric:tabular-nums}
.mp4-kb-body{padding-left:24px;font-size:14px;line-height:24px;color:var(--mp4-fg2);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.mp4-kb-foot{margin-top:auto;padding-left:24px;min-width:0;display:flex;align-items:center;gap:8px;font-size:14px;line-height:24px;color:var(--mp4-fg3);overflow:hidden;white-space:nowrap}
.mp4-kb-where{display:inline-flex;align-items:center;gap:4px;min-width:0}
.mp4-kb-card{min-height:62px}
.mp4-kb-group{display:flex;flex-direction:column;gap:8px}.mp4-kb-group+.mp4-kb-group{margin-top:8px}
.mp4-kb-proj{all:unset;box-sizing:border-box;cursor:pointer;display:flex;align-items:center;gap:8px;min-width:0;padding:0 8px;height:20px;font-size:14px;line-height:24px;color:var(--mp4-fg2);border-radius:8px}.mp4-kb-proj:hover{color:var(--mp4-fg)}.mp4-kb-proj:focus-visible{outline:2px solid var(--rx-blue-9);outline-offset:-2px}
.mp4-kb-none{margin:0;padding:4px 8px 4px;font-size:14px;line-height:24px;color:var(--mp4-fg3)}
.mp4-kb-tools{margin-left:auto;display:inline-flex;align-items:center;gap:4px}
.mp4-kb-tools .mp4-kb-tool{margin-left:0}
.mp4-kb-tool-on{background:var(--rx-gray-4);color:var(--mp4-fg)}
.mp4-kb-glyph{display:inline-flex;color:var(--mp4-fg2)}
.mp4-ib-empty .mp4-kb-tool{margin-left:8px;vertical-align:middle}
.mp4-kb-quiet .mp4-kb-title{font-weight:400;color:var(--mp4-fg2)}
.mp4-kb-x{all:unset;box-sizing:border-box;cursor:pointer;position:absolute;top:9px;right:8px;display:inline-flex;align-items:center;justify-content:center;width:22px;height:22px;border-radius:8px;color:var(--mp4-fg3);background:var(--mp4-bg);opacity:0;transition:opacity 250ms ease-out,background 250ms ease-out}
.mp4-kb-card:hover .mp4-kb-x,.mp4-kb-x:focus-visible{opacity:1}.mp4-kb-x:hover{background:var(--rx-gray-4);color:var(--mp4-fg)}@media (hover:none){.mp4-kb-x{opacity:1}}
.mp4-kb-card:has(.mp4-kb-x):hover .mp4-kb-when{visibility:hidden}
@media (hover:none){.mp4-kb-card:has(.mp4-kb-x) .mp4-kb-top{padding-right:28px}}
.mp4-kb-more{all:unset;box-sizing:border-box;cursor:pointer;padding:4px 8px;font-size:14px;line-height:24px;color:var(--mp4-fg3)}.mp4-kb-more:hover{color:var(--mp4-fg)}
.mp4-ib-empty{margin:0;padding:48px 8px;font-size:14px;line-height:24px;color:var(--mp4-fg3);text-align:center}
/* DESIGN.md v0.2 applied beyond the three core tabs: section headings 16, white cards with hairlines, icon buttons without frames. */
.mp4-setgroup>h4,.mp4-setgroup>h4 .mp4-setgroup-n{font-size:16px;line-height:24px;font-weight:600}.mp4-setgroup>h4 .mp4-setgroup-n{font-weight:400}
.mp4-setlist{border-color:var(--mp4-t-border);border-radius:var(--mp4-r-card)}
.mp4-banner{background:none;border-color:var(--mp4-t-border);border-radius:var(--mp4-r-card);padding:12px 16px}
.mp4-banner .mp4-btn,.mp4-btn{height:32px;padding:0 16px;border-radius:var(--mp4-r-ctl)}
.mp4-page-head .mp4-tab-title,.mp4-tab-title{font-size:16px;line-height:24px;font-weight:600}
.mp4-dfile-dl{all:unset;box-sizing:border-box;cursor:pointer;flex:none;display:inline-flex;align-items:center;justify-content:center;width:32px;height:32px;margin-right:4px;border-radius:var(--mp4-r-ctl);color:var(--mp4-fg2)}.mp4-dfile-dl:hover{background:var(--mp4-t-hover);color:var(--mp4-fg)}.mp4-dfile-dl:focus-visible{outline:1.5px solid var(--mp4-fg)}
.mp4-dfile-main{padding:8px 12px;gap:8px}
.mp4-setlist>.mp4-file-item{padding:0}
.mp4-dfile-preview pre{font:14px/24px var(--dsw-font-family,-apple-system,"PingFang SC",sans-serif)}
.mp-workspace-meta,.mp-workspace-meta-wait{font-size:14px;color:var(--rx-gray-11)}
.mp4-sec>.mp4-k{margin:0;padding:0;font-size:16px;line-height:24px;font-weight:600;color:var(--mp4-fg)}
.mp4-dfile{border-color:var(--mp4-t-border);border-radius:var(--mp4-r-card)}.mp4-setlist>.mp4-dfile{padding:0}.mp4-dfile-main{padding:8px 16px}
.mp4-dfile:has(+.mp4-dfile){border-bottom:0;border-bottom-left-radius:0;border-bottom-right-radius:0}.mp4-dfile+.mp4-dfile{margin-top:0;border-top-color:var(--mp4-t-line);border-top-left-radius:0;border-top-right-radius:0}
.mp4-crumb .mp4-crumb-up{height:24px}
.mp4-sec>.mp4-dfile+.mp4-dfile{margin-top:-4px}.mp4-filepane .mp4-sec>.mp4-dfile+.mp4-dfile{margin-top:-8px}
`;


		function installStyles() {
			const style = document.createElement("style");
			style.setAttribute("data-mira-projects", "");
			style.textContent = CSS;
			document.head.appendChild(style);
			return () => style.remove();
		}

		// ------------------------------------------------------------------ plugin

		const inject = ["slots", "locale", "uiWorkspace", "layout", "sessions", "sidebarRight", "sidebarRightTabs", "inputTriggers", "uiConversation"];

		function apply(ctx) {
			ctx.effect(installStyles, "mira-projects: styles");
			ctx.effect(() => ctx.locale.register(NS, { zh, en }), "mira-projects: dictionaries");
			const t = ctx.locale.bind(NS);
			shell.workspace = ctx.uiWorkspace; shell.sidebar = ctx.sidebarRight; shell.sessions = ctx.sessions;
			ctx.effect(() => {
				window.addEventListener("keydown", onUpKey, true);
				return () => window.removeEventListener("keydown", onUpKey, true);
			}, "mira-projects: up key");
			// E2E #1: a first visit lands on DSH's own start page; Mira's home is the personal assistant, so open it, once per browser.
			ctx.effect(() => {
				let welcomed = true;
				try { welcomed = window.localStorage.getItem("mira-projects:welcomed") === "1"; } catch (_e) { /* storage unavailable: leave DSH's page */ }
				if (welcomed) return () => {};
				const timer = window.setTimeout(() => {
					try { window.localStorage.setItem("mira-projects:welcomed", "1"); } catch (_e) { /* private window */ }
					// DSH may already show an empty draft conversation here; only a link to somewhere else is left alone.
					if (!window.location.hash && /\/$/.test(window.location.pathname)) openMira();
				}, 1500);
				return () => window.clearTimeout(timer);
			}, "mira-projects: first visit");

			// Inside a task's DSH Session: the plugin's right-panel tabs (DSH's files, terminal and browser sit beside them).
			// Each tab type belongs to one level (Mira, a project's conversation, a task); DSH lists every registered type on its
			// 开始 page, so each also takes its own card there, which shows only in Sessions of that level.
			const tabType = (id, kind, title, guide, level) => {
				ctx.effect(() => ctx.sidebarRightTabs.register({ id, kind, title: () => title, guide: [{ ...guide, title: () => title }] }), `mira-projects: ${kind} tab type`);
				// DSH saves a tab's title when it is opened and shows that on its chip; a title registered here is shown instead, so
				// a tab opened before a rename reads as it does today. Only the chip's text: never in the way of the plugin loading.
				ctx.effect(() => { try { return ctx.slots.inject("sidebar.right.pane.tab.title", () => ctx.slots.register({ name: "sidebar.right.pane.tab.title", key: id }, () => title)); } catch (_e) { return () => {}; } }, `mira-projects: ${id} title`);
				ctx.effect(() => ctx.slots.inject("sidebar.right.tab.guide.entry", () => ctx.slots.register({ name: "sidebar.right.tab.guide.entry", key: id, locale: NS, inject: (sessionId) => ({ sessionId }) }, guideEntry(level))), `mira-projects: ${id} guide card`);
			};
			const tabBody = (id, Component) => ctx.effect(() => ctx.slots.inject("sidebar.right.pane.tab", () => ctx.slots.register({ name: "sidebar.right.pane.tab", key: id, locale: NS, inject: (sessionId) => ({ sessionId }) }, Component)), `mira-projects: ${id} body`);
			tabType(TASK_TAB_ID, TASK_TAB_KIND, "任务详情", { id: "mira-task", order: 5, description: () => "背景、产出和进展" }, "task");
			tabType(CHANGES_TAB_ID, CHANGES_TAB_KIND, "交付", { id: "mira-changes", order: 6, description: () => "改了什么、检查结果、MR" }, "task");
			tabType(AGENTS_TAB_ID, AGENTS_TAB_KIND, "并行执行", { id: "mira-agents", order: 7, description: () => "主会话派出的子智能体" }, "task");
			tabBody(TASK_TAB_ID, TaskTab); tabBody(CHANGES_TAB_ID, ChangesTab); tabBody(AGENTS_TAB_ID, AgentsTab);
			// A delivered file in its own tab: a resource type (its address names the file), not a guide card.
			ctx.effect(() => ctx.sidebarRightTabs.register({ id: DOC_TAB_ID, kind: DOC_TAB_KIND, patterns: [DOC_PREFIX + "**"], priority: "builtin", canOpen: a => !!docOf(a), title: a => docOf(a)?.name || "文件" }), "mira-projects: document tab type");
			tabBody(DOC_TAB_ID, DocTab);
			// Inside Mira's own Session: her overviews and settings as DSH right-panel tabs, suggested lines above her composer.
			for (const tab of MIRA_TABS) { tabType(tab.id, tab.kind, tab.title, { id: tab.id, order: tab.order, description: () => tab.desc }, tab.quiet ? "quiet" : "mira"); tabBody(tab.id, tab.body); }
			ctx.effect(() => ctx.slots.inject("conversation.input.dock", () => ctx.slots.register({ name: "conversation.input.dock", id: "mira-suggestions", order: 5, locale: NS, inject: (sessionId) => ({ sessionId }) }, MiraSuggestions)), "mira-projects: Mira suggestions");
			// Inside a task: what it waits on the owner to decide, above the composer.
			ctx.effect(() => ctx.slots.inject("conversation.input.dock", () => ctx.slots.register({ name: "conversation.input.dock", id: "mira-task-decisions", order: 4, locale: NS, inject: (sessionId) => ({ sessionId }) }, TaskDecisionDock)), "mira-projects: task decisions");
			ctx.effect(() => ctx.slots.inject("conversation.input.dock", () => ctx.slots.register({ name: "conversation.input.dock", id: "mira-task-elsewhere", order: 3.5, locale: NS, inject: (sessionId) => ({ sessionId }) }, TaskElsewhereDock)), "mira-projects: task elsewhere");
			ctx.effect(() => ctx.slots.inject("conversation.input.dock", () => ctx.slots.register({ name: "conversation.input.dock", id: "mira-shot", order: 2, locale: NS, inject: (sessionId) => ({ sessionId }) }, ShotDock)), "mira-projects: screenshot dock");
			ctx.effect(() => { const on = () => { void takeShot(); }; on(); window.addEventListener("hashchange", on); return () => window.removeEventListener("hashchange", on); }, "mira-projects: screenshot ask");
			ctx.effect(() => {
				const click = e => {
					const a = e.target && e.target.closest && e.target.closest("a[href]"), m = a && TASK_LINK.exec(a.getAttribute("href") || "");
					if (!m || e.button > 1) return;
					e.preventDefault(); e.stopPropagation();
					void openTaskLink(m[1]);
				};
				const typed = () => { const m = TASK_LINK.exec(window.location.hash || ""); if (!m) return; window.history.replaceState(window.history.state, "", window.location.pathname + window.location.search); void openTaskLink(m[1]); };
				document.addEventListener("click", click, true); document.addEventListener("auxclick", click, true); window.addEventListener("hashchange", typed); typed();
				return () => { document.removeEventListener("click", click, true); document.removeEventListener("auxclick", click, true); window.removeEventListener("hashchange", typed); };
			}, "mira-projects: task links");
			ctx.effect(() => ctx.slots.inject("conversation.input.dock", () => ctx.slots.register({ name: "conversation.input.dock", id: "mira-memory-notes", order: 3, locale: NS, inject: (sessionId) => ({ sessionId }) }, MiraMemoryNotes)), "mira-projects: memory notes");
			// Beside the title of a task and of its sub-agents: up to the project, and who works on the task.
			ctx.effect(() => ctx.slots.inject("conversation.session.header.actions", () => ctx.slots.register({ name: "conversation.session.header.actions", id: "mira-project", order: 1, locale: NS, inject: (sessionId) => ({ sessionId }) }, SessionProject)), "mira-projects: project beside the title");
			// Keep the last two Sessions shown even while no list reads them.
			ctx.effect(() => { const mounted = shell.sidebar?.mounted; notePreviousSession(); return mounted && typeof mounted.subscribe === "function" ? mounted.subscribe(notePreviousSession) : () => {}; }, "mira-projects: session trail");
			// DSH's own @ menu offers our projects and agents in Mira, project and task conversations.
			mentionTriggers = ctx.inputTriggers;
			ctx.effect(() => ctx.inputTriggers.registerSource(MENTION_SOURCE), "mira-projects: @ projects and agents");
			ctx.effect(() => { const timer = window.setTimeout(() => hideUntitledSessions(ctx.inputTriggers), 1500); return () => window.clearTimeout(timer); }, "mira-projects: @ menu without untitled conversations");
			// A sent mention reads as one chip in the owner's bubble. DSH keeps what a rule matched, so a changed name list is a new rule.
			ctx.effect(() => {
				let dispose = null, key = "", gone = false;
				onMentionData = data => {
					const names = mentionNames(data), next = JSON.stringify(names);
					if (gone || next === key) return;
					key = next; dispose?.();
					dispose = names.length ? ctx.uiConversation.userText.register(mentionRule(names)) : null;
				};
				mentionData().catch(() => {});
				return () => { gone = true; onMentionData = null; dispose?.(); };
			}, "mira-projects: sent mentions");
			ctx.effect(() => ctx.slots.inject("conversation.user.text.node", () => ctx.slots.register({ name: "conversation.user.text.node", key: MENTION_NODE, priority: 100, locale: NS }, MentionChip)), "mira-projects: mention chip");
			ctx.effect(() => ctx.slots.inject("conversation.chat.node", () => ctx.slots.register({ name: "conversation.chat.node", key: "turn-trigger", priority: -100, locale: NS }, MiraTrigger)), "mira-projects: wake notices");
			// Inside a project conversation: the project page's panels as DSH right-panel tabs.
			for (const tab of PROJECT_TABS) { tabType(tab.id, tab.kind, tab.title, { id: tab.id, order: tab.order, description: () => tab.desc }, tab.quiet ? "quiet" : "project"); tabBody(tab.id, PROJECT_TAB_BODIES[tab.id]); }
			// The plugin's own tools read as cards in the conversation instead of raw calls.
			const views = { request_decision: DecisionToolView, update_card: UpdateCardView, inbox_read: InboxReadView, post_message: PostMessageView, message_coordinator: MessageCoordinatorView, withdraw_decision: WithdrawDecisionView,
				open_thread: OpenThreadView, add_member: AddMemberView, message_thread: MessageThreadView, list_threads: ListThreadsView, read_thread: ReadThreadView, no_reply: NoReplyView };
			for (const [tool, View] of Object.entries(views)) ctx.effect(() => ctx.slots.inject("tool.call.toolview", () => ctx.slots.register({ name: "tool.call.toolview", key: tool, locale: NS, inject: (sessionId) => ({ sessionId }) }, View)), `mira-projects: ${tool} view`);
			// No floating pet (the owner, 2026-11-18 night: 「右下角的悬浮球这个功能就关掉」): the pixel figure is the assistant's
			// avatar only. MiraBall stays in the code, unregistered.
			// The assistant's avatar at the head of its own conversation.
			ctx.effect(() => ctx.slots.inject("conversation.session.header.actions", () => ctx.slots.register({ name: "conversation.session.header.actions", id: "mira-avatar", order: 0, locale: NS, inject: (sessionId) => ({ sessionId }) }, MiraHeaderAvatar)), "mira-projects: assistant avatar in its conversation");
			ctx.effect(() => ctx.slots.inject("shell.overlay", () => ctx.slots.register({ name: "shell.overlay", id: "mira-studio", order: 60 }, LookStudioHost)), "mira-projects: look studio");
			// The summon key (⌥M unless the owner picked another or none): a capture listener, as DSH's shortcut service refuses some combinations.
			ctx.effect(() => {
				const onKey = e => {
					if (studioBus.recording) return;
					const look = sharedFeed("api/mira-projects/personal", { loaded: false }).getSnapshot().look;
					const key = look === undefined || look === null ? DEFAULT_HOTKEY : look.hotkey;
					if (!hotkeyMatches(key, e)) return;
					e.preventDefault(); e.stopPropagation();
					summon();
				};
				document.addEventListener("keydown", onKey, true);
				return () => document.removeEventListener("keydown", onKey, true);
			}, "mira-projects: summon key");
			// The left column answers "where": projects and the latest few things. DSH's own list is one click away.
			ctx.effect(() => ctx.slots.inject("sidebar.workspaces", () => {
				let dispose = null;
				const notify = () => { for (const fn of shell.navListeners) fn(); };
				shell.showNav = (persist = true) => {
					if (!dispose) dispose = ctx.slots.register({ name: "sidebar.workspaces", priority: -10 }, WorkspaceProjects);
					if (persist) writePref("sidebar", "projects");
					if (!shell.showingNav) { shell.showingNav = true; notify(); }
				};
				shell.showNative = (persist = true) => {
					dispose?.(); dispose = null;
					if (persist) writePref("sidebar", "native");
					if (shell.showingNav) { shell.showingNav = false; notify(); }
				};
				if (readPref("sidebar") === "native") shell.showNative(false); else shell.showNav(false);
				const onStorage = e => { if (e.key === "mira-projects:sidebar") { if (e.newValue === "native") shell.showNative(false); else shell.showNav(false); } };
				window.addEventListener("storage", onStorage);
				return () => { window.removeEventListener("storage", onStorage); shell.showNative(false); shell.showNav = null; shell.showNative = null; shell.expandSidebar = null; };
			}), "mira-projects: left column");
			ctx.effect(() => ctx.slots.inject("sidebar.footer.action", () => ctx.slots.register({ name: "sidebar.footer.action", id: "mira-projects.sidebar-mode", order: 10 }, SidebarModeSwitch)), "mira-projects: sidebar mode switch");

			ctx.effect(() => ctx.slots.inject("main", () => {
				const dispose = ctx.slots.register({ name: "main", key: PANEL_ID, locale: NS }, ProjectsPage);
				const disposeHome = ctx.slots.register({ name: "main", key: HOME_ID, locale: NS }, PersonalHome);
				if (readRoute() || isIndexRoute()) ctx.layout.selectPanel(PANEL_ID);
				else if (isHomeRoute()) ctx.layout.selectPanel(HOME_ID);
				return () => { dispose(); disposeHome(); };
			}), "mira-projects: page");
			ctx.effect(() => {
				const restore = () => {
					if (readRoute() || isIndexRoute()) ctx.layout.selectPanel(PANEL_ID);
					else if (homeIsFallbackOnly()) openMira({ keep: true });
					else if (isHomeRoute()) ctx.layout.selectPanel(HOME_ID);
					else if (window.location.hash === "#mira-projects-new") ctx.layout.selectPanel(PANEL_ID);
					else if (Object.prototype.hasOwnProperty.call(window.history.state || {}, "miraProjectsPanel")) ctx.layout.selectPanel(window.history.state.miraProjectsPanel);
				};
				const create = () => { if (window.location.hash !== "#mira-projects-new") window.history.pushState({ ...window.history.state, miraProjectsPanel: PANEL_ID }, "", "#mira-projects-new");ctx.layout.selectPanel(PANEL_ID); };
				window.addEventListener("popstate", restore); window.addEventListener("hashchange", restore);window.addEventListener("mira-projects:navigate", restore);window.addEventListener("mira-personal:navigate", restore);window.addEventListener("mira-projects:create", create);
				const off = ctx.layout.panelInfo.subscribe(() => {
					const active = ctx.layout.panelInfo.getSnapshot().activePanelId;
					if ((active === HOME_ID || active === PANEL_ID) && readPref("sidebar") !== "native") shell.showNav?.(false);
					if (active === HOME_ID && (!isHomeRoute() || homeIsFallbackOnly())) { openMira();return; }
					// 「项目」 opens the index of every project.
					if (active === PANEL_ID && !readRoute() && !isIndexRoute() && window.location.hash !== "#mira-projects-new") { navigateIndex();return; }
					if ((active !== PANEL_ID && (readRoute() || isIndexRoute())) || (active !== HOME_ID && isHomeRoute()) || (active !== PANEL_ID && window.location.hash === "#mira-projects-new")) {
						window.history.pushState({ ...window.history.state, miraProjectsPanel: active }, "", window.location.pathname + window.location.search);
					}
				});
				// DSH's Mira entry opens her Session even when its panel is already the selected one (a click then changes nothing).
				const entryClick = e => { if (e.target?.closest?.("button")?.querySelector("[data-mira-entry]")) openMira(); };
				document.addEventListener("click", entryClick, true);
				if (ctx.layout.panelInfo.getSnapshot().activePanelId === HOME_ID && (!isHomeRoute() || homeIsFallbackOnly())) openMira();
				return () => { off(); document.removeEventListener("click", entryClick, true); window.removeEventListener("popstate", restore); window.removeEventListener("hashchange", restore);window.removeEventListener("mira-projects:navigate", restore);window.removeEventListener("mira-personal:navigate", restore);window.removeEventListener("mira-projects:create", create); };
			}, "mira-projects: deep links");
			ctx.effect(() => ctx.slots.inject("sidebar.panellist", () => ctx.slots.register({
				name: "sidebar.panellist",
				id: PANEL_ID,
				order: 20,
				locale: NS,
				label: () => t("panel"),
			}, ProjectsIcon)), "mira-projects: panel entry");
			ctx.effect(() => ctx.slots.inject("sidebar.panellist", () => ctx.slots.register({ name: "sidebar.panellist", id: HOME_ID, order: 15, locale: NS, label: () => "个人助理" }, MiraEntryIcon)), "mira-projects: personal entry");
		}

		exports.apply = apply;
		exports.inject = inject;
		exports.executionRows = executionRows;
		exports.livePosts = livePosts;
		exports.skillDiff = skillDiff; exports.kanbanLine = kanbanLine; exports.STATUS_LOOK = STATUS_LOOK; exports.bannerOf = bannerOf; exports.timelineRows = timelineRows; exports.deliveredFiles = deliveredFiles; exports.sizeLabel = sizeLabel;
		exports.mentionCandidates = mentionCandidates; exports.deliveryOrder = deliveryOrder; exports.titledOnly = titledOnly; exports.mentionClipboard = mentionClipboard; exports.mentionText = mentionText; exports.mentionNames = mentionNames; exports.mentionRule = mentionRule; exports.iconSpec = iconSpec; exports.modelName = modelName; exports.executorOf = executorOf; exports.shownName = shownName; exports.isProjectExecutor = isProjectExecutor; exports.taskStatus = taskStatus; exports.projectLook = projectLook; exports.layoutTimeline = layoutTimeline; exports.readWake = readWake; exports.wakeMessages = wakeMessages; exports.typedInSession = typedInSession; exports.workspaceScene = workspaceScene; exports.inboxItems = inboxItems; exports.miraScene = miraScene; exports.sceneShift = sceneShift; exports.isTemplate = isTemplate; exports.lookCells = lookCells; exports.lookPortrait = lookPortrait; exports.bestTemplate = bestTemplate; exports.hotkeyFrom = hotkeyFrom; exports.hotkeyMatches = hotkeyMatches; exports.hotkeyLabel = hotkeyLabel; exports.LOOK_TEMPLATES = LOOK_TEMPLATES; exports.LINE_BODIES = LINE_BODIES; exports.LINE_ACCESSORIES = LINE_ACCESSORIES; exports.ICONS = ICONS; exports.PROJECT_TINTS = PROJECT_TINTS; exports.PROJECT_ICON_NAMES = PROJECT_ICON_NAMES; exports.PROJECT_ICON_SHAPES = PROJECT_ICON_SHAPES;
		exports.createRevisionFeed = createRevisionFeed;
		exports.pickAsk = pickAsk; exports.inboxProjects = inboxProjects; exports.inboxNotes = inboxNotes; exports.noteAsks = noteAsks; exports.suggestionsFor = suggestionsFor; exports.panelFits = panelFits;
		return module.exports;
	}
});
