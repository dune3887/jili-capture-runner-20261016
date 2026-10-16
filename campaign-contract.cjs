// 采集目标按“以上”严格多一条；不足 20 万的旧测试记录不抵扣本次目标。
// 调度改为单节点分批：队列游标是真实 gameId，批次由清单实时计算，不沿用旧 20 节点×5 泳道编号。
const assert = require('node:assert/strict'), crypto = require('node:crypto');
const targets = Object.freeze({ normal: 300001, feature: 20001 });
// 默认每批 10 款、批内并发 10；只有资源测试证据通过后才允许 20。
const batchLimits = Object.freeze({ defaultGames: 10, defaultConcurrency: 10, hardMaxGames: 20, hardMaxConcurrency: 20 });
// 表格与人工规则明确停用、不进入 Slots 队列的游戏（捕鱼等）。
const disabledGameIds = Object.freeze([262, 389, 420, 468, 546, 512, 551, 690]);
function featureModes(profile) {
  const result = [];
  // Show 是客户端展示字段，不能据此认定购买/加注不存在；以价格和请求验收为准。
  const prices = profile.gameMall?.PriceOdd?.length ? profile.gameMall.PriceOdd : profile.mall?.PriceOdd > 1 ? [profile.mall.PriceOdd] : profile.rulesBuy || [];
  prices.forEach((price, index) => { if (Number.isFinite(price) && price > 1) result.push('buy:' + index); });
  const extra = profile.extra || {};
  const multipliers = extra.Mul?.length ? extra.Mul.slice(1) : profile.rulesExtra || [];
  multipliers.forEach((m, index) => { if (Number.isFinite(m) && m > 1) result.push(String(index + 1)); });
  return result;
}
function nextMode(counts, profile, phase = 'normal') {
  if ((counts['0'] || 0) < targets.normal) return '0';
  if (phase === 'normal') return null;
  assert.ok(profile, '购买和加注必须有官方模式配置');
  return featureModes(profile).find(mode => (counts[mode] || 0) < targets.feature) ?? null;
}
function repairLocalProtocolBlock(game, entry) {
  // 四款旧原件均只有缺失客户端文件的本地错误；仅恢复一次，不清除后续官方拒绝。
  if (entry.clientFlowReview || entry.status !== 'blocked' || entry.reason !== '协议或结算分支需要复核') return false;
  if ([820, 401, 639, 304].includes(game.id) && !entry.clientProofRepair) entry.clientProofRepair = true;
  // 仅恢复已复验的本地奖金/协议故障；14/67入口不可用、51地区限制仍暂停。
  else if ([21, 30, 44, 48, 769].includes(game.id) && !entry.adaptationRepair) entry.adaptationRepair = '20261013';
  else return false;
  entry.status = 'queued'; delete entry.retryAt; delete entry.reason; return true;
}
function retryWakeAt(entries, now, deadline, cooldown = 0) {
  const times = entries.filter(e => e.status !== 'blocked').map(e => Math.max(e.retryAt || 0, cooldown)).filter(t => t > now && t < deadline);
  return times.length ? Math.min(...times) : null;
}
// 人类要求逐款重新探测全部被拒游戏；每提升一次 revision 允许再复核一次，历史暂停审计全部保留（可回滚）。
const clientFlowRevision = '20261016-full-probe-v1';
function reviewStack(entry) { return entry?.clientFlowReview ? [...(entry.clientFlowReview.history || []), { revision: entry.clientFlowReview.revision, previous: entry.clientFlowReview.previous }] : []; }
function reviewedAtCurrentRevision(entry) { return entry?.clientFlowReview?.revision === clientFlowRevision; }
function restoreClientFlowEntry(seed = {}, cached, published) {
  const entry = cached || { counts: {}, ...seed };
  if (!reviewedAtCurrentRevision(entry) && published?.revision === clientFlowRevision) {
    entry.clientFlowReview = { revision: clientFlowRevision, previous: { status: seed.status, reason: seed.reason }, history: reviewStack(entry) };
    entry.status = published.status; entry.reason = published.reason; delete entry.retryAt;
  }
  if (reviewedAtCurrentRevision(entry) && published?.revision === clientFlowRevision && published.status === 'blocked') { entry.status = 'blocked'; entry.reason = published.reason; delete entry.retryAt; }
  return entry;
}
function revalidateClientFlow(entry, enabled) {
  if (!enabled || reviewedAtCurrentRevision(entry)) return false;
  const previous = { status: entry.status, reason: entry.reason, retryAt: entry.retryAt };
  entry.clientFlowReview = { revision: clientFlowRevision, previous, history: reviewStack(entry) };
  if (entry.status !== 'blocked') return false;
  entry.status = 'queued'; delete entry.reason; delete entry.retryAt; return true;
}
// 每款游戏恰好一个执行位置：按真实 gameId 排序后连续切批，不按旧泳道取模，避免遗漏或重复。
function planBatches(games, batchSize = batchLimits.defaultGames) {
  assert.ok(Number.isInteger(batchSize) && batchSize >= 1 && batchSize <= batchLimits.hardMaxGames, '批大小必须为 1 至 ' + batchLimits.hardMaxGames);
  const batches = [];
  for (let index = 0; index < games.length; index += batchSize) batches.push({ index: batches.length, games: games.slice(index, index + batchSize) });
  return batches;
}
// 游标是真实 gameId：从该游戏开始，按环形顺序遍历整批队列一次，不遗漏任何一款。
function sweepFrom(batches, cursorModule) {
  if (!batches.length) return { batches: [], start: 0 };
  let start = 0;
  if (cursorModule) {
    const found = batches.findIndex(batch => batch.games.some(game => game.module === cursorModule));
    start = found < 0 ? 0 : found;
  }
  return { batches: batches.slice(start).concat(batches.slice(0, start)), start, batchesBeforeWrap: batches.length - start };
}
function queueGame(game, entry, phase) {
  const counts = entry?.counts || {};
  if (entry?.status === 'blocked') return { queued: false, kind: 'blocked', reason: entry.reason || '官方业务拒绝或待复核' };
  // 普通未达标一律入队；普通达标后按阶段决定是否入队，缺少官方模式时入队取回一次模式配置。
  if ((counts['0'] || 0) < targets.normal) return { queued: true, status: entry?.status || 'queued' };
  if (phase === 'normal') return { queued: false, kind: 'normal-target-met', reason: '普通已达标，等待切换购买/加注阶段' };
  if (!entry?.profile) return { queued: true, status: entry?.status || 'queued', profilePending: true };
  const pending = featureModes(entry.profile).filter(mode => (counts[mode] || 0) < targets.feature);
  if (!pending.length) return { queued: false, kind: 'completed', reason: '全部模式已达标' };
  return { queued: true, status: entry?.status || 'queued' };
}
// 遍历完整清单：可采且未达标的进入队列；blocked、未适配、明确停用的记录跳过原因。
function queuePlan(manifest, state, { phase = 'normal', batchSize = batchLimits.defaultGames, cursor = null } = {}) {
  const blockedIds = new Set((manifest.blocked || []).map(item => item.id));
  const disabled = new Set(disabledGameIds);
  const games = manifest.games.slice().sort((a, b) => a.id - b.id);
  const queue = [], skipped = [];
  for (const game of games) {
    const reason = blockedIds.has(game.id) ? (manifest.blocked.find(item => item.id === game.id)?.reason || '缺协议或 RTP') : disabled.has(game.id) ? '明确停用，不进入 Slots 队列' : null;
    if (reason) { skipped.push({ id: game.id, module: game.module, kind: 'not-adapted', reason }); continue; }
    const decision = queueGame(game, state.games?.[game.module], phase);
    if (decision.queued) queue.push(game); else skipped.push({ id: game.id, module: game.module, kind: decision.kind, reason: decision.reason });
  }
  for (const id of disabledGameIds) if (!games.some(game => game.id === id)) skipped.push({ id, module: null, kind: 'disabled', reason: '明确停用，不进入 Slots 队列' });
  for (const item of manifest.blocked || []) if (!games.some(game => game.id === item.id)) skipped.push({ id: item.id, module: null, kind: 'not-adapted', reason: item.reason });
  const plan = sweepFrom(planBatches(queue, batchSize), cursor?.module);
  return { queue, skipped, batches: plan.batches, startBatch: plan.start, queueLength: queue.length };
}
// 批内切片：把剩余运行时间平均分给本次环形遍历里的批次，单批不会被别批饿死。
function batchSliceMs(remainingMs, batchesLeft, { minMs = 60000 } = {}) {
  assert.ok(Number.isInteger(batchesLeft) && batchesLeft >= 0, '剩余批次数必须为非负整数');
  if (batchesLeft <= 1) return Math.max(0, remainingMs);
  return Math.max(minMs, Math.floor(remainingMs / batchesLeft));
}
// 20 款并发：默认需要真实 20 路并发采集资源测试；人类在本次指令中直接要求启用时，
// 允许以“离线并发测量通过 + 明确记录的指令来源”放行，缺任何一项都回落到 10。
function resourceTestAllows(resourceTest, games) {
  if (!resourceTest || resourceTest.passed !== true) return false;
  if (!Number.isInteger(resourceTest.games) || resourceTest.games < games) return false;
  if (!Number.isFinite(resourceTest.peakRssMiB) || !Number.isFinite(resourceTest.samples) || resourceTest.samples < games) return false;
  if (resourceTest.mode === 'live-capture') return true;
  if (resourceTest.mode === 'offline-verify' && resourceTest.operatorOverride && resourceTest.operatorOverride.approvedBy === 'human') return true;
  return false;
}
function resolveBatchConfig(env = {}, resourceTest = null) {
  const requestedGames = Number(env.JL_BATCH_GAMES || batchLimits.defaultGames), requestedConcurrency = Number(env.JL_BATCH_CONCURRENCY || 0) || requestedGames;
  assert.ok(Number.isInteger(requestedGames) && requestedGames >= 1, '批大小必须为正整数');
  assert.ok(Number.isInteger(requestedConcurrency) && requestedConcurrency >= 1, '批内并发必须为正整数');
  const capped = [];
  let games = Math.min(requestedGames, batchLimits.hardMaxGames);
  if (requestedGames > batchLimits.hardMaxGames) capped.push(`games:${requestedGames}->${games}`);
  if (games > batchLimits.defaultGames && !resourceTestAllows(resourceTest, games)) { capped.push(`games:${games}->${batchLimits.defaultGames}`); games = batchLimits.defaultGames; }
  let concurrency = Math.min(requestedConcurrency, batchLimits.hardMaxConcurrency);
  if (requestedConcurrency > batchLimits.hardMaxConcurrency) capped.push(`concurrency:${requestedConcurrency}->${concurrency}`);
  if (concurrency > batchLimits.defaultConcurrency && !resourceTestAllows(resourceTest, concurrency)) { capped.push(`concurrency:${concurrency}->${batchLimits.defaultConcurrency}`); concurrency = batchLimits.defaultConcurrency; }
  if (concurrency > games) { capped.push(`concurrency:${concurrency}->${games}`); concurrency = games; }
  return { games, concurrency, requestedGames, requestedConcurrency, cappedBy: capped, resourceTest: resourceTest ? { games: resourceTest.games, passed: resourceTest.passed === true, runId: resourceTest.runId ?? null } : null };
}
function seal(bytes, key) {
  assert.equal(key.length, 32);
  const iv = crypto.randomBytes(12), cipher = crypto.createCipheriv('aes-256-gcm', key, iv);
  const ciphertext = Buffer.concat([cipher.update(bytes), cipher.final()]);
  return { v: 1, iv: iv.toString('base64'), tag: cipher.getAuthTag().toString('base64'), data: ciphertext.toString('base64') };
}
function open(envelope, key) {
  assert.equal(envelope.v, 1); assert.equal(key.length, 32);
  const decipher = crypto.createDecipheriv('aes-256-gcm', key, Buffer.from(envelope.iv, 'base64'));
  decipher.setAuthTag(Buffer.from(envelope.tag, 'base64'));
  return Buffer.concat([decipher.update(Buffer.from(envelope.data, 'base64')), decipher.final()]);
}
module.exports = { clientFlowRevision, restoreClientFlowEntry, revalidateClientFlow, targets, batchLimits, disabledGameIds, planBatches, sweepFrom, queuePlan, batchSliceMs, resolveBatchConfig, resourceTestAllows, featureModes, nextMode, repairLocalProtocolBlock, retryWakeAt, seal, open };
