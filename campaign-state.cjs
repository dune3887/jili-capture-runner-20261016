// 队列状态按真实 gameId/module 保存，不保存旧泳道编号；状态只按可验证来源轮次推进：
// 迟到的旧轮 running 不能清除新轮的暂停，旧轮暂停也不能覆盖更新的合法恢复状态。
const assert = require('node:assert/strict');
const stateVersion = 2;
function emptyState(campaignId) {
  assert.ok(typeof campaignId === 'string' && campaignId, '活动身份必须存在');
  return { stateVersion, campaignId, cursor: null, cursorUpdatedAt: null, games: {} };
}
// 来源轮次 = run 创建时间（可由 API 核对）+ run ID + 批序号 + 产物序号。
function sourceOrder(source = {}) {
  return [String(source.roundAt || ''), Number(source.runId || 0), Number(source.batchIndex ?? -1), Number(source.artifact || 0)];
}
function compareSource(left, right) {
  const a = sourceOrder(left || {}), b = sourceOrder(right || {});
  for (let index = 0; index < a.length; index++) if (a[index] !== b[index]) return a[index] < b[index] ? -1 : 1;
  return 0;
}
// 计数只增不减：迟到旧轮或往返快照都不能让已确认的来源进度回退。
function mergeCounts(previous = {}, incoming = {}) {
  const counts = { ...previous };
  for (const [mode, value] of Object.entries(incoming)) {
    assert.ok(Number.isInteger(value) && value >= 0, '采集计数必须为非负整数');
    counts[mode] = Math.max(counts[mode] || 0, value);
  }
  return counts;
}
const auditFields = ['profile', 'clientFlowReview', 'sessionReloginRepair', 'clientProofRepair', 'adaptationRepair'];
function mergeEntry(previous, incoming) {
  if (!previous) return { ...incoming, counts: { ...(incoming.counts || {}) } };
  const order = compareSource(incoming.source, previous.source);
  if (order < 0) return previous;
  const merged = { ...previous, ...incoming };
  merged.counts = mergeCounts(previous.counts, incoming.counts);
  // 审计字段（复核记录、重登/适配标记、模式档位）不能因后来缺字段的快照丢失。
  for (const field of auditFields) if (merged[field] === undefined && previous[field] !== undefined) merged[field] = previous[field];
  // 更新一轮快照：运行字段以新快照为准，新快照已经清掉的退避/原因不得被旧值复活。
  if (order > 0) for (const field of ['reason', 'retryAt']) if (incoming[field] === undefined) delete merged[field];
  if (order === 0 && previous.status === 'blocked' && incoming.status !== 'blocked') {
    merged.status = previous.status; merged.reason = previous.reason;
    if (previous.retryAt !== undefined) merged.retryAt = previous.retryAt;
  }
  return merged;
}
function markEntry(entry, source, updatedAt = new Date().toISOString()) {
  entry.source = { roundAt: source.roundAt, runId: Number(source.runId || 0), batchIndex: Number(source.batchIndex ?? -1) };
  entry.updatedAt = updatedAt;
  return entry;
}
function mergeGame(state, module, incoming, source) {
  assert.ok(state.games, '队列状态必须包含 games');
  const entry = mergeEntry(state.games[module], { ...incoming, source: incoming.source || source });
  state.games[module] = entry;
  return entry;
}
// 旧 20 节点×5 泳道状态按真实 gameId/module 合并；泳道编号只作为来源审计记录，不进入新队列。
function mergeLegacyFiles(state, { runId, roundAt, artifact, files = {} } = {}) {
  let merged = 0;
  const lanes = [];
  for (const [name, content] of Object.entries(files)) {
    const match = /^lane-(\d+)\.json$/.exec(name);
    if (!match) continue;
    const lane = JSON.parse(Buffer.isBuffer(content) ? content.toString('utf8') : JSON.stringify(content));
    assert.equal(lane.campaignId, state.campaignId, '旧泳道状态活动身份不符');
    lanes.push({ name, lane: Number(match[1]), modules: Object.keys(lane.games || {}).length });
    for (const [module, data] of Object.entries(lane.games || {})) {
      const source = { roundAt, runId, batchIndex: -1, artifact };
      const entry = mergeGame(state, module, data, source);
      if (!entry.legacyLanes) entry.legacyLanes = [];
      const label = `lane-${match[1]}@${runId}`;
      if (!entry.legacyLanes.includes(label)) entry.legacyLanes.push(label);
      merged++;
    }
  }
  lanes.sort((a, b) => a.lane - b.lane);
  return { merged, lanes };
}
// 新队列状态（queue-state.json）：游标是真实 gameId，批次由清单实时计算。
// 没有 stateVersion 的旧格式按真实 gameId/module 迁移，来源轮次视为最旧，不覆盖更新的状态。
function queueStateOf(name, content) {
  const raw = JSON.parse(Buffer.isBuffer(content) ? content.toString('utf8') : JSON.stringify(content));
  assert.ok(raw && typeof raw === 'object' && raw.campaignId, '队列状态缺少活动身份');
  assert.ok(!('stateVersion' in raw) || raw.stateVersion === stateVersion, '队列状态版本不符');
  if (raw.stateVersion !== stateVersion) {
    const state = emptyState(raw.campaignId);
    for (const [module, entry] of Object.entries(raw.games || {})) state.games[module] = { ...entry, source: entry.source || { roundAt: '', runId: 0, batchIndex: -1 } };
    state.migratedFrom = 'legacy-module-state';
    return state;
  }
  assert.ok(raw.games && typeof raw.games === 'object', '队列状态缺少 games');
  return raw;
}
function mergeQueueStates(target, incoming, source = {}) {
  assert.equal(incoming.campaignId, target.campaignId, '队列状态活动身份不符');
  let merged = 0;
  for (const [module, data] of Object.entries(incoming.games || {})) { mergeGame(target, module, data, source); merged++; }
  // 游标同样只按可验证来源轮次推进，不能由迟到快照回退。
  const incomingCursor = incoming.cursorSource || { roundAt: incoming.cursorUpdatedAt || '', runId: Number(source.runId || 0), batchIndex: Number(source.batchIndex ?? -1) };
  const targetCursor = target.cursorSource || { roundAt: target.cursorUpdatedAt || '', runId: 0, batchIndex: -1 };
  if (incoming.cursor && compareSource(incomingCursor, targetCursor) > 0) {
    target.cursor = incoming.cursor;
    target.cursorUpdatedAt = incoming.cursorUpdatedAt || target.cursorUpdatedAt;
    target.cursorSource = incomingCursor;
  }
  return merged;
}
function setCursor(state, module, source, updatedAt = new Date().toISOString()) {
  state.cursor = { module };
  state.cursorUpdatedAt = updatedAt;
  state.cursorSource = { roundAt: source?.roundAt, runId: Number(source?.runId || 0), batchIndex: Number(source?.batchIndex ?? -1) };
  return state.cursor;
}
module.exports = { stateVersion, emptyState, sourceOrder, compareSource, mergeCounts, mergeEntry, markEntry, mergeGame, mergeLegacyFiles, queueStateOf, mergeQueueStates, setCursor };
