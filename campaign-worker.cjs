// 单节点分批采集：本进程只处理一个批次，批内有界并发；每款沿用既有 30 局小批次轮转，
// 任何一款都不能独占节点；未知结算分支停用采集，不能滤掉中奖局凑数量。
const fs = require('node:fs'), path = require('node:path'), zlib = require('node:zlib'), assert = require('node:assert/strict');
const { spawn, spawnSync } = require('node:child_process');
const { nextMode, targets, repairLocalProtocolBlock, clientFlowRevision, restoreClientFlowEntry, revalidateClientFlow, retryWakeAt } = require('./campaign-contract.cjs');
const queueState = require('./campaign-state.cjs');
const root = __dirname, manifest = require('./campaign-manifest.json');
const batchIndex = Number(process.argv[2]), batchIds = String(process.argv[3] || '').split(',').filter(Boolean).map(Number);
assert.ok(Number.isInteger(batchIndex) && batchIndex >= 0, '批序号必须为非负整数');
assert.ok(batchIds.length >= 1 && batchIds.every(Number.isInteger), '本批必须包含至少一款真实游戏');
const phase = process.env.JL_PHASE || 'normal';
// 每款小批次沿用既有 30 局上限；批内并发与批截止时间由调度层给出，保证单款无法无限占用节点。
const roundBatch = Number(process.env.JL_ROUND_BATCH || 30);
assert.ok(Number.isInteger(roundBatch) && roundBatch >= 1 && roundBatch <= 30, '每款小批次必须为 1 至 30 局');
const concurrency = Number(process.env.JL_BATCH_CONCURRENCY || 10);
assert.ok(Number.isInteger(concurrency) && concurrency >= 1 && concurrency <= 20, '批内并发必须为 1 至 20');
const sliceMinutes = Number(process.env.JL_BATCH_MINUTES || 45);
assert.ok(Number.isFinite(sliceMinutes) && sliceMinutes > 0 && sliceMinutes <= 50, '本批时长必须大于 0 且不超过 50 分钟');
const deadline = Date.now() + sliceMinutes * 60000;
// 来源轮次用于接收端按可验证轮次合并状态，而不是按下载顺序覆盖。
const source = { roundAt: process.env.JL_SOURCE_ROUND_AT || new Date().toISOString(), runId: Number(process.env.JL_SOURCE_RUN || 0), batchIndex };
const stateFile = path.join(root, 'state/queue-state.json');
const privateRoot = path.join(root, `private/batch-${batchIndex}`);
const samplesRoot = path.join(root, `samples/batch-${batchIndex}`);
let slotSerial = 0, saveSerial = 0;
fs.mkdirSync(privateRoot, { recursive: true, mode: 0o700 });
fs.mkdirSync(samplesRoot, { recursive: true });
fs.mkdirSync(path.dirname(stateFile), { recursive: true });
// 队列状态缺失时按空状态继续，暂停仍然来自 campaign-pauses.json 与已入库回执。
const state = fs.existsSync(stateFile) ? queueState.queueStateOf('queue-state.json', fs.readFileSync(stateFile)) : queueState.emptyState(manifest.campaignId);
assert.equal(state.campaignId, manifest.campaignId, '活动身份变动，必须核对旧产物后重新建队列');
const receipts = JSON.parse(fs.readFileSync(path.join(root, 'receipts.json')));
assert.equal(receipts.campaignId, manifest.campaignId, '入库回执活动不符');
const pauseFile = path.join(root, 'campaign-pauses.json'), pauseSeed = fs.existsSync(pauseFile) ? JSON.parse(fs.readFileSync(pauseFile)) : {};
const games = batchIds.map(id => { const game = manifest.games.find(item => item.id === id); assert.ok(game, '本批包含清单外游戏：' + id); return game; });
function save() { const temp = stateFile + '.new-' + process.pid + '-' + (saveSerial++); fs.writeFileSync(temp, JSON.stringify(state)); fs.renameSync(temp, stateFile); }
function batchBytes() {
  const directory = samplesRoot, files = {};
  if (fs.existsSync(directory)) for (const name of fs.readdirSync(directory, { recursive: true })) { const file = path.join(directory, String(name)); if (fs.statSync(file).isFile()) files[name] = fs.readFileSync(file).toString('base64'); }
  // 与最终产物采用相同的无损压缩级别，保留全部原件及原有体积保护阈值。
  return zlib.gzipSync(Buffer.from(JSON.stringify({ samples: files })), { level: 9 }).length;
}
function capture(game, count, mode, slotRoot) {
  return new Promise(resolve => {
    const child = spawn(process.execPath, [path.join(root, 'capture-jl.cjs'), game.code, String(count), mode], {
      cwd: root,
      env: {
        PATH: process.env.PATH, LANG: 'C.UTF-8', JL_REQUEST_INTERVAL_MS: process.env.JL_REQUEST_INTERVAL_MS || '500',
        JL_SPIN_INTERVAL_MS: process.env.JL_SPIN_INTERVAL_MS || '5000', JL_MAX_RECREATE: '0',
        JL_MAX_SESSION_RELOGIN: [304, 639].includes(game.id) ? '3' : '0', JL_CAMPAIGN_ID: manifest.campaignId,
        JL_CAPTURE_PRIVATE_DIR: slotRoot,
      }, stdio: ['ignore', 'pipe', 'pipe'],
    });
    // 会话 URL 仅写在加密的原件中；公开日志只输出游戏 ID、模式、数量和分类状态。
    child.stdout.resume(); let diagnostic = '';
    child.stderr.on('data', bytes => { if (diagnostic.length < 65536) diagnostic += bytes.toString().slice(0, 65536 - diagnostic.length); });
    child.on('error', error => resolve({ status: 1, error: error.message, stderr: diagnostic }));
    child.on('close', (status, signal) => resolve({ status: status ?? 1, signal, stderr: diagnostic }));
  });
}
function prepareEntry(game) {
  const entry = restoreClientFlowEntry(pauseSeed[game.module], state.games[game.module], receipts.clientFlowStates?.[game.module]);
  // 入库回执是已确认事实，旧缓存里的来源计数只能取较大值，不能回退或虚增。
  entry.imported = { ...(receipts.confirmed[game.module] || {}) };
  entry.counts = queueState.mergeCounts(entry.counts, entry.imported);
  state.games[game.module] = entry;
  revalidateClientFlow(entry, process.env.JL_CLIENT_FLOW_REVALIDATION === clientFlowRevision);
  repairLocalProtocolBlock(game, entry);
  // 人类授权恢复已核实的会话失效；每款只解锁一次，后续失败仍暂停。
  if (!entry.clientFlowReview && [304, 639].includes(game.id) && entry.status === 'blocked' && entry.reason === '官方业务拒绝 ret=304：官方会话不存在' && !entry.sessionReloginRepair) {
    entry.sessionReloginRepair = '20261014-v1'; entry.status = 'queued'; delete entry.reason; delete entry.retryAt;
  }
  return entry;
}
async function runSlot(game, entry) {
  const mode = nextMode(entry.counts, entry.profile, phase);
  if (mode == null) { entry.status = 'captured-pending-import'; return { game, mode: null, accepted: 0, status: 'done' }; }
  const target = mode === '0' ? targets.normal : targets.feature;
  const count = Math.min(roundBatch, target - (entry.counts[mode] || 0));
  if (count <= 0) return { game, mode, accepted: 0, status: 'done' };
  // 每次采集都有独立私有目录：批内并发时不会误读其他游戏的原件。
  const slotRoot = path.join(privateRoot, `${game.code}-s${slotSerial++}`);
  fs.mkdirSync(slotRoot, { recursive: true, mode: 0o700 });
  const childResult = await capture(game, count, mode, slotRoot); let status = childResult.status;
  let dirs = fs.existsSync(slotRoot) ? fs.readdirSync(slotRoot) : [];
  if (!dirs.length && status !== 0) {
    const name = game.code + '-native-' + Date.now() + '-diagnostic';
    fs.mkdirSync(path.join(slotRoot, name), { mode: 0o700 }); dirs = [name];
  }
  let accepted = 0, httpStatus, sourceFailure;
  for (const name of dirs) {
    const dir = path.join(slotRoot, name), profile = path.join(dir, 'mode-profile.json');
    if (status !== 0) fs.writeFileSync(path.join(dir, 'child-failure.json'), JSON.stringify(childResult));
    if (fs.existsSync(profile)) entry.profile = JSON.parse(fs.readFileSync(profile));
    const file = path.join(dir, 'accepted.json');
    if (fs.existsSync(file) && JSON.parse(fs.readFileSync(file)).rounds > 0) {
      const data = JSON.parse(fs.readFileSync(file));
      if (data.gameId !== game.id || data.gameCode !== game.code || data.campaignId !== manifest.campaignId) throw Error('采集身份不符');
      const verified = spawnSync(process.execPath, [path.join(root, 'jl-data.cjs'), 'verify', dir], {
        cwd: root, env: { PATH: process.env.PATH, JL_CAMPAIGN_ID: manifest.campaignId, JL_RTP_TAGS_FILE: path.join(root, 'campaign-rtp.json') }, timeout: 30000, maxBuffer: 2e6,
      });
      if (verified.status === 0) accepted += JSON.parse(verified.stdout).rounds;
      else { status = 1; fs.writeFileSync(path.join(dir, 'verify-failure.txt'), verified.stderr || '复验失败'); fs.renameSync(file, path.join(dir, 'rejected-accepted.json')); }
    }
    const failure = path.join(dir, 'http-failure.json');
    if (fs.existsSync(failure)) { const f = JSON.parse(fs.readFileSync(failure)); httpStatus = f.status; entry.retryAt = Date.now() + Math.max(f.retryAfterSeconds || 0, 900) * 1000; }
    const sourceFile = path.join(dir, 'source-failure.json');
    if (fs.existsSync(sourceFile)) sourceFailure = JSON.parse(fs.readFileSync(sourceFile));
    fs.renameSync(dir, path.join(samplesRoot, name));
  }
  fs.rmSync(slotRoot, { recursive: true, force: true });
  entry.counts[mode] = (entry.counts[mode] || 0) + accepted;
  if (status !== 0 || !accepted) {
    entry.status = httpStatus === 0 || httpStatus === 429 || httpStatus >= 500 || !dirs.length ? 'paused' : 'blocked';
    entry.reason = sourceFailure ? (sourceFailure.ret == null ? sourceFailure.reason : `官方业务拒绝 ret=${sourceFailure.ret}：${sourceFailure.reason}`) : httpStatus === 0 ? '来源网络失败（未重放 Spin）' : httpStatus ? `HTTP ${httpStatus}` : dirs.length ? '协议或结算分支需要复核' : '试玩入口/网络失败';
    entry.retryAt ||= Date.now() + 900000;
    if (httpStatus === 429) fs.writeFileSync(path.join(root, 'state/batch-cooldown.json'), JSON.stringify({ until: entry.retryAt, batchIndex }));
  } else {
    entry.status = nextMode(entry.counts, entry.profile, phase) == null ? 'captured-pending-import' : 'running';
    // 只有退避后的新批次完整复验成功，才清除旧网络错误，避免仍运行却显示旧暂停原因。
    delete entry.reason; delete entry.retryAt;
  }
  queueState.markEntry(entry, source);
  return { game, mode, accepted, status: entry.status, reason: entry.reason || null };
}
(async () => {
  const entries = new Map(games.map(game => [game.id, prepareEntry(game)]));
  save();
  let waves = 0, slots = 0, captured = 0;
  const maxWaves = Number(process.env.JL_BATCH_MAX_WAVES || Infinity);
  const cooldownFile = path.join(root, 'state/batch-cooldown.json');
  const cooldownUntil = () => fs.existsSync(cooldownFile) ? Number(JSON.parse(fs.readFileSync(cooldownFile)).until) || 0 : 0;
  const live = () => games.filter(game => {
    const entry = entries.get(game.id);
    if (entry.status === 'blocked') return false;
    if ((entry.retryAt || 0) > Date.now()) return false;
    return nextMode(entry.counts, entry.profile, phase) != null;
  });
  while (Date.now() < deadline && waves < maxWaves) {
    // 429 共享退避：等到退避结束或本批截止，不重置退避、不换节点、不重放失败 Spin。
    if (cooldownUntil() > Date.now()) {
      await new Promise(resolve => setTimeout(resolve, Math.max(0, Math.min(cooldownUntil(), deadline) - Date.now())));
      if (Date.now() >= deadline) break;
    }
    const pending = live();
    if (!pending.length) {
      // 暂时失败先按原退避等待，不因本批零进展提前退出；业务拒绝与超标不会自动恢复。
      const wake = retryWakeAt(games.map(game => entries.get(game.id)), Date.now(), deadline, cooldownUntil());
      if (wake === null) break;
      await new Promise(resolve => setTimeout(resolve, Math.max(0, Math.min(wake, deadline) - Date.now())));
      continue;
    }
    // 提前结束本批，为仍在执行的小批次和状态留空间，避免原件到最后才超限。
    if (batchBytes() > 8 * 1024 * 1024) { console.log('本批原件达到8MiB，提前交付并等待下一轮'); break; }
    // 波次轮转：本批每款游戏每波恰好一个执行位置，并发不超过配置，任何一款都不能独占节点。
    const offset = (waves * concurrency) % pending.length;
    const wave = pending.slice(offset).concat(pending.slice(0, offset)).slice(0, concurrency);
    waves++;
    const results = await Promise.all(wave.map(game => runSlot(game, entries.get(game.id))));
    slots += wave.length; captured += results.reduce((total, result) => total + result.accepted, 0);
    save();
    for (const result of results) console.log(JSON.stringify({ gameId: result.game.id, batchIndex, mode: result.mode, accepted: result.accepted, status: result.status, reason: result.reason }));
  }
  save();
  // 本批记录：源端通过局数、每款状态与来源轮次，供调度层与接收端逐批核对。
  const batchRecord = {
    campaignId: manifest.campaignId, batchIndex, source, phase,
    gameIds: games.map(game => game.id), waves, slots, captured, concurrency, roundBatch,
    startedAt: new Date(deadline - sliceMinutes * 60000).toISOString(), finishedAt: new Date().toISOString(),
    games: games.map(game => {
      const entry = entries.get(game.id);
      return { id: game.id, module: game.module, status: entry.status, reason: entry.reason ?? null, counts: entry.counts, imported: entry.imported, retryAt: entry.retryAt ?? null, source: entry.source }
    }),
    diskStopped: fs.existsSync(samplesRoot) && batchBytes() > 8 * 1024 * 1024,
  };
  fs.writeFileSync(path.join(root, `state/batch-${batchIndex}.json`) + '.new', JSON.stringify(batchRecord));
  fs.renameSync(path.join(root, `state/batch-${batchIndex}.json`) + '.new', path.join(root, `state/batch-${batchIndex}.json`));
  console.log(JSON.stringify({ batchIndex, gameIds: batchRecord.gameIds, waves, slots, captured }));
})().catch(error => {
  // 公开日志不输出会话信息；失败原因写进本批私有目录，便于复核而不外泄。
  try { fs.writeFileSync(path.join(privateRoot, 'worker-failure.json'), JSON.stringify({ message: String(error?.message || error), stack: String(error?.stack || '').split('\n').slice(0, 6) })); } catch { }
  console.error('本批采集队列校验失败，原件保留用于复核'); process.exitCode = 1;
});
