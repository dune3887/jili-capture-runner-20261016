// 公开的 GitHub 调度入口（单节点）：计划、调度、采集、按批归档都在同一作业内完成。
// 运行包与结果均加密，SSH/MongoDB 凭据不会传入运行器；上传失败时检查点不推进。
const fs = require('node:fs'), path = require('node:path'), crypto = require('node:crypto'), zlib = require('node:zlib'), assert = require('node:assert/strict');
const { spawn, execFileSync } = require('node:child_process');
const { seal, open, queuePlan, resolveBatchConfig, batchSliceMs, batchLimits, nextMode } = require('./campaign-contract.cjs');
const queueState = require('./campaign-state.cjs');
const enginePackage = require('./engine-package.cjs');
const pilot = process.env.JL_PILOT === 'true';
const minutes = Number(process.env.JL_MINUTES || 45);
if (!Number.isInteger(minutes) || minutes < 1 || minutes > 50) throw Error('运行时间必须为 1 至 50 分钟');
const key = Buffer.from(process.env.JL_ENGINE_KEY || '', 'hex');
if (key.length !== 32) throw Error('运行包解密密钥未配置');
const root = path.resolve('engine'), output = path.resolve('output');
fs.mkdirSync(root, { recursive: true, mode: 0o700 }); fs.mkdirSync(output, { recursive: true });
const receipts = JSON.parse(fs.readFileSync('receipts.json'));
// 单节点也保留包体与积压保护：每个批次独立加密、独立交付，任一批次超过 12MiB 即停止并保留原件。
const maxBatchBytes = 12 * 1024 * 1024, minFreeBytes = 1024 * 1024 * 1024;
// 工作流为每批准备 4 个上传位置：超过 4 个批次本轮不再继续，避免采集了却无法交付。
const maxBatchesPerRun = Math.min(4, Number(process.env.JL_MAX_BATCHES || 4));
// 人类要求：单节点同时可采不足阈值时提前释放节点（池子本身就不足时不空转等待）。
const minActiveGames = Math.min(20, Math.max(1, Number(process.env.JL_MIN_ACTIVE_GAMES || 10)));
function liveGameCount(manifest, state, phase) {
  let live = 0;
  for (const game of manifest.games) {
    const entry = state.games[game.module] || {};
    if (entry.status === 'blocked') continue;
    if ((entry.retryAt || 0) > Date.now()) continue;
    if (nextMode(entry.counts || {}, entry.profile, phase) == null) continue;
    live++;
  }
  return live;
}
const token = process.env.GITHUB_TOKEN;
async function api(route, method = 'GET') {
  const response = await fetch('https://api.github.com/repos/' + process.env.GITHUB_REPOSITORY + route, { method, headers: { Authorization: 'Bearer ' + token, Accept: 'application/vnd.github+json', 'X-GitHub-Api-Version': '2022-11-28' } });
  if (!response.ok) throw Error('GitHub 产物管理失败 HTTP ' + response.status);
  return response.status === 204 ? null : response.json();
}
function filesIn(directory) {
  const files = {}; if (!fs.existsSync(directory)) return files;
  for (const file of fs.readdirSync(directory, { recursive: true })) { const name = String(file), full = path.join(directory, name); if (fs.statSync(full).isFile()) files[name] = fs.readFileSync(full).toString('base64'); }
  return files;
}
function unpack(files, directory) {
  for (const [name, bytes] of Object.entries(files)) { if (!/^[\w./-]+$/.test(name) || path.isAbsolute(name) || name.split('/').includes('..')) throw Error('运行包路径非法'); const file = path.join(directory, name); fs.mkdirSync(path.dirname(file), { recursive: true }); fs.writeFileSync(file, Buffer.from(bytes, 'base64'), { mode: 0o600 }); }
}
function freeBytes() { try { const stats = fs.statfsSync(root); return stats.bavail * stats.bsize; } catch { return Infinity; } }
function launchWorker(batch, sliceMs, config, source) {
  return new Promise(resolve => {
    const env = {
      PATH: process.env.PATH, JL_PILOT: String(pilot), JL_MINUTES: String(minutes), JL_PHASE: process.env.JL_PHASE || 'normal',
      JL_REQUEST_INTERVAL_MS: '500', JL_SPIN_INTERVAL_MS: process.env.JL_SPIN_INTERVAL_MS || '500', JL_MAX_RECREATE: '0', JL_MAX_SESSION_RELOGIN: '3',
      JL_CLIENT_FLOW_REVALIDATION: JSON.parse(fs.readFileSync('release.json')).clientFlowRevalidation || '',
      JL_BATCH_MINUTES: String(Math.max(1 / 60, sliceMs / 60000)), JL_BATCH_CONCURRENCY: String(config.concurrency), JL_ROUND_BATCH: '30',
      // 单款每轮的小批次数量上限：pilot 固定一小批次，其余可运维收紧，避免任何一款长期占用节点。
      JL_BATCH_MAX_WAVES: pilot ? '1' : (process.env.JL_BATCH_MAX_WAVES || ''), JL_SOURCE_RUN: String(source.runId), JL_SOURCE_ROUND_AT: source.roundAt,
    };
    const child = spawn(process.execPath, [path.join(root, 'campaign-worker.cjs'), String(batch.index), batch.games.map(game => game.id).join(',')], { cwd: root, env, stdio: 'inherit' });
    child.on('error', () => resolve(1)); child.on('close', status => resolve(status));
  });
}
(async () => {
  const runId = Number(process.env.GITHUB_RUN_ID || 0);
  // 可验证来源轮次：优先使用 GitHub 记录的 run 创建时间，供接收端按轮次合并状态。
  let roundAt = new Date().toISOString();
  try { if (runId) roundAt = (await api('/actions/runs/' + runId)).created_at || roundAt; } catch { console.log('运行元数据读取失败，使用本机时间作为来源轮次'); }
  const source = { runId, roundAt };
  const artifacts = []; for (let page = 1; ; page++) { const result = await api(`/actions/artifacts?per_page=100&page=${page}`); artifacts.push(...result.artifacts); if (result.artifacts.length < 100) break; }
  // 清理仅限已验证入库且本地已归档的本项目临时产物；未入库产物不会在这里被删除。
  for (const artifact of artifacts) { if (receipts.artifacts.includes(String(artifact.id)) && /^jl-capture-/.test(artifact.name) && !artifact.expired) await api('/actions/artifacts/' + artifact.id, 'DELETE'); }
  // 沿用既有 150MiB 积压保护：未确认入库的产物超限时暂停本轮采集。
  const pending = artifacts.filter(artifact => !artifact.expired && !receipts.artifacts.includes(String(artifact.id))).reduce((total, artifact) => total + artifact.size_in_bytes, 0);
  if (pending > 150 * 1024 * 1024) { console.log('未入库产物超过 150MiB，暂停采集等待整合'); return; }
  const bundle = JSON.parse(zlib.gunzipSync(open(enginePackage.read(process.cwd()), key), { maxOutputLength: 100 * 1024 * 1024 }));
  if (bundle.manifest.campaignId !== receipts.campaignId) throw Error('活动身份不符');
  unpack(bundle.files, root);
  fs.writeFileSync(path.join(root, 'campaign-manifest.json'), JSON.stringify(bundle.manifest));
  fs.writeFileSync(path.join(root, 'receipts.json'), JSON.stringify(receipts));
  // 队列状态：仓库根目录的迁移状态（旧 20 节点×5 泳道按真实 gameId/module 合并）叠加本次检查点。
  const state = queueState.emptyState(bundle.manifest.campaignId);
  if (fs.existsSync('campaign-state.json')) {
    const migrated = queueState.queueStateOf('campaign-state.json', fs.readFileSync('campaign-state.json'));
    queueState.mergeQueueStates(state, migrated, { runId: 0, roundAt: migrated.cursorUpdatedAt || '', batchIndex: 0 });
    state.migration = migrated.migration || null;
  }
  fs.writeFileSync(path.join(root, 'campaign-state.json'), JSON.stringify(state));
  if (fs.existsSync('state.enc.json')) {
    const checkpoint = JSON.parse(open(JSON.parse(fs.readFileSync('state.enc.json')), key));
    for (const [name, bytes] of Object.entries(checkpoint)) {
      if (name === 'queue-state.json') queueState.mergeQueueStates(state, queueState.queueStateOf(name, Buffer.from(bytes, 'base64')), { runId, roundAt, batchIndex: Number.MAX_SAFE_INTEGER });
      else { const file = path.join(root, 'state', name); fs.mkdirSync(path.dirname(file), { recursive: true }); fs.writeFileSync(file, Buffer.from(bytes, 'base64'), { mode: 0o600 }); }
    }
  }
  fs.mkdirSync(path.join(root, 'state'), { recursive: true });
  fs.writeFileSync(path.join(root, 'state/queue-state.json'), JSON.stringify(state));
  execFileSync('npm', ['ci', '--omit=dev', '--ignore-scripts', '--no-audit', '--no-fund'], { cwd: root, stdio: 'inherit' });
  // 资源测试证据决定是否允许把批大小/并发提到 20；默认每批 10 款、批内并发 10。
  const resourceTestFile = 'resource-test.json';
  const resourceTest = fs.existsSync(resourceTestFile) ? JSON.parse(fs.readFileSync(resourceTestFile)) : null;
  const config = resolveBatchConfig({ JL_BATCH_GAMES: process.env.JL_BATCH_GAMES, JL_BATCH_CONCURRENCY: process.env.JL_BATCH_CONCURRENCY }, resourceTest);
  const batchSize = pilot ? Math.min(config.games, Number(process.env.JL_BATCH_PILOT_GAMES || 2)) : config.games;
  const plan = queuePlan(bundle.manifest, state, { phase: process.env.JL_PHASE || 'normal', batchSize, cursor: state.cursor });
  const runDeadline = Date.now() + minutes * 60000;
  const queueReport = { campaignId: bundle.manifest.campaignId, runId, source, head: process.env.GITHUB_SHA || null, minutes, nodes: 1, pilot, config, batchSize, queueLength: plan.queueLength, queue: plan.queue.map(game => ({ id: game.id, module: game.module })), skipped: plan.skipped, batches: plan.batches.map(batch => ({ index: batch.index, gameIds: batch.games.map(game => game.id) })), delivery: [] };
  console.log(JSON.stringify({ nodes: 1, runId, queueLength: plan.queueLength, skipped: plan.skipped.length, batches: queueReport.batches, config }));
  const delivered = [];
  const startLive = liveGameCount(bundle.manifest, state, process.env.JL_PHASE || 'normal');
  console.log(JSON.stringify({ nodes: 1, runId, startLive, minActiveGames }));
  const batches = pilot ? plan.batches.slice(0, 1) : plan.batches;
  for (let position = 0; position < batches.length && delivered.length < maxBatchesPerRun; position++) {
    const batch = batches[position], remaining = runDeadline - Date.now();
    // 池子原本够、如今同时可采不足阈值：不再占用节点，剩余可采游戏留给下一轮。
    if (position > 0 && startLive >= minActiveGames && liveGameCount(bundle.manifest, state, process.env.JL_PHASE || 'normal') < minActiveGames) {
      console.log('同时可采游戏不足 ' + minActiveGames + ' 款，提前释放本节点');
      break;
    }
    if (remaining <= 5000) { console.log('运行时间已到，安全收尾'); break; }
    if (freeBytes() < minFreeBytes) { console.log('本节点剩余磁盘不足 1GiB，安全收尾'); break; }
    const batchesLeft = Math.min(batches.length - position, maxBatchesPerRun - delivered.length);
    const sliceMs = Math.min(batchSliceMs(remaining, batchesLeft), remaining);
    const status = await launchWorker(batch, sliceMs, config, source);
    // 必须把工作进程写回的状态按来源轮次并回调度状态：不能用运行前的内存副本覆盖本批计数。
    const workerStateFile = path.join(root, 'state/queue-state.json');
    if (fs.existsSync(workerStateFile)) queueState.mergeQueueStates(state, queueState.queueStateOf('queue-state.json', fs.readFileSync(workerStateFile)), { ...source, batchIndex: batch.index });
    const recordFile = path.join(root, `state/batch-${batch.index}.json`);
    const record = fs.existsSync(recordFile) ? JSON.parse(fs.readFileSync(recordFile)) : null;
    const executed = record ? record.slots : 0;
    // 游标推进到下一个批次的真实 gameId；本批没有获得任何执行位置时保持原游标重试。
    const nextBatch = batches[(position + 1) % batches.length];
    if (executed > 0) queueState.setCursor(state, nextBatch.games[0].module, { ...source, batchIndex: nextBatch.index });
    fs.writeFileSync(path.join(root, 'state/queue-state.json'), JSON.stringify(state));
    const samples = filesIn(path.join(root, `samples/batch-${batch.index}`));
    const states = { 'queue-state.json': Buffer.from(JSON.stringify(state)).toString('base64'), ...(record ? { [`batch-${batch.index}.json`]: Buffer.from(JSON.stringify(record)).toString('base64') } : {}) };
    const payload = zlib.gzipSync(Buffer.from(JSON.stringify({
      campaignId: bundle.manifest.campaignId, source, batch: { index: batch.index, gameIds: batch.games.map(game => game.id), sliceMinutes: sliceMs / 60000, executed, waves: record?.waves ?? 0, captured: record?.captured ?? 0, diskStopped: record?.diskStopped ?? false, workerStatus: status, cursor: state.cursor },
      samples, states, queueReport: { ...queueReport, cursor: state.cursor },
    })), { level: 9 });
    if (payload.length > maxBatchBytes) { console.log(`第 ${batch.index} 批产物超过 12MiB，未发布该批并安全收尾；原件保留在运行包内`); break; }
    const resultKey = crypto.randomBytes(32), sealed = seal(payload, resultKey);
    sealed.sealedKey = crypto.publicEncrypt({ key: bundle.resultPublicKey, oaepHash: 'sha256' }, resultKey).toString('base64');
    fs.writeFileSync(path.join(output, `jl-batch-${batch.index}.enc.json`), JSON.stringify(sealed));
    fs.writeFileSync(path.join(output, `jl-state-${batch.index}.enc.json`), JSON.stringify(seal(Buffer.from(JSON.stringify({ 'queue-state.json': Buffer.from(JSON.stringify(state)).toString('base64') })), key)));
    delivered.push({ index: batch.index, gameIds: batch.games.map(game => game.id), captured: record?.captured ?? 0, executed, files: [`jl-batch-${batch.index}.enc.json`] });
    queueReport.delivery.push({ index: batch.index, gameIds: batch.games.map(game => game.id), captured: record?.captured ?? 0, executed });
    console.log(JSON.stringify({ delivered: batch.index, gameIds: batch.games.map(game => game.id), captured: record?.captured ?? 0, status }));
    if (Date.now() >= runDeadline) break;
  }
  fs.writeFileSync(path.join(output, 'run-report.json'), JSON.stringify({ ...queueReport, delivered, finishedAt: new Date().toISOString() }, null, 2));
  if (delivered.length) { fs.writeFileSync(path.join(output, 'delivery.json'), JSON.stringify({ campaignId: bundle.manifest.campaignId, source, delivered })); if (process.env.GITHUB_OUTPUT) fs.appendFileSync(process.env.GITHUB_OUTPUT, `produced=true\nbatches=${delivered.length}\n`); }
  console.log(JSON.stringify({ ok: true, delivered: delivered.map(item => item.index), maxBatchBytes, config: config.cappedBy }));
})().catch(error => { console.error(error.message.replace(/https?:\/\/\S+/g, '[会话信息省略]')); process.exitCode = 1; });
