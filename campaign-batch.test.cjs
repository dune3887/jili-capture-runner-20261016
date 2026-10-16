// 验收：23 款模拟游戏按 10、10、3 分批无遗漏、无重复；每款恰好一个执行位置；
// 批内并发不超过配置，20 款并发只有实际资源测试证据通过后启用。
const assert=require('node:assert/strict');
const c=require('./campaign-contract.cjs');
const games=Array.from({length:23},(_,index)=>({id:100+index,module:'jl_'+(100+index)+'_x',code:String(100+index)}));
// 23 款：10、10、3 三批，覆盖完整且互不重复。
const batches=c.planBatches(games,10);
assert.deepEqual(batches.map(batch=>batch.games.length),[10,10,3],'23 款必须分成 10、10、3');
assert.deepEqual(batches.map(batch=>batch.index),[0,1,2]);
const flat=batches.flatMap(batch=>batch.games.map(game=>game.id));
assert.equal(flat.length,23,'批次合计必须恰好等于队列长度');
assert.equal(new Set(flat).size,23,'同一款不得出现在多个批次');
assert.deepEqual([...flat].sort((a,b)=>a-b),games.map(game=>game.id),'不得遗漏任何一款游戏');
const positions=new Map();
for(const batch of batches)for(const game of batch.games)positions.set(game.id,(positions.get(game.id)||0)+1);
assert.ok([...positions.values()].every(count=>count===1),'每款游戏恰好一个执行位置');
// 环形遍历：从任意游标开始，一次遍历覆盖全部批次且不重复。
for(const start of [games[0],games[10],games[22]]){
  const sweep=c.sweepFrom(batches,start.module);
  assert.equal(sweep.batches.length,3);
  const ids=sweep.batches.flatMap(batch=>batch.games.map(game=>game.id));
  assert.equal(ids.length,23);assert.equal(new Set(ids).size,23);
}
assert.deepEqual(c.sweepFrom(batches,games[10].module).batches.map(batch=>batch.index),[1,2,0]);
// 143 款清单默认 10 款一批：15 批，末批 3 款，覆盖完整。
const all=Array.from({length:143},(_,index)=>({id:index+1,module:'m'+index,code:String(index+1)}));
const large=c.planBatches(all,10);
assert.equal(large.length,15);assert.equal(large.at(-1).games.length,3);
assert.equal(new Set(large.flatMap(batch=>batch.games.map(game=>game.id))).size,143);
// 默认每批 10 款、批内并发 10；20 款并发必须有实际资源测试证据。
const base=c.resolveBatchConfig({});
assert.equal(base.games,10);assert.equal(base.concurrency,10);assert.deepEqual(base.cappedBy,[]);
const wanted20=c.resolveBatchConfig({JL_BATCH_GAMES:'20',JL_BATCH_CONCURRENCY:'20'});
assert.equal(wanted20.games,10,'没有资源测试证据时批大小必须封顶为 10');
assert.equal(wanted20.concurrency,10,'没有资源测试证据时并发必须封顶为 10');
assert.ok(wanted20.cappedBy.includes('games:20->10'),'封顶原因必须记录，不能静默降级');
const evidence={passed:true,mode:'live-capture',games:20,samples:20,peakRssMiB:512,runId:38753604739};
const allowed=c.resolveBatchConfig({JL_BATCH_GAMES:'20',JL_BATCH_CONCURRENCY:'20'},evidence);
assert.equal(allowed.games,20,'资源测试通过后才允许 20 款一批');
assert.equal(allowed.concurrency,20);
assert.equal(c.resolveBatchConfig({JL_BATCH_GAMES:'20',JL_BATCH_CONCURRENCY:'20'},{passed:false,mode:'live-capture',games:20,samples:20,peakRssMiB:1}).games,10,'失败的资源测试不能启用 20');
assert.equal(c.resolveBatchConfig({JL_BATCH_GAMES:'20',JL_BATCH_CONCURRENCY:'20'},{passed:true,mode:'offline-verify',games:20,samples:20,peakRssMiB:512}).games,10,'离线测量不能放行 20 款并发');
// 人类直接要求启用 20 路并发时，必须有明确记录的指令来源才放行；只有离线测量不够。
assert.equal(c.resolveBatchConfig({JL_BATCH_GAMES:'20',JL_BATCH_CONCURRENCY:'20'},{passed:true,mode:'offline-verify',games:20,samples:20,peakRssMiB:1322,operatorOverride:{approvedBy:'human',at:'2026-10-16T02:00:00Z',note:'人类要求单节点同时开启 20 款采集'}}).games,20,'人类直接指令并记录后可启用 20 路并发');
assert.equal(c.resolveBatchConfig({JL_BATCH_GAMES:'20',JL_BATCH_CONCURRENCY:'20'},{passed:true,mode:'offline-verify',games:20,samples:20,peakRssMiB:1322,operatorOverride:{approvedBy:'agent'}}).games,10,'非人类指令不得放行 20 路并发');
assert.equal(c.resolveBatchConfig({JL_BATCH_GAMES:'20',JL_BATCH_CONCURRENCY:'20'},{passed:true,mode:'live-capture',games:19,samples:20,peakRssMiB:1}).games,10,'证据不足 20 款不能启用 20');
assert.equal(c.resolveBatchConfig({JL_BATCH_GAMES:'10',JL_BATCH_CONCURRENCY:'20'},evidence).concurrency,10,'批内并发不得超过批大小');
assert.equal(c.resolveBatchConfig({JL_BATCH_GAMES:'4'}).concurrency,4,'不足 10 款可采时按实际数量运行');
assert.equal(c.resolveBatchConfig({JL_BATCH_GAMES:'4'}).games,4);
assert.throws(()=>c.planBatches(games,21),/批大小/);
// 批时间切片：剩余时间平均分给本次遍历的批次，单批有下限，最后一批用尽剩余时间。
assert.equal(c.batchSliceMs(4500000,1),4500000);
assert.equal(c.batchSliceMs(4500000,3),1500000);
assert.equal(c.batchSliceMs(120000,10),60000);
assert.equal(c.batchSliceMs(4500000,0),4500000);
// 队列遍历：blocked、未适配、明确停用、普通达标都记录跳过原因；可采且未达标进入队列。
const manifest={campaignId:'fixture',games:[
  {id:10,module:'jl_10_a',code:'10'},{id:262,module:'jl_262_fish',code:'262'},{id:304,module:'jl_304_uap',code:'304'},
  {id:515,module:'jl_515_x',code:'515'},{id:639,module:'jl_639_tgr',code:'639'},{id:800,module:'jl_800_done',code:'800'}],
  blocked:[{id:515,code:'515',reason:'ENOENT: 缺少 515Proto.proto'}]};
const state={stateVersion:2,campaignId:'fixture',cursor:null,games:{
  jl_10_a:{counts:{'0':12},status:'blocked',reason:'官方业务拒绝 ret=304：下注限额'},
  jl_304_uap:{counts:{'0':35913},status:'running'},
  jl_639_tgr:{counts:{'0':38976},status:'paused',reason:'来源网络失败（未重放 Spin）',retryAt:1791900000000},
  jl_800_done:{counts:{'0':300001},status:'running'}}};
const plan=c.queuePlan(manifest,state,{phase:'normal',batchSize:10});
assert.deepEqual(plan.queue.map(game=>game.id),[304,639],'只有可采且未达标的进入队列');
assert.equal(plan.queueLength,2);
const skipped=Object.fromEntries(plan.skipped.map(item=>[item.id,item]));
assert.equal(skipped[10].kind,'blocked');assert.match(skipped[10].reason,/下注限额/);
assert.equal(skipped[262].kind,'not-adapted','明确停用的游戏不能进入 Slots 队列');
assert.equal(skipped[515].kind,'not-adapted');assert.match(skipped[515].reason,/Proto/);
assert.equal(skipped[800].kind,'normal-target-met');
assert.equal(plan.batches.length,1);assert.deepEqual(plan.batches[0].games.map(game=>game.id),[304,639]);
// 游标是真实 gameId：从 639 开始遍历时顺序轮转，仍覆盖同一批全部游戏。
const rotated=c.queuePlan(manifest,state,{phase:'normal',batchSize:1,cursor:{module:'jl_639_tgr'}});
assert.deepEqual(rotated.batches.map(batch=>batch.games.map(game=>game.id)),[[639],[304]]);
// 明确停用清单必须固定，不能因为凑并发而解除暂停。
assert.deepEqual(c.disabledGameIds,[262,389,420,468,546,512,551,690]);
console.log('分批（23 款 10/10/3）、游标环形遍历、并发上限与队列跳过原因检查通过');
