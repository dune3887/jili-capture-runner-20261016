// 验收：旧 20 节点×5 泳道缓存按真实 gameId/module 合并迁移；暂停、退避、恢复标记、
// 审计与计数都保留；迟到旧轮不能清除新暂停，旧暂停也不能覆盖更新的合法恢复状态。
const assert=require('node:assert/strict');
const s=require('./campaign-state.cjs');
const campaignId='jili-20261013-v1';
function legacyLane(modules){
  const games={};
  for(const [module,entry] of Object.entries(modules))games[module]=entry;
  return {campaignId,games};
}
// 复刻旧分配：20 节点 × 5 泳道，每款按 i%100 落到唯一泳道，互不重复。
const modules=Array.from({length:143},(_,index)=>`jl_${index+1}_g`);
const state=s.emptyState(campaignId);
let mergedTotal=0;
for(let node=0;node<20;node++)for(let lane=0;lane<5;lane++){
  const games={};
  modules.forEach((module,index)=>{if(index%100===node*5+lane)games[module]={counts:{'0':index+1},status:'blocked',reason:'协议或结算分支需要复核'};});
  if(!Object.keys(games).length)continue;
  const result=s.mergeLegacyFiles(state,{runId:49,roundAt:'2026-10-15T13:32:26Z',artifact:11980525343,files:{[`lane-${lane}.json`]:Buffer.from(JSON.stringify(legacyLane(games)))}});
  mergedTotal+=result.merged;
}
assert.equal(mergedTotal,143,'旧泳道合并必须覆盖全部 143 款，不重复计算');
assert.equal(Object.keys(state.games).length,143);
assert.equal(state.games[modules[0]].status,'blocked','旧暂停必须保留');
assert.equal(state.games[modules[0]].counts['0'],1,'旧计数必须保留');
assert.deepEqual(state.games[modules[0]].legacyLanes,['lane-0@49'],'泳道编号只作为来源审计');
assert.ok(!JSON.stringify(state.games).includes('"lane-0"'),'泳道编号不得进入新队列状态字段');
// 旧暂停、退避时间、恢复标记、审计与计数在合并后都要保留。
const paused=s.emptyState(campaignId);
s.mergeGame(paused,'jl_304_uap',{counts:{'0':35913},status:'paused',reason:'来源网络失败（未重放 Spin）',retryAt:1791900000000,profile:{gameMall:{PriceOdd:[45]}},sessionReloginRepair:'20261014-v1',clientFlowReview:{revision:'20261015-async-info-v1'},imported:{'0':35913}},{roundAt:'2026-10-15T13:32:26Z',runId:49,batchIndex:-1});
const kept=paused.games.jl_304_uap;
assert.equal(kept.retryAt,1791900000000,'退避时间必须保留');
assert.equal(kept.sessionReloginRepair,'20261014-v1','恢复标记必须保留');
assert.equal(kept.clientFlowReview.revision,'20261015-async-info-v1','复核审计必须保留');
assert.equal(kept.profile.gameMall.PriceOdd[0],45);
assert.equal(kept.counts['0'],35913);
// 迟到的旧轮 running 不能清除新轮的暂停。
s.mergeGame(paused,'jl_304_uap',{counts:{'0':35913},status:'running',source:{roundAt:'2026-10-15T12:50:28Z',runId:48,batchIndex:-1}},{roundAt:'2026-10-15T13:32:26Z',runId:49,batchIndex:-1});
assert.equal(paused.games.jl_304_uap.status,'paused','迟到旧轮不得把新暂停改回 running');
assert.equal(paused.games.jl_304_uap.reason,'来源网络失败（未重放 Spin）');
// 旧轮暂停不得覆盖更新的合法恢复状态。
const recovered=s.emptyState(campaignId);
s.mergeGame(recovered,'jl_597_gb',{counts:{'0':4733},status:'blocked',reason:'官方业务拒绝 ret=304：官方会话不存在',clientFlowReview:{revision:'20261015-async-info-v1'}},{roundAt:'2026-10-15T12:50:28Z',runId:48,batchIndex:-1});
s.mergeGame(recovered,'jl_597_gb',{counts:{'0':4733},status:'running',source:{roundAt:'2026-10-15T14:41:15Z',runId:50,batchIndex:0}},{roundAt:'2026-10-15T14:41:15Z',runId:50,batchIndex:0});
assert.equal(recovered.games.jl_597_gb.status,'running','更新的合法恢复不得被旧暂停覆盖');
assert.equal(recovered.games.jl_597_gb.clientFlowReview.revision,'20261015-async-info-v1','恢复不丢旧审计');
// 同一来源轮次内 blocked 不会被 running 清除；计数只增不减。
const same=s.emptyState(campaignId);
s.mergeGame(same,'jl_44_fivestar',{counts:{'0':2439},status:'blocked',reason:'官方业务拒绝 ret=304：下注限额'},{roundAt:'2026-10-15T14:41:15Z',runId:50,batchIndex:0});
s.mergeGame(same,'jl_44_fivestar',{counts:{'0':2400},status:'running',reason:null,retryAt:0},{roundAt:'2026-10-15T14:41:15Z',runId:50,batchIndex:0});
assert.equal(same.games.jl_44_fivestar.status,'blocked','同轮次不得用 running 清除业务拒绝');
assert.equal(same.games.jl_44_fivestar.reason,'官方业务拒绝 ret=304：下注限额');
assert.equal(same.games.jl_44_fivestar.counts['0'],2439,'计数不得因迟到/往返快照回退');
// 游标是真实 gameId，且只按来源轮次推进。
const cursorState=s.emptyState(campaignId);
s.setCursor(cursorState,'jl_304_uap',{roundAt:'2026-10-15T14:41:15Z',runId:50,batchIndex:0});
const older=s.emptyState(campaignId);s.setCursor(older,'jl_639_tgr',{roundAt:'2026-10-15T12:50:28Z',runId:48,batchIndex:3});
s.mergeQueueStates(cursorState,older,{roundAt:'2026-10-15T12:50:28Z',runId:48,batchIndex:3});
assert.equal(cursorState.cursor.module,'jl_304_uap','迟到快照不得回退游标');
assert.equal(cursorState.cursorSource.runId,50);
// 新格式队列状态：往返合并后每款状态与来源都在，且能按来源继续推进。
const live=s.emptyState(campaignId);
s.markEntry(s.mergeGame(live,'jl_639_tgr',{counts:{'0':38976},status:'running'},{roundAt:'2026-10-15T14:41:15Z',runId:50,batchIndex:0}),{roundAt:'2026-10-15T14:41:15Z',runId:50,batchIndex:0},'2026-10-15T14:45:00Z');
const roundTrip=s.queueStateOf('queue-state.json',Buffer.from(JSON.stringify(live)));
assert.equal(roundTrip.games.jl_639_tgr.source.runId,50);
assert.equal(roundTrip.games.jl_639_tgr.updatedAt,'2026-10-15T14:45:00Z');
const target=s.emptyState(campaignId);
assert.equal(s.mergeQueueStates(target,roundTrip,{roundAt:'2026-10-15T14:41:15Z',runId:50,batchIndex:0}),1);
assert.equal(target.games.jl_639_tgr.status,'running');
assert.throws(()=>s.queueStateOf('queue-state.json',Buffer.from(JSON.stringify({stateVersion:1,campaignId,games:{}}))),/版本/);
assert.throws(()=>s.mergeQueueStates(target,{campaignId:'other',games:{}}),/活动身份/);
console.log('旧泳道迁移、暂停与退避保持、来源轮次合并、游标推进检查通过');
