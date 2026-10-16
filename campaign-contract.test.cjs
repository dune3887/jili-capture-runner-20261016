const assert=require('node:assert/strict'),crypto=require('node:crypto');
const c=require('./campaign-contract.cjs');
const profile={gameMall:{PriceOdd:[50,100]},extra:{Mul:[1,1.5,8]},extraFields:['Mul']};
assert.equal(c.nextMode({'0':300000},profile,'features'),'0');
assert.equal(c.nextMode({'0':300001},profile,'normal'),null);
assert.equal(c.nextMode({'0':300001},profile,'features'),'buy:0');
assert.equal(c.nextMode({'0':300001,'buy:0':20001,'buy:1':20001,'1':20000},profile,'features'),'1');
assert.equal(c.nextMode({'0':300001,'buy:0':20001,'buy:1':20001,'1':20001,'2':20001},profile,'features'),null);
assert.deepEqual(c.featureModes({mall:{PriceOdd:100,Show:0},extra:{Mul:[1,2],ShowExtra:false},extraFields:['Mul','ShowExtra']}),['buy:0','1'],'展示字段不得使实际价格档被漏采');
assert.deepEqual(c.featureModes({rulesBuy:[60,100,80]}),['buy:0','buy:1','buy:2'],'官方已知购买尚未取得 Info 价格时必须留为待验证，不能假完成');
for(const id of [21,30,44,48,769]){
 const entry={status:'blocked',reason:'协议或结算分支需要复核',counts:{'0':1}};
 assert.equal(c.repairLocalProtocolBlock({id},entry),true);
 assert.deepEqual(entry.counts,{'0':1});entry.status='blocked';
 assert.equal(c.repairLocalProtocolBlock({id},entry),false,'适配修复只恢复一次');
 assert.equal(c.repairLocalProtocolBlock({id},{status:'blocked',reason:'官方业务拒绝 ret=304：会话不存在'}),false);
}
for(const id of [14,67,51,27,447])assert.equal(c.repairLocalProtocolBlock({id},{status:'blocked',reason:'协议或结算分支需要复核'}),false);
const key=crypto.randomBytes(32),sealed=c.seal(Buffer.from('private session'),key);
const repaired={status:'blocked',reason:'协议或结算分支需要复核',counts:{'0':17},retryAt:9999999999999};
assert.equal(c.repairLocalProtocolBlock({id:820},repaired),true);
assert.deepEqual(repaired.counts,{'0':17},'修复不能重置计数');
repaired.status='blocked';assert.equal(c.repairLocalProtocolBlock({id:820},repaired),false,'一次修复后不得自动清除新的拒绝');
assert.equal(c.repairLocalProtocolBlock({id:820},{status:'blocked',reason:'官方业务拒绝 ret=304：下注限额'}),false);
assert.equal(c.repairLocalProtocolBlock({id:658},{status:'blocked',reason:'协议或结算分支需要复核'}),false);
assert.equal(c.retryWakeAt([{status:'paused',retryAt:16000}],1000,60000),16000,'暂时失败必须等待退避后恢复');
assert.equal(c.retryWakeAt([{status:'running'},{status:'paused',retryAt:16000}],1000,60000,20000),20000,'共享429退避不能提前');
assert.equal(c.retryWakeAt([{status:'blocked',retryAt:16000}],1000,60000),null,'业务拒绝不能自动恢复');
assert.equal(c.retryWakeAt([{status:'paused',retryAt:60000}],1000,60000),null,'退避超过截止时间不再请求');
assert.equal(c.open(sealed,key).toString(),'private session');
assert.throws(()=>c.open({...sealed,tag:Buffer.alloc(16).toString('base64')},key),'篡改包必须失败');
console.log('目标边界、官方模式、暂停恢复与加密验签检查通过');

const historical={status:'blocked',reason:'官方业务拒绝 ret=304：下注限额',retryAt:123,counts:{'0':7}};
assert.equal(c.revalidateClientFlow(historical,false),false);
assert.equal(c.revalidateClientFlow(historical,true),true);assert.deepEqual(historical.counts,{'0':7});
assert.equal(historical.clientFlowReview.previous.reason,'官方业务拒绝 ret=304：下注限额');
historical.status='blocked';historical.reason='新的拒绝';assert.equal(c.revalidateClientFlow(historical,true),false);
const restored=c.restoreClientFlowEntry({status:'blocked',reason:'旧拒绝'},undefined,{revision:c.clientFlowRevision,status:'running'});
assert.equal(restored.status,'running');assert.equal(c.revalidateClientFlow(restored,true),false);
const freshRejected=c.restoreClientFlowEntry({},historical,{revision:c.clientFlowRevision,status:'running'});
assert.equal(freshRejected.reason,'新的拒绝','新拒绝不得被旧公开running状态覆盖');

const staleRunning={counts:{'0':9},status:'running',clientFlowReview:{revision:c.clientFlowRevision}};
assert.equal(c.restoreClientFlowEntry({},staleRunning,{revision:c.clientFlowRevision,status:'blocked',reason:'新版明确拒绝'}).status,'blocked','旧缓存不得覆盖已收到的新版拒绝');
