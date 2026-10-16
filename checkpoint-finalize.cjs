// 检查点只按“连续上传成功”的批次推进：任何一批上传失败或未交付都不能越过它，
// 未实际入库的批次也不会生成清理回执。
const fs = require('node:fs'), path = require('node:path');
const delivery = JSON.parse(fs.readFileSync('output/delivery.json'));
const outcomes = [process.env.U0, process.env.U1, process.env.U2, process.env.U3];
const uploaded = [];
let last = null;
for (let index = 0; index < delivery.delivered.length && index < outcomes.length; index++) {
  if (outcomes[index] !== 'success') break;
  uploaded.push(delivery.delivered[index].index);
  last = delivery.delivered[index].index;
}
let checkpoint = false;
if (last !== null) {
  const file = path.join('output', `jl-state-${last}.enc.json`);
  // 与 cache/restore 使用同一路径，避免缓存还原后文件名与读取路径不一致。
  if (fs.existsSync(file)) { fs.copyFileSync(file, 'state.enc.json'); checkpoint = true; }
}
const summary = { delivered: delivery.delivered.map(item => item.index), uploaded, checkpoint, lastBatch: last, failedSlot: uploaded.length < delivery.delivered.length ? uploaded.length : null };
console.log(JSON.stringify(summary));
if (process.env.GITHUB_OUTPUT) fs.appendFileSync(process.env.GITHUB_OUTPUT, `checkpoint=${checkpoint}\nuploaded=${uploaded.length}\nlastBatch=${last === null ? '' : last}\n`);
