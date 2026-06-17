// 改 5 个飞书 record 轮次/任务类型/User Prompt 字段
// 5551/5553/5554/5555: R2; 5559: R3
// User Prompt: PROMPT.txt → NEXT_PROMPT_R1.txt
const fs = require('fs');
const { execFileSync } = require('child_process');
const https = require('https');
const { URL } = require('url');

const APP_ID = 'cli_a95fb8c174785cc9';
const APP_SECRET = 'gXVlL9GVkHPhMU90XCbwIgfpj5fMOYEK';
const APP_TOKEN = 'IkR2b7p55aXzHNst41AcxvoxnVb';
const TABLE_ID = 'tbldgCbC3v0MO8pv';

const PROXIES = [
  { folder: '5551-rust-fs',          recordId: 'recvmJ89FqEu2o', round: 2, roundZh: '第二轮' },
  { folder: '5553-go-search',        recordId: 'recvmJbAmtxi3D', round: 2, roundZh: '第二轮' },
  { folder: '5554-python-celery-alt', recordId: 'recvmJ8VvYvPfB', round: 2, roundZh: '第二轮' },
  { folder: '5555-vue-cms',          recordId: 'recvmJ9CBvC6yq', round: 2, roundZh: '第二轮' },
  { folder: '5559-python-bio',       recordId: 'recvmJ9fTaCpZH', round: 3, roundZh: '第三轮' },
];

function httpsRequest(urlStr, options, body) {
  return new Promise((resolve, reject) => {
    const u = new URL(urlStr);
    const opts = {
      hostname: u.hostname,
      port: u.port || 443,
      path: u.pathname + u.search,
      method: options.method || 'GET',
      headers: options.headers || {},
    };
    const req = https.request(opts, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        try { resolve({ status: res.statusCode, body: JSON.parse(data) }); }
        catch (e) { resolve({ status: res.statusCode, body: data }); }
      });
    });
    req.on('error', reject);
    if (body) req.write(body);
    req.end();
  });
}

async function main() {
  // get token
  const tokenRes = await httpsRequest(
    'https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal',
    { method: 'POST', headers: { 'Content-Type': 'application/json; charset=utf-8' } },
    JSON.stringify({ app_id: APP_ID, app_secret: APP_SECRET })
  );
  const token = tokenRes.body.tenant_access_token;
  console.log(`Got token: ${token.slice(0, 20)}...\n`);

  for (const p of PROXIES) {
    console.log(`=== ${p.folder} (${p.roundZh}) ===`);

    // 1. 读 NEXT_PROMPT_R1.txt (新 User Prompt)
    const nextPromptPath = `/c/Users/白东鑫/work01/SoloCoder/${p.folder}/NEXT_PROMPT_R1.txt`;
    const userPrompt = fs.readFileSync(nextPromptPath, 'utf8');

    // 2. 拿当前 record 字段
    const getRes = await httpsRequest(
      `https://open.feishu.cn/open-apis/bitable/v1/apps/${APP_TOKEN}/tables/${TABLE_ID}/records/${p.recordId}`,
      { headers: { 'Authorization': `Bearer ${token}` } }
    );
    const curFields = getRes.body.data?.record?.fields || {};

    // 3. 构造新 fields
    const newFields = {
      ...curFields,
      '轮次': p.roundZh,
      '任务类型': 'Bug修复',
      'User Prompt': userPrompt,
    };

    // 4. PUT 改字段 (plain string)
    const putRes = await httpsRequest(
      `https://open.feishu.cn/open-apis/bitable/v1/apps/${APP_TOKEN}/tables/${TABLE_ID}/records/${p.recordId}`,
      {
        method: 'PUT',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json; charset=utf-8',
        },
      },
      JSON.stringify({ fields: newFields })
    );
    console.log(`  PUT status: ${putRes.status}, code: ${putRes.body.code}, msg: ${putRes.body.msg || ''}`);

    if (putRes.body.code !== 0) {
      console.log(`  ❌ 失败, full body: ${JSON.stringify(putRes.body).slice(0, 500)}`);
    } else {
      console.log(`  ✅ 已更新 轮次→${p.roundZh}, 任务类型→Bug修复, User Prompt→NEXT_PROMPT_R1.txt (${userPrompt.length} 字符)`);
    }
    console.log('');
  }
}

main().catch(e => { console.error(e); process.exit(1); });
