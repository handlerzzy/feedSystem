import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

// ==============================================================================
// FeedFlow 核心推荐流阶梯并发压测场景脚本 (k6)
//
// 压测目标：
//   - 接口：POST /feed/listLatest (携带已登录用户的真实点赞 Token)
//   - 穿透：内存向量池余弦检索 + Redis 曝光过滤 + 本地多特征精排 + 多路配额融合
//   - 阶梯加压：50 ➔ 200 ➔ 500 虚拟用户 (VU)
//   - SLA 红线预算：P99 < 25ms, P95 < 15ms, 错误率 < 0.1%
// ==============================================================================

// 自定义业务指标监控
const feedLatency = new Trend('feed_recommend_latency_ms', true);
const feedSuccessRate = new Rate('feed_success_rate');
const feedReqCounter = new Counter('feed_request_total');

// 加载预热好的测试账号凭据池
let tokens = [];
try {
  tokens = JSON.parse(open('./tokens.json'));
} catch (e) {
  tokens = [{ token: '', username: 'anonymous', account_id: 0 }];
}

const BASE_URL = __ENV.BASE_URL || 'http://backend:8080';
const isSmoke = __ENV.SMOKE === 'true';

const target1 = Number(__ENV.TARGET_1 || 50);
const target2 = Number(__ENV.TARGET_2 || 200);
const target3 = Number(__ENV.TARGET_3 || 500);

const thresholdP99 = __ENV.THRESHOLD_P99 || (isSmoke ? '50' : '450');
const thresholdP95 = __ENV.THRESHOLD_P95 || (isSmoke ? '30' : '300');

function parseDurationSeconds(d) {
  if (!d) return 0;
  if (typeof d === 'number') return d;
  const match = String(d).match(/^(\d+(?:\.\d+)?)(s|m|h)?$/);
  if (!match) return 0;
  const val = parseFloat(match[1]);
  const unit = match[2] || 's';
  if (unit === 's') return val;
  if (unit === 'm') return val * 60;
  if (unit === 'h') return val * 3600;
  return val;
}

const s1Ramp = __ENV.STAGE_1_RAMP || '10s';
const s1Hold = __ENV.STAGE_1_HOLD || '20s';
const s2Ramp = __ENV.STAGE_2_RAMP || '15s';
const s2Hold = __ENV.STAGE_2_HOLD || '30s';
const s3Ramp = __ENV.STAGE_3_RAMP || '15s';
const s3Hold = __ENV.STAGE_3_HOLD || '30s';
const sCooldown = __ENV.STAGE_COOLDOWN || '10s';

const stage2StartSec = parseDurationSeconds(s1Ramp) + parseDurationSeconds(s1Hold);
const stage3StartSec = stage2StartSec + parseDurationSeconds(s2Ramp) + parseDurationSeconds(s2Hold);

const lightP99Target = target1 <= 50 ? '25' : '100';
const normalP99Target = target2 <= 200 ? '100' : thresholdP99;

// 声明式场景配置
export const options = isSmoke
  ? {
      // 冒烟模式：1 VU，轻量校验链路连通性与 Token 解析
      vus: 1,
      duration: '3s',
      summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(50)', 'p(90)', 'p(95)', 'p(99)'],
      thresholds: {
        http_req_duration: [`p(99)<${thresholdP99}`, `p(95)<${thresholdP95}`],
        http_req_failed: ['rate<0.01'],
      },
    }
  : {
      summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(50)', 'p(90)', 'p(95)', 'p(99)'],
      scenarios: {
        stage_light: {
          executor: 'ramping-vus',
          startVUs: 0,
          stages: [
            { duration: s1Ramp, target: target1 },
            { duration: s1Hold, target: target1 },
          ],
          gracefulRampDown: '5s',
        },
        stage_normal: {
          executor: 'ramping-vus',
          startTime: `${stage2StartSec}s`,
          startVUs: 0,
          stages: [
            { duration: s2Ramp, target: target2 },
            { duration: s2Hold, target: target2 },
          ],
          gracefulRampDown: '5s',
        },
        stage_peak: {
          executor: 'ramping-vus',
          startTime: `${stage3StartSec}s`,
          startVUs: 0,
          stages: [
            { duration: s3Ramp, target: target3 },
            { duration: s3Hold, target: target3 },
            { duration: sCooldown, target: 0 },
          ],
          gracefulRampDown: '5s',
        },
      },
      thresholds: {
        // 多梯度 SLA 预算分级与排队论对齐（Spec Line 71：≤50 VU: P99 ≤ 25ms; 200 VU: P99 ≤ 100ms; 500 VU: P99 ≤ 450ms）
        'http_req_duration{scenario:stage_light}': [`p(99)<${lightP99Target}`],
        'http_req_duration{scenario:stage_normal}': [`p(99)<${normalP99Target}`],
        'http_req_duration{scenario:stage_peak}': [`p(99)<${thresholdP99}`],
        http_req_duration: [`p(99)<${thresholdP99}`],
        http_req_failed: ['rate<0.001'],
      },
    };

export default function () {
  // 根据 VU 编号均衡分配已登录用户凭据
  const user = tokens.length > 0 ? tokens[(__VU - 1) % tokens.length] : null;
  const token = user ? user.token : '';

  const url = `${BASE_URL}/feed/listLatest`;
  const payload = JSON.stringify({
    limit: 10,
  });

  const headers = {
    'Content-Type': 'application/json',
  };
  if (token) {
    headers['Authorization'] = `Bearer ${token}`;
  }

  const params = {
    headers: headers,
    timeout: '5s',
  };

  const res = http.post(url, payload, params);

  const ok = check(res, {
    'HTTP 状态码为 200': (r) => r.status === 200,
    '响应体包含 video_list 数组': (r) => {
      try {
        const body = JSON.parse(r.body);
        return Array.isArray(body.video_list) && body.video_list.length > 0;
      } catch (e) {
        return false;
      }
    },
  });

  feedLatency.add(res.timings.duration);
  feedSuccessRate.add(ok);
  feedReqCounter.add(1);

  // 少量 pacing 防止纯自旋发压打爆客户端本地文件描述符
  if (__ENV.PACING_MS) {
    sleep(Number(__ENV.PACING_MS) / 1000);
  }
}

// 压测完成后的可视化控制台与基准报告输出
export function handleSummary(data) {
  const req = data.metrics.http_req_duration;
  const failed = data.metrics.http_req_failed;
  const reqs = data.metrics.http_reqs;

  const p50 = req && req.values ? (req.values['p(50)'] || req.values['med'] || 0).toFixed(2) : '0.00';
  const p90 = req && req.values ? (req.values['p(90)'] || 0).toFixed(2) : '0.00';
  const p95 = req && req.values ? (req.values['p(95)'] || 0).toFixed(2) : '0.00';
  const p99 = req && req.values ? (req.values['p(99)'] || 0).toFixed(2) : '0.00';
  const avg = req && req.values ? (req.values['avg'] || 0).toFixed(2) : '0.00';
  const min = req && req.values ? (req.values['min'] || 0).toFixed(2) : '0.00';
  const max = req && req.values ? (req.values['max'] || 0).toFixed(2) : '0.00';

  const totalReqs = reqs && reqs.values ? reqs.values.count : 0;
  const rateQPS = reqs && reqs.values ? (reqs.values.rate || 0).toFixed(1) : '0.0';
  const failRate = failed && failed.values ? (failed.values.rate * 100).toFixed(3) : '0.000';

  const slaTargetP99 = target3 <= 50 ? '≤ 25 ms' : `≤ ${thresholdP99} ms (单机峰值)`;
  const slaTargetP95 = target3 <= 50 ? '≤ 15 ms' : `≤ ${thresholdP95} ms`;
  const p99ThresholdNum = Number(thresholdP99);
  const p95ThresholdNum = Number(thresholdP95);
  const p99Pass = Number(p99) <= p99ThresholdNum ? `✅ PASS (≤${thresholdP99}ms)` : `❌ FAIL (>${thresholdP99}ms)`;
  const p95Pass = Number(p95) <= p95ThresholdNum ? `✅ PASS (≤${thresholdP95}ms)` : `❌ FAIL (>${thresholdP95}ms)`;
  const p50Target = target3 <= 50 ? 10 : 150;
  const p50Pass = Number(p50) <= p50Target ? `✅ PASS (≤${p50Target}ms)` : `❌ FAIL (>${p50Target}ms)`;
  const p90Target = target3 <= 50 ? 20 : 250;
  const p90Pass = Number(p90) <= p90Target ? `✅ PASS (≤${p90Target}ms)` : `❌ FAIL (>${p90Target}ms)`;
  const errorPass = Number(failRate) < 0.1 ? '✅ PASS (<0.1%)' : '❌ FAIL (>=0.1%)';

  // 多梯度场景指标提取
  const lightReq = data.metrics['http_req_duration{scenario:stage_light}'];
  const normalReq = data.metrics['http_req_duration{scenario:stage_normal}'];
  const peakReq = data.metrics['http_req_duration{scenario:stage_peak}'];

  const getMetricP99 = (m) => (m && m.values && m.values['p(99)'] != null ? m.values['p(99)'].toFixed(2) : null);
  const getMetricP95 = (m) => (m && m.values && m.values['p(95)'] != null ? m.values['p(95)'].toFixed(2) : null);

  const lightP99 = getMetricP99(lightReq);
  const lightP95 = getMetricP95(lightReq);
  const normalP99 = getMetricP99(normalReq);
  const peakP99 = getMetricP99(peakReq);

  const tieredConsoleSection = !isSmoke && (lightP99 != null || normalP99 != null || peakP99 != null) ? `
--------------------------------------------------------------------------------
  多梯度阶梯 SLA 达成情况 (Tiered SLA Breakdown):
    - Tier 2 轻载 (≤${target1} VU):  P99 = ${lightP99 || '-'} ms, P95 = ${lightP95 || '-'} ms  [门禁: ${lightP99 && Number(lightP99) <= 25 ? '✅ PASS (≤25ms)' : '❌ FAIL'}]
    - Tier 3 常态 (${target2} VU):    P99 = ${normalP99 || '-'} ms  [门禁: ${normalP99 && Number(normalP99) <= 100 ? '✅ PASS (≤100ms)' : '❌ FAIL'}]
    - Tier 4 峰值 (${target3} VU):    P99 = ${peakP99 || '-'} ms  [门禁: ${peakP99 && Number(peakP99) <= p99ThresholdNum ? `✅ PASS (≤${thresholdP99}ms)` : '❌ FAIL'}]` : '';

  const consoleReport = `
================================================================================
           FeedFlow 推荐流端到端高并发压测基准看板 (k6 Benchmark)           
================================================================================
  总请求量 (Requests):     ${totalReqs}
  吞吐能力 (Throughput):   ${rateQPS} QPS
  请求失败率 (Fail Rate):  ${failRate}%  [门禁: ${errorPass}]
--------------------------------------------------------------------------------
  延迟分位数 (Latency Percentiles):
    - Min:                 ${min} ms
    - Avg:                 ${avg} ms
    - P50 (中位数):        ${p50} ms  [门禁: ${p50Pass}]
    - P90:                 ${p90} ms  [门禁: ${p90Pass}]
    - P95:                 ${p95} ms  [门禁: ${p95Pass}]
    - P99:                 ${p99} ms  [门禁: ${p99Pass}]
    - Max:                 ${max} ms${tieredConsoleSection}
================================================================================
`;

  const tieredMarkdownSection = !isSmoke && (lightP99 != null || normalP99 != null || peakP99 != null) ? `
## 2. 多梯度阶梯 SLA 达成详情

| 阶梯梯度 | 并发规模 (VU) | 实测 P99 | 实测 P95 | SLA 门禁目标 | 判定 |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Tier 2: 轻载基线** | ≤ ${target1} VU | \`${lightP99 || '-'} ms\` | \`${lightP95 || '-'} ms\` | P99 ≤ 25 ms, P95 ≤ 15 ms | ${lightP99 && Number(lightP99) <= 25 ? '✅ PASS (≤25ms)' : (lightP99 ? '❌ FAIL' : '-')} |
| **Tier 3: 常态高负荷** | ${target2} VU | \`${normalP99 || '-'} ms\` | - | P99 ≤ 100 ms | ${normalP99 && Number(normalP99) <= 100 ? '✅ PASS (≤100ms)' : (normalP99 ? '❌ FAIL' : '-')} |
| **Tier 4: 单机极限冲击** | ${target3} VU | \`${peakP99 || '-'} ms\` | - | P99 ≤ ${thresholdP99} ms | ${peakP99 && Number(peakP99) <= p99ThresholdNum ? `✅ PASS (≤${thresholdP99}ms)` : (peakP99 ? '❌ FAIL' : '-')} |
` : '';

  const markdownReport = `# FeedFlow 推荐流端到端压测性能基准看板

> 压测时间: ${new Date().toISOString()}  
> 发压端: Dockerized \`grafana/k6\` 容器网络直连  
> 目标接口: \`POST /feed/listLatest\` (带真实点赞身份凭据)
> 并发模式: ${isSmoke ? '1 VU 冒烟验证' : `阶梯并发 (${target1} ➔ ${target2} ➔ ${target3} VU)`}

## 1. 核心吞吐与质量指标

| 指标 | 实测数值 | SLA 目标 | 门禁判定 |
| :--- | :--- | :--- | :--- |
| **总请求数** | \`${totalReqs}\` | - | - |
| **吞吐吞吐率 (QPS)** | \`${rateQPS} req/s\` | - | - |
| **P50 延迟** | \`${p50} ms\` | ≤ ${p50Target} ms | ${p50Pass} |
| **P90 延迟** | \`${p90} ms\` | ≤ ${p90Target} ms | ${p90Pass} |
| **P95 延迟** | \`${p95} ms\` | ${slaTargetP95} | ${p95Pass} |
| **P99 延迟** | \`${p99} ms\` | ${slaTargetP99} | ${p99Pass} |
| **请求失败率** | \`${failRate}%\` | < 0.1% | ${errorPass} |
${tieredMarkdownSection}
## ${tieredMarkdownSection ? '3' : '2'}. 统计明细

- **最小耗时 (Min)**: \`${min} ms\`
- **平均耗时 (Avg)**: \`${avg} ms\`
- **最大耗时 (Max)**: \`${max} ms\`
`;

  return {
    stdout: consoleReport,
    '/test/summary.json': JSON.stringify(data, null, 2),
    '/test/summary.md': markdownReport,
  };
}
