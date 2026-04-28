/**
 * 試合詳細分析ページ
 *
 * URLパラメータ: ?id=<match_id>
 * データ: analysis-data/<id>.json を fetch して描画する
 *
 * 依存: ssl-field.js, heatmap-renderer.js (先に読み込み済み)
 * goals.js の GoalScenePlayer / buildSceneCard は defer で後から読み込まれるため、
 * ゴールシーン描画は DOMContentLoaded + goals.js 読み込み後に実行する。
 */

// ============================================================
// ユーティリティ
// ============================================================
function formatSec(sec) {
  const s = Math.max(0, Math.round(sec));
  const m = Math.floor(s / 60);
  const ss = String(s % 60).padStart(2, '0');
  return `${m}:${ss}`;
}

function clamp(v, lo, hi) { return Math.max(lo, Math.min(hi, v)); }

function makeTeamCard({ title, subtitle, yellow: y, blue: bl }) {
  const div = document.createElement('div');
  div.className = 'an-team-stat-card';
  div.innerHTML = `
    <h3>${title}${subtitle ? ` <span class="an-card-subtitle">${subtitle}</span>` : ''}</h3>
    <div class="an-team-stat-row">
      <div class="an-team-stat-item">
        <span class="an-team-val yellow">${y.val}<span class="an-team-val-unit">${y.unit}</span></span>
        <span class="an-team-sub">${y.label}</span>
      </div>
      <div class="an-team-stat-item right">
        <span class="an-team-val blue">${bl.val}<span class="an-team-val-unit">${bl.unit}</span></span>
        <span class="an-team-sub">${bl.label}</span>
      </div>
    </div>
  `;
  return div;
}

// ============================================================
// 試合統計レンダリング
// ============================================================
function renderMatchStats(stats, yName, bName) {
  const ballGrid = document.getElementById('stats-ball-grid');
  const teamGrid = document.getElementById('stats-team-grid');
  const robotTables = document.getElementById('stats-robot-tables');
  if (!ballGrid || !teamGrid || !robotTables) return;

  const b = stats.ball;
  const sprintThreshold = stats.sprint_threshold_ms ?? 2.0;

  for (const { value, unit, label } of [
    { value: b.max_speed_ms.toFixed(1), unit: 'm/s', label: 'ボール最高速度' },
    { value: b.avg_speed_ms.toFixed(1), unit: 'm/s', label: 'ボール平均速度' },
    { value: b.total_distance_m.toFixed(0), unit: 'm', label: 'ボール総移動距離' },
    { value: String(b.kick_count), unit: '回', label: 'キック検出数' },
  ]) {
    const div = document.createElement('div');
    div.className = 'an-stat-card';
    div.innerHTML = `
      <p class="an-stat-value ball">${value}<span class="an-stat-unit"> ${unit}</span></p>
      <p class="an-stat-label">${label}</p>
    `;
    ballGrid.appendChild(div);
  }

  const ys = stats.robots.yellow;
  const bs = stats.robots.blue;

  teamGrid.appendChild(makeTeamCard({
    title: 'チーム総走行距離',
    yellow: { val: ys.total_distance_m.toFixed(0), unit: ' m', label: yName },
    blue:   { val: bs.total_distance_m.toFixed(0), unit: ' m', label: bName },
  }));

  teamGrid.appendChild(makeTeamCard({
    title: 'チーム最速ロボット',
    yellow: { val: ys.fastest.max_speed_ms.toFixed(1), unit: ' m/s', label: `${yName} #${ys.fastest.id}` },
    blue:   { val: bs.fastest.max_speed_ms.toFixed(1), unit: ' m/s', label: `${bName} #${bs.fastest.id}` },
  }));

  teamGrid.appendChild(makeTeamCard({
    title: 'スプリント回数',
    subtitle: `(${sprintThreshold.toFixed(1)} m/s 超)`,
    yellow: { val: ys.total_sprint_count, unit: '', label: yName },
    blue:   { val: bs.total_sprint_count, unit: '', label: bName },
  }));

  const terr = b.territory;
  const terrDiv = document.createElement('div');
  terrDiv.className = 'an-team-stat-card';
  terrDiv.innerHTML = `
    <h3>ボール陣地分析 <span class="an-card-subtitle">(x座標の正負で判定)</span></h3>
    <div class="an-territory-bar">
      <div class="an-territory-yellow" style="width:${terr.positive_pct}%">${terr.positive_pct >= 15 ? terr.positive_pct + '%' : ''}</div>
      <div class="an-territory-blue"   style="width:${terr.negative_pct}%">${terr.negative_pct >= 15 ? terr.negative_pct + '%' : ''}</div>
    </div>
    <div class="an-territory-labels">
      <span class="an-team-sub">${yName} 陣地側 (x&gt;0)  ${terr.positive_pct}%</span>
      <span class="an-team-sub">${terr.negative_pct}%  ${bName} 陣地側 (x&le;0)</span>
    </div>
  `;
  teamGrid.appendChild(terrDiv);

  for (const [teamStats, teamName, cls] of [
    [ys, yName, 'yellow'],
    [bs, bName, 'blue'],
  ]) {
    if (teamStats.robots.length === 0) continue;
    const wrap = document.createElement('div');
    const rows = teamStats.robots.map(r => `
      <tr>
        <td><strong>#${r.id}</strong></td>
        <td class="an-tabular">${r.max_speed_ms.toFixed(1)}</td>
        <td class="an-tabular">${r.total_distance_m.toFixed(1)}</td>
        <td>${r.sprint_count}</td>
      </tr>
    `).join('');
    wrap.innerHTML = `
      <p class="an-robot-table-title" style="color:var(--an-${cls})">${teamName} ロボット別統計</p>
      <div class="an-table-wrap">
        <table class="an-table">
          <thead><tr>
            <th>ID</th><th>最高速度 (m/s)</th><th>走行距離 (m)</th><th>スプリント</th>
          </tr></thead>
          <tbody>${rows}</tbody>
        </table>
      </div>
    `;
    robotTables.appendChild(wrap);
  }
}

// ============================================================
// Canvas ベース 2D ヒートマップ描画ユーティリティ
// ============================================================

/**
 * ヒートマップ用カラー計算 (HeatmapRenderer._color 相当)
 * @param {number} norm 0-1
 * @param {'yellow'|'blue'|'hot'} scheme
 */
function _hm2dColor(norm, scheme) {
  if (norm <= 0) return null;
  const alpha = Math.min(0.95, 0.15 + norm * 0.80);
  let r, g, b;
  switch (scheme) {
    case 'yellow':
      r = (60  + norm * 195) | 0;
      g = (50  + norm * 200) | 0;
      b = (0   + norm * 60)  | 0;
      break;
    case 'blue':
      r = (0   + norm * 100) | 0;
      g = (20  + norm * 150) | 0;
      b = (80  + norm * 175) | 0;
      break;
    default:
      r = Math.min(255, (norm * 3 * 255)) | 0;
      g = Math.min(255, Math.max(0, (norm * 3 - 1) * 255)) | 0;
      b = Math.min(255, Math.max(0, (norm * 3 - 2) * 255)) | 0;
  }
  return `rgba(${r},${g},${b},${alpha.toFixed(2)})`;
}

/**
 * Canvas に汎用 2D ヒートマップを描画する。
 * @param {HTMLCanvasElement} canvas
 * @param {{
 *   data: [number,number,number][],  // [[xBin,yBin,count],...]
 *   binsX: number, binsY: number,
 *   xMin: number, xMax: number, yMin: number, yMax: number,
 *   xLabel: string, yLabel: string,
 *   colorScheme: string,
 *   overlayFn?: function,
 * }} opts
 */
function drawHeatmap2D(canvas, { data, binsX, binsY, xMin, xMax, yMin, yMax,
                                  xLabel = '', yLabel = '', colorScheme = 'hot',
                                  overlayFn = null }) {
  if (!canvas) return;
  const dpr = window.devicePixelRatio || 1;
  const W = canvas.offsetWidth  || 300;
  const H = canvas.offsetHeight || (W * 3 / 4);
  canvas.width  = W * dpr;
  canvas.height = H * dpr;
  const ctx = canvas.getContext('2d');
  ctx.scale(dpr, dpr);

  const ML = 44, MR = 8, MT = 8, MB = 32;
  const plotW = W - ML - MR;
  const plotH = H - MT - MB;

  // 背景
  ctx.fillStyle = '#12121f';
  ctx.fillRect(0, 0, W, H);
  ctx.fillStyle = '#0b0b18';
  ctx.fillRect(ML, MT, plotW, plotH);

  // セル描画
  if (data && data.length > 0) {
    let maxCount = 1;
    for (const [,, c] of data) if (c > maxCount) maxCount = c;
    const logMax = Math.log(maxCount + 1);
    const cellW = plotW / binsX;
    const cellH = plotH / binsY;
    for (const [xb, yb, count] of data) {
      const norm = Math.log(count + 1) / logMax;
      const color = _hm2dColor(norm, colorScheme);
      if (!color) continue;
      ctx.fillStyle = color;
      ctx.fillRect(ML + xb * cellW, MT + plotH - (yb + 1) * cellH, cellW, cellH);
    }
  }

  // ゼロライン
  ctx.setLineDash([4, 4]);
  ctx.strokeStyle = 'rgba(255,255,255,0.2)';
  ctx.lineWidth = 1;
  if (xMin < 0 && xMax > 0) {
    const zx = ML + (-xMin) / (xMax - xMin) * plotW;
    ctx.beginPath(); ctx.moveTo(zx, MT); ctx.lineTo(zx, MT + plotH); ctx.stroke();
  }
  if (yMin < 0 && yMax > 0) {
    const zy = MT + plotH - (-yMin) / (yMax - yMin) * plotH;
    ctx.beginPath(); ctx.moveTo(ML, zy); ctx.lineTo(ML + plotW, zy); ctx.stroke();
  }
  ctx.setLineDash([]);

  // 枠線
  ctx.strokeStyle = 'rgba(255,255,255,0.3)';
  ctx.lineWidth = 1;
  ctx.strokeRect(ML, MT, plotW, plotH);

  // フォント設定
  ctx.fillStyle = 'rgba(255,255,255,0.65)';
  ctx.font = '10px Inter, sans-serif';

  // X 軸目盛り
  ctx.textAlign = 'center';
  ctx.textBaseline = 'top';
  const xTickCount = 5;
  for (let i = 0; i <= xTickCount; i++) {
    const v = xMin + (xMax - xMin) * i / xTickCount;
    const px = ML + plotW * i / xTickCount;
    ctx.fillText(v.toFixed(1), px, MT + plotH + 4);
  }

  // Y 軸目盛り
  ctx.textAlign = 'right';
  ctx.textBaseline = 'middle';
  const yTickCount = 5;
  for (let i = 0; i <= yTickCount; i++) {
    const v = yMin + (yMax - yMin) * i / yTickCount;
    const py = MT + plotH - plotH * i / yTickCount;
    ctx.fillText(v.toFixed(1), ML - 4, py);
  }

  // 軸ラベル
  ctx.fillStyle = 'rgba(255,255,255,0.8)';
  ctx.font = '11px Inter, sans-serif';
  ctx.textAlign = 'center';
  ctx.textBaseline = 'bottom';
  ctx.fillText(xLabel, ML + plotW / 2, H - 1);

  ctx.save();
  ctx.translate(11, MT + plotH / 2);
  ctx.rotate(-Math.PI / 2);
  ctx.textBaseline = 'top';
  ctx.fillText(yLabel, 0, 0);
  ctx.restore();

  // オーバーレイ (ロボット輪郭など)
  if (overlayFn) overlayFn(ctx, ML, MT, plotW, plotH, xMin, xMax, yMin, yMax);
}

/** 加速方向ヒートマップ用オーバーレイ: 原点にロボット輪郭を描画 */
function _robotDirectionOverlay(ctx, ML, MT, plotW, plotH, xMin, xMax, yMin, yMax) {
  const toX = v => ML + (v - xMin) / (xMax - xMin) * plotW;
  const toY = v => MT + plotH - (v - yMin) / (yMax - yMin) * plotH;
  const cx = toX(0), cy = toY(0);
  const r = Math.min(plotW, plotH) * 0.055;

  ctx.strokeStyle = 'rgba(255,255,255,0.75)';
  ctx.fillStyle   = 'rgba(255,255,255,0.75)';
  ctx.lineWidth = 1.5;
  ctx.setLineDash([]);

  ctx.beginPath();
  ctx.arc(cx, cy, r, 0, Math.PI * 2);
  ctx.stroke();

  // 前方向 (+X = 右)
  const arrowTip = cx + r * 1.6;
  ctx.beginPath();
  ctx.moveTo(cx, cy);
  ctx.lineTo(arrowTip, cy);
  ctx.stroke();

  ctx.beginPath();
  ctx.moveTo(arrowTip, cy);
  ctx.lineTo(arrowTip - r * 0.4, cy - r * 0.3);
  ctx.lineTo(arrowTip - r * 0.4, cy + r * 0.3);
  ctx.closePath();
  ctx.fill();
}

// ============================================================
// ロボット動作特性分析レンダリング (TIGERs ETDP 2026 §3)
// ============================================================
function renderMotionAnalysis(motion, yName, bName) {
  const section = document.getElementById('motion-analysis-section');
  if (!section) return;
  section.style.display = '';

  const limitsGrid = document.getElementById('motion-limits-grid');
  const yl = motion.yellow.limits;
  const bl = motion.blue.limits;
  const yValid = yl.valid, bValid = bl.valid;
  const na = '–';

  if (limitsGrid) {
    limitsGrid.appendChild(makeTeamCard({
      title: '推定速度限界',
      subtitle: '(99.5パーセンタイル)',
      yellow: { val: yValid ? yl.velocity_limit.toFixed(2) : na, unit: ' m/s', label: yName },
      blue:   { val: bValid ? bl.velocity_limit.toFixed(2) : na, unit: ' m/s', label: bName },
    }));
    limitsGrid.appendChild(makeTeamCard({
      title: '推定加速度限界',
      subtitle: '(75パーセンタイル)',
      yellow: { val: yValid ? yl.accel_limit.toFixed(2) : na, unit: ' m/s²', label: yName },
      blue:   { val: bValid ? bl.accel_limit.toFixed(2) : na, unit: ' m/s²', label: bName },
    }));
    limitsGrid.appendChild(makeTeamCard({
      title: '推定減速限界',
      subtitle: '(95パーセンタイル)',
      yellow: { val: yValid ? yl.decel_limit.toFixed(2) : na, unit: ' m/s²', label: yName },
      blue:   { val: bValid ? bl.decel_limit.toFixed(2) : na, unit: ' m/s²', label: bName },
    }));
    limitsGrid.appendChild(makeTeamCard({
      title: '速度サンプル数',
      yellow: { val: yl.sample_count.toLocaleString(), unit: '', label: yName },
      blue:   { val: bl.sample_count.toLocaleString(), unit: '', label: bName },
    }));
  }

  // 速度ヒストグラム (チーム別)
  function renderSpeedHist(containerId, hist, teamName, color) {
    const el = document.getElementById(containerId);
    if (!el) return;
    const labels = hist.bins.map((_, i) => ((i + 0.5) * hist.bin_width).toFixed(1));
    new ApexCharts(el, {
      chart: { type: 'bar', height: 200, background: 'transparent', toolbar: { show: false },
               animations: { enabled: false } },
      theme: { mode: 'dark' },
      series: [{ name: teamName, data: hist.bins, color }],
      xaxis: { categories: labels, title: { text: '速度 (m/s)' }, tickAmount: 10,
               labels: { rotate: 0 } },
      yaxis: { title: { text: '頻度' }, labels: { formatter: v => v.toLocaleString() } },
      plotOptions: { bar: { columnWidth: '95%' } },
      dataLabels: { enabled: false },
      grid: { borderColor: 'rgba(255,255,255,0.08)' },
      legend: { show: false },
    }).render();
  }

  function setTeamTitles(yId, bId) {
    const yEl = document.getElementById(yId);
    const bEl = document.getElementById(bId);
    if (yEl) yEl.textContent = yName;
    if (bEl) bEl.textContent = bName;
  }

  setTeamTitles('speed-hist-yellow-title', 'speed-hist-blue-title');
  renderSpeedHist('speed-hist-yellow', motion.yellow.speed_histogram, yName, '#FDD663');
  renderSpeedHist('speed-hist-blue',   motion.blue.speed_histogram,   bName, '#5B9BF5');

  // 速度-加速度ヒートマップ (X=加速度, Y=速度)
  function renderSAHeatmap(canvasId, teamMotion, colorScheme) {
    const canvas = document.getElementById(canvasId);
    if (!canvas) return;
    const hm = teamMotion.speed_accel_heatmap;
    const accelBins = Math.round((hm.accel_max - hm.accel_min) / hm.accel_bin_width);
    const speedBins = Math.round(hm.speed_max / hm.speed_bin_width);
    // データを軸入れ替え: [[speed_bin, accel_bin, cnt]] → [[accel_bin, speed_bin, cnt]]
    const swapped = hm.data.map(([sb, ab, cnt]) => [ab, sb, cnt]);
    drawHeatmap2D(canvas, {
      data: swapped,
      binsX: accelBins, binsY: speedBins,
      xMin: hm.accel_min, xMax: hm.accel_max,
      yMin: 0,            yMax: hm.speed_max,
      xLabel: '加速度 (m/s²)', yLabel: '速度 (m/s)',
      colorScheme,
    });
  }

  setTeamTitles('sa-heatmap-yellow-title', 'sa-heatmap-blue-title');

  // 加速方向ヒートマップ (ロボットローカル座標)
  function renderDAHeatmap(canvasId, teamMotion, colorScheme) {
    const canvas = document.getElementById(canvasId);
    if (!canvas || !teamMotion.directional_accel) return;
    const da = teamMotion.directional_accel;
    const bins = Math.round((da.accel_max - da.accel_min) / da.bin_width);
    drawHeatmap2D(canvas, {
      data: da.data,
      binsX: bins, binsY: bins,
      xMin: da.accel_min, xMax: da.accel_max,
      yMin: da.accel_min, yMax: da.accel_max,
      xLabel: '前後方向加速度 (m/s²)', yLabel: '左右方向加速度 (m/s²)',
      colorScheme,
      overlayFn: _robotDirectionOverlay,
    });
  }

  setTeamTitles('da-heatmap-yellow-title', 'da-heatmap-blue-title');

  // ResizeObserver でサイズ確定後に Canvas を描画
  function drawAllCanvases() {
    renderSAHeatmap('sa-heatmap-yellow', motion.yellow, 'yellow');
    renderSAHeatmap('sa-heatmap-blue',   motion.blue,   'blue');
    renderDAHeatmap('da-heatmap-yellow', motion.yellow, 'yellow');
    renderDAHeatmap('da-heatmap-blue',   motion.blue,   'blue');
  }

  if (typeof ResizeObserver !== 'undefined') {
    const ro = new ResizeObserver(drawAllCanvases);
    ['sa-heatmap-yellow','sa-heatmap-blue','da-heatmap-yellow','da-heatmap-blue']
      .forEach(id => { const el = document.getElementById(id); if (el) ro.observe(el); });
  } else {
    setTimeout(drawAllCanvases, 100);
  }
}

// ============================================================
// FullReplayPlayer — JSON フレームを再生する
// ============================================================
class FullReplayPlayer {
  constructor({ frames, fps, robotElements, ballEl, scoreTimeline, refereeCommands }) {
    this.frames = frames;
    this.fps = fps;
    this.interval = 1000 / fps;
    this.robotElements = robotElements;
    this.ballEl = ballEl;
    this.scoreTimeline = scoreTimeline;     // [{t_sec, yellow, blue}, ...]
    this.refereeCommands = refereeCommands; // [{t_sec, stage, command}, ...]
    this.currentIdx = 0;
    this.playing = false;
    this.speed = 1.0;
    this.elapsed = 0;
    this.lastTime = null;
    this._rafId = null;
    this._boundLoop = this._loop.bind(this);

    // コールバック
    this.onFrame = null;  // (frame, idx) => void
  }

  get totalFrames() { return this.frames.length; }

  get currentFrame() { return this.frames[this.currentIdx] || null; }

  get totalSec() {
    const last = this.frames[this.totalFrames - 1];
    return last ? last.t_sec : 0;
  }

  seek(idx) {
    this.currentIdx = clamp(idx, 0, this.totalFrames - 1);
    this._render();
  }

  play() {
    if (this.playing || this.totalFrames === 0) return;
    if (this.currentIdx >= this.totalFrames - 1) this.currentIdx = 0;
    this.playing = true;
    this.elapsed = 0;
    this.lastTime = null;
    this._rafId = requestAnimationFrame(this._boundLoop);
  }

  pause() {
    this.playing = false;
    if (this._rafId) { cancelAnimationFrame(this._rafId); this._rafId = null; }
  }

  setSpeed(s) { this.speed = s; }

  _loop(now) {
    if (!this.playing) return;
    if (this.lastTime !== null) {
      this.elapsed += (now - this.lastTime) * this.speed;
      while (this.elapsed >= this.interval) {
        this.elapsed -= this.interval;
        this.currentIdx++;
        if (this.currentIdx >= this.totalFrames) {
          this.currentIdx = this.totalFrames - 1;
          this.playing = false;
          this._render();
          if (this.onStop) this.onStop();
          return;
        }
      }
    }
    this.lastTime = now;
    this._render();
    this._rafId = requestAnimationFrame(this._boundLoop);
  }

  _render() {
    const frame = this.frames[this.currentIdx];
    if (!frame) return;
    updateFrame(frame, this.robotElements, this.ballEl);
    if (this.onFrame) this.onFrame(frame, this.currentIdx);
  }

  /** t_sec 以下の最後の要素を二分探索で返す。見つからなければ fallback を返す。*/
  static _bisect(arr, t_sec, fallback) {
    if (arr.length === 0 || arr[0].t_sec > t_sec) return fallback;
    let lo = 0, hi = arr.length - 1;
    while (lo < hi) {
      const mid = (lo + hi + 1) >> 1;
      if (arr[mid].t_sec <= t_sec) lo = mid; else hi = mid - 1;
    }
    return arr[lo];
  }

  /** t_sec に対応するスコアを返す */
  scoreAt(t_sec) {
    return FullReplayPlayer._bisect(this.scoreTimeline, t_sec, { yellow: 0, blue: 0 });
  }

  /** t_sec に対応するレフェリーコマンドを返す */
  commandAt(t_sec) {
    return FullReplayPlayer._bisect(this.refereeCommands, t_sec, { stage: '–', command: '–' });
  }
}

// ============================================================
// イベントカテゴリ判定
// ============================================================
const EVENT_GOAL_TYPES  = new Set([8, 39, 42, 43]);
const EVENT_FOUL_TYPES  = new Set([13,14,15,17,18,19,20,21,22,24,26,27,28,29,31,32,34,35,36,47,48]);
const EVENT_BALL_TYPES  = new Set([6,7,11,41]);

function eventCategory(typeVal) {
  if (EVENT_GOAL_TYPES.has(typeVal)) return 'goal';
  if (EVENT_FOUL_TYPES.has(typeVal)) return 'foul';
  if (EVENT_BALL_TYPES.has(typeVal)) return 'ball';
  return 'info';
}

// ============================================================
// メイン処理
// ============================================================
(async () => {
  const params = new URLSearchParams(location.search);
  const matchId = params.get('id');

  if (!matchId) {
    document.getElementById('loading-msg').textContent = 'URLパラメータ ?id= が指定されていません。';
    return;
  }

  // データ読み込み
  let data;
  try {
    const res = await fetch(`./analysis-data/${encodeURIComponent(matchId)}.json`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    data = await res.json();
  } catch (e) {
    document.getElementById('loading-msg').textContent = `データの読み込みに失敗しました: ${e.message}`;
    return;
  }

  // ローディング非表示、コンテンツ表示
  document.getElementById('loading-overlay').style.display = 'none';
  document.getElementById('main-content').style.display = 'block';

  const { meta, match_stats, motion_analysis, replay_frames, ball_heatmap, robot_heatmaps,
          goal_scenes, events, possession, score_timeline, referee_commands } = data;

  // ============================================================
  // ① スコアボード & ヘッダー
  // ============================================================
  const yName = meta.teams.yellow || 'Yellow';
  const bName = meta.teams.blue   || 'Blue';
  document.title = `${yName} vs ${bName} – 試合分析`;
  document.getElementById('an-title')    && (document.getElementById('an-title').textContent    = `${yName} vs ${bName}`);
  document.getElementById('match-title').textContent   = `${yName} vs ${bName}`;
  document.getElementById('match-subtitle').textContent = `${meta.filename}  |  ${formatSec(meta.duration_sec)}`;

  document.getElementById('sb-yellow-name').textContent  = yName;
  document.getElementById('sb-blue-name').textContent    = bName;
  document.getElementById('sb-yellow-score').textContent = String(meta.final_score.yellow);
  document.getElementById('sb-blue-score').textContent   = String(meta.final_score.blue);
  document.getElementById('sb-duration').textContent     = formatSec(meta.duration_sec);

  // ヒートマップタイトル & フィルタボタンにチーム名を反映
  const heatYellowTitle = document.getElementById('heatmap-yellow-title');
  const heatBlueTitle   = document.getElementById('heatmap-blue-title');
  if (heatYellowTitle) heatYellowTitle.textContent = `${yName} チーム`;
  if (heatBlueTitle)   heatBlueTitle.textContent   = `${bName} チーム`;

  const filterBtnYellow = document.getElementById('filter-btn-yellow');
  const filterBtnBlue   = document.getElementById('filter-btn-blue');
  if (filterBtnYellow) filterBtnYellow.textContent = yName;
  if (filterBtnBlue)   filterBtnBlue.textContent   = bName;

  if (meta.gdrive_url) {
    const dlLink = document.getElementById('log-download-link');
    dlLink.href = meta.gdrive_url;
    dlLink.style.display = '';
  }

  // ============================================================
  // ② 試合統計
  // ============================================================
  if (match_stats) renderMatchStats(match_stats, yName, bName);
  if (motion_analysis) renderMotionAnalysis(motion_analysis, yName, bName);

  // ============================================================
  // ③ フルリプレイ
  // ============================================================
  const { svg, robotElements, ballEl, eventOverlay } = buildFieldSVG();
  document.getElementById('replay-field-container').appendChild(svg);
  new SvgZoomPan(svg);

  const player = new FullReplayPlayer({
    frames: replay_frames,
    fps: 3,
    robotElements,
    ballEl,
    scoreTimeline: score_timeline || [],
    refereeCommands: referee_commands || [],
  });

  // UI 要素
  const btnPlay      = document.getElementById('rp-play');
  const btnStepBack  = document.getElementById('rp-step-back');
  const btnStepFwd   = document.getElementById('rp-step-fwd');
  const rateSelect   = document.getElementById('rp-rate-select');
  const timelineTrack = document.getElementById('rp-timeline-track');
  const timelineProg  = document.getElementById('rp-timeline-progress');
  const timelineThumb = document.getElementById('rp-timeline-thumb');
  const timeCurrent   = document.getElementById('rp-time-current');
  const timeTotal     = document.getElementById('rp-time-total');
  const scoreYellow   = document.getElementById('replay-score-yellow');
  const scoreBlue     = document.getElementById('replay-score-blue');
  const stageEl       = document.getElementById('replay-stage');
  const commandEl     = document.getElementById('replay-command');

  timeTotal.textContent = formatSec(player.totalSec);

  function updateReplayUI(frame, idx) {
    const frac = player.totalFrames > 1 ? idx / (player.totalFrames - 1) : 0;
    const pct = (frac * 100).toFixed(2) + '%';
    timelineProg.style.width = pct;
    timelineThumb.style.left = pct;
    timeCurrent.textContent = formatSec(frame.t_sec);

    const score = player.scoreAt(frame.t_sec);
    scoreYellow.textContent = String(score.yellow);
    scoreBlue.textContent   = String(score.blue);

    const cmd = player.commandAt(frame.t_sec);
    stageEl.textContent   = cmd.stage;
    commandEl.textContent = cmd.command || '–';
  }

  function resetPlayIcon() {
    btnPlay.querySelector('#rp-play-icon').style.display  = '';
    btnPlay.querySelector('#rp-pause-icon').style.display = 'none';
  }

  player.onFrame = updateReplayUI;
  player.onStop  = resetPlayIcon;

  if (replay_frames.length > 0) player.seek(0);

  // 再生ボタン
  btnPlay.addEventListener('click', () => {
    if (player.playing) {
      player.pause();
      resetPlayIcon();
    } else {
      player.play();
      btnPlay.querySelector('#rp-play-icon').style.display  = 'none';
      btnPlay.querySelector('#rp-pause-icon').style.display = '';
    }
  });

  btnStepBack.addEventListener('click', () => { player.pause(); player.seek(player.currentIdx - 1); resetPlayIcon(); });
  btnStepFwd.addEventListener('click',  () => { player.pause(); player.seek(player.currentIdx + 1); resetPlayIcon(); });
  rateSelect.addEventListener('change', () => player.setSpeed(parseFloat(rateSelect.value)));

  // タイムラインシーク
  function seekFromPointer(e) {
    const rect = timelineTrack.getBoundingClientRect();
    const frac = clamp((e.clientX - rect.left) / rect.width, 0, 1);
    const idx  = Math.round(frac * (player.totalFrames - 1));
    player.seek(idx);
  }
  let dragging = false;
  timelineTrack.addEventListener('pointerdown', (e) => { dragging = true; timelineTrack.setPointerCapture(e.pointerId); seekFromPointer(e); });
  timelineTrack.addEventListener('pointermove', (e) => { if (dragging) seekFromPointer(e); });
  timelineTrack.addEventListener('pointerup',   ()  => { dragging = false; });

  // キーボードショートカット
  document.addEventListener('keydown', (e) => {
    if (['INPUT','SELECT','TEXTAREA'].includes(e.target.tagName)) return;
    if (e.key === ' ') { e.preventDefault(); btnPlay.click(); }
    else if (e.key === 'ArrowLeft')  { player.pause(); player.seek(player.currentIdx - 1); }
    else if (e.key === 'ArrowRight') { player.pause(); player.seek(player.currentIdx + 1); }
    else if (e.key === 'Home') { player.pause(); player.seek(0); }
    else if (e.key === 'End')  { player.pause(); player.seek(player.totalFrames - 1); }
  });

  // ============================================================
  // ④ スコアタイムライン (ApexCharts)
  // ============================================================
  if (score_timeline && score_timeline.length > 0) {
    const categories = score_timeline.map(s => formatSec(s.t_sec));
    new ApexCharts(document.getElementById('score-chart'), {
      chart: { type: 'line', height: 220, background: 'transparent', toolbar: { show: false },
               animations: { enabled: false } },
      theme: { mode: 'dark' },
      series: [
        { name: yName, data: score_timeline.map(s => s.yellow), color: '#FDD663' },
        { name: bName, data: score_timeline.map(s => s.blue),   color: '#5B9BF5' },
      ],
      xaxis: { categories, labels: { show: score_timeline.length < 60 }, title: { text: '時刻' } },
      yaxis: { labels: { formatter: v => String(Math.round(v)) }, min: 0,
               title: { text: 'スコア' }, tickAmount: Math.max(meta.final_score.yellow, meta.final_score.blue) + 1 },
      stroke: { curve: 'stepline', width: 2 },
      markers: { size: 4 },
      grid: { borderColor: 'rgba(255,255,255,0.08)' },
      tooltip: { x: { formatter: v => v } },
    }).render();
  }

  // ============================================================
  // ⑤ ポゼッション (ApexCharts)
  // ============================================================
  if (possession && possession.timestamps && possession.timestamps.length > 0) {
    const yellowRatio = possession.yellow_ratio.map(r => Math.round(r * 100));
    const blueRatio   = yellowRatio.map(r => 100 - r);
    new ApexCharts(document.getElementById('possession-chart'), {
      chart: { type: 'area', height: 220, background: 'transparent', toolbar: { show: false },
               animations: { enabled: false }, stacked: true },
      theme: { mode: 'dark' },
      series: [
        { name: yName, data: yellowRatio, color: '#FDD663' },
        { name: bName, data: blueRatio,   color: '#5B9BF5' },
      ],
      xaxis: {
        categories: possession.timestamps.map(t => formatSec(t)),
        tickAmount: Math.min(12, possession.timestamps.length),
        labels: { rotate: -30 },
        title: { text: '時刻' },
      },
      yaxis: { max: 100, labels: { formatter: v => `${v}%` }, title: { text: 'ポゼッション率' } },
      fill: { opacity: 0.7 },
      stroke: { width: 1 },
      grid: { borderColor: 'rgba(255,255,255,0.08)' },
      tooltip: { y: { formatter: v => `${v}%` } },
      dataLabels: { enabled: false },
    }).render();
  }

  // ============================================================
  // ⑥ ヒートマップ
  // ============================================================
  function initCanvas(id) {
    const canvas = document.getElementById(id);
    if (!canvas) return null;
    // デバイスピクセル比に対応
    const dpr = window.devicePixelRatio || 1;
    const w = canvas.offsetWidth  || 400;
    const h = canvas.offsetHeight || (w * 3 / 4);
    canvas.width  = w * dpr;
    canvas.height = h * dpr;
    canvas.getContext('2d').scale(dpr, dpr);
    return canvas;
  }

  // ResizeObserver でサイズ確定後に描画
  const drawHeatmaps = () => {
    const ballCanvas   = initCanvas('heatmap-ball');
    const yellowCanvas = initCanvas('heatmap-yellow');
    const blueCanvas   = initCanvas('heatmap-blue');

    if (ballCanvas && ball_heatmap)
      new HeatmapRenderer(ballCanvas).render(ball_heatmap, 'hot');
    if (yellowCanvas && robot_heatmaps?.yellow)
      new HeatmapRenderer(yellowCanvas).render(robot_heatmaps.yellow, 'yellow');
    if (blueCanvas && robot_heatmaps?.blue)
      new HeatmapRenderer(blueCanvas).render(robot_heatmaps.blue, 'blue');
  };

  if (typeof ResizeObserver !== 'undefined') {
    const ro = new ResizeObserver(drawHeatmaps);
    ['heatmap-ball','heatmap-yellow','heatmap-blue'].forEach(id => {
      const el = document.getElementById(id);
      if (el) ro.observe(el);
    });
  } else {
    setTimeout(drawHeatmaps, 100);
  }

  // ============================================================
  // ⑦ ゴールシーン集
  // ============================================================
  function renderGoalScenes() {
    const container = document.getElementById('goal-scenes-container');
    if (!container) return;
    if (!goal_scenes || goal_scenes.length === 0) {
      container.innerHTML = '<p style="color:var(--an-on-variant)">ゴールシーンデータがありません。</p>';
      return;
    }
    for (const scene of goal_scenes) {
      container.appendChild(buildSceneCard(
        { ...scene, date: '' },
        { yellow: yName, blue: bName },
      ));
    }
  }

  // goals.js の GoalScenePlayer が読み込まれるまで待つ
  if (typeof GoalScenePlayer !== 'undefined') {
    renderGoalScenes();
  } else {
    window.addEventListener('load', renderGoalScenes);
  }

  // ============================================================
  // ⑧ イベントテーブル
  // ============================================================
  const eventsData = events || [];
  let currentEventFilter = 'all';

  function renderEvents(filter) {
    const tbody = document.getElementById('events-tbody');
    if (!tbody) return;
    tbody.innerHTML = '';

    const filtered = filter === 'all'    ? eventsData :
                     filter === 'yellow' ? eventsData.filter(e => e.by_team === 'yellow') :
                     filter === 'blue'   ? eventsData.filter(e => e.by_team === 'blue') :
                     eventsData.filter(e => eventCategory(e.type) === filter);

    for (const ev of filtered) {
      const cat = eventCategory(ev.type);
      const tr = document.createElement('tr');
      const locStr = ev.location ? `(${ev.location.x}, ${ev.location.y})` : '–';
      const teamChip = ev.by_team
        ? `<span class="an-team-chip ${ev.by_team}"></span>${ev.by_team === 'yellow' ? yName : bName}`
        : '–';
      tr.innerHTML = `
        <td style="font-family:'JetBrains Mono',monospace;font-size:0.8rem">${ev.t_sec.toFixed(1)}</td>
        <td><span class="an-event-badge ${cat}">${ev.label}</span></td>
        <td>${teamChip}</td>
        <td>${ev.by_bot != null ? `#${ev.by_bot}` : '–'}</td>
        <td style="font-size:0.78rem;color:var(--an-on-variant)">${locStr}</td>
      `;
      tbody.appendChild(tr);
    }

    if (filtered.length === 0) {
      const tr = document.createElement('tr');
      tr.innerHTML = '<td colspan="5" style="text-align:center;color:var(--an-on-variant)">該当するイベントがありません</td>';
      tbody.appendChild(tr);
    }
  }

  renderEvents('all');

  document.getElementById('event-filter-bar')?.addEventListener('click', (e) => {
    const btn = e.target.closest('.an-filter-btn');
    if (!btn) return;
    document.querySelectorAll('.an-filter-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    currentEventFilter = btn.dataset.filter;
    renderEvents(currentEventFilter);
  });

  // ============================================================
  // ⑨ レフェリーコマンドテーブル
  // ============================================================
  const cmdTbody = document.getElementById('commands-tbody');
  if (cmdTbody && referee_commands) {
    for (const c of referee_commands) {
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td style="font-family:'JetBrains Mono',monospace;font-size:0.8rem">${c.t_sec.toFixed(1)}</td>
        <td>${c.stage}</td>
        <td>${c.command || '–'}</td>
      `;
      cmdTbody.appendChild(tr);
    }
    if (referee_commands.length === 0) {
      const tr = document.createElement('tr');
      tr.innerHTML = '<td colspan="3" style="text-align:center;color:var(--an-on-variant)">データなし</td>';
      cmdTbody.appendChild(tr);
    }
  }

})();
