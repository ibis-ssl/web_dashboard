/**
 * SSL ゲームログプレイヤー
 * ssl-field.js, ssl-log-parser.js が先に読み込まれていること
 */

// Referee ステージ名マッピング
const STAGE_NAMES = {
  0:  'プレゲーム',
  1:  '前半',
  2:  'ハーフタイム',
  3:  '後半プレゲーム',
  4:  '後半',
  5:  'オーバータイム休憩',
  6:  'オーバータイム前半プレゲーム',
  7:  'オーバータイム前半',
  8:  'オーバータイムハーフタイム',
  9:  'オーバータイム後半プレゲーム',
  10: 'オーバータイム後半',
  11: 'PK戦休憩',
  12: 'PK戦',
  13: '試合終了',
};

// Referee コマンド名マッピング
const COMMAND_NAMES = {
  0:  'HALT',
  1:  '停止 (STOP)',
  2:  '通常プレー開始',
  3:  'フォースプレー開始',
  4:  'キックオフ準備 (Yellow)',
  5:  'キックオフ準備 (Blue)',
  6:  'ペナルティキック準備 (Yellow)',
  7:  'ペナルティキック準備 (Blue)',
  8:  'フリーキック (Yellow)',
  9:  'フリーキック (Blue)',
  10: 'インダイレクトFK (Yellow)',
  11: 'インダイレクトFK (Blue)',
  12: 'タイムアウト (Yellow)',
  13: 'タイムアウト (Blue)',
  14: 'ゴール (Yellow)',
  15: 'ボールプレースメント (Yellow)',
  16: 'ボールプレースメント (Blue)',
};

// ゲームイベント情報マッピング (GameEvent.Type enum 値 → ラベル・アイコン・カテゴリ)
const GAME_EVENT_INFO = {
  // ボールアウト
  6:  { label: 'ボールアウト(タッチ)', icon: '↗️', category: 'ball' },
  7:  { label: 'ボールアウト(ゴール)', icon: '↗️', category: 'ball' },
  11: { label: 'エイムレスキック',      icon: '↗️', category: 'ball' },
  // 試合停止ファール
  13: { label: 'キーパー保持',     icon: '🛑', category: 'foul' },
  14: { label: 'ダブルタッチ',     icon: '🛑', category: 'foul' },
  17: { label: 'オーバードリブル', icon: '🛑', category: 'foul' },
  19: { label: 'エリア接近',       icon: '🛑', category: 'foul' },
  24: { label: 'プッシング',       icon: '🛑', category: 'foul' },
  26: { label: 'ホールディング',   icon: '🛑', category: 'foul' },
  27: { label: '転倒',             icon: '🛑', category: 'foul' },
  31: { label: 'マルチプルDF',     icon: '🛑', category: 'foul' },
  43: { label: '境界線横断',       icon: '🛑', category: 'foul' },
  51: { label: '部品脱落',         icon: '🛑', category: 'foul' },
  // 軽微なファール
  15: { label: 'エリア内タッチ',     icon: '⚠️', category: 'foul' },
  18: { label: 'ボール速度超過',     icon: '⚠️', category: 'foul' },
  21: { label: '衝突(引分)',         icon: '⚠️', category: 'foul' },
  22: { label: '衝突',              icon: '⚠️', category: 'foul' },
  20: { label: '配置妨害',          icon: '⚠️', category: 'foul' },
  28: { label: 'STOP中速度超過',    icon: '⚠️', category: 'foul' },
  29: { label: 'ボール接近',        icon: '⚠️', category: 'foul' },
  52: { label: '交代回数超過',      icon: '⚠️', category: 'foul' },
  // ゴール関連
  39: { label: 'ゴール(確認中)', icon: '⚽', category: 'goal' },
  8:  { label: 'ゴール',         icon: '⚽', category: 'goal' },
  44: { label: '無効ゴール',     icon: '❌', category: 'goal' },
  45: { label: 'PK失敗',         icon: '❌', category: 'goal' },
  // その他
  2:  { label: '試合の停滞', icon: '⏸️', category: 'info' },
  38: { label: 'ロボット数超過', icon: '🤖', category: 'info' },
  3:  { label: '配置失敗',    icon: '⚠️', category: 'foul' },
  5:  { label: '配置成功',    icon: '✅', category: 'info' },
};

function formatTimeNs(ns) {
  const totalMs = Number(ns) / 1e6;
  const totalSec = totalMs / 1000;
  const min = Math.floor(totalSec / 60);
  const sec = (totalSec % 60).toFixed(1).padStart(4, '0');
  return `${min}:${sec}`;
}

function findFrameIndex(frames, targetNs) {
  if (frames.length === 0) return 0;
  let lo = 0, hi = frames.length - 1;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (frames[mid].timestampNs < targetNs) lo = mid + 1;
    else hi = mid;
  }
  if (lo > 0) {
    const diffPrev = targetNs - frames[lo - 1].timestampNs;
    const diffCurr = frames[lo].timestampNs - targetNs;
    if (diffPrev < diffCurr) return lo - 1;
  }
  return lo;
}

function findRefereeIndex(snapshots, targetNs) {
  if (snapshots.length === 0) return -1;
  let lo = 0, hi = snapshots.length - 1;
  while (lo < hi) {
    const mid = (lo + hi + 1) >> 1;
    if (snapshots[mid].timestampNs <= targetNs) lo = mid;
    else hi = mid - 1;
  }
  return snapshots[lo].timestampNs <= targetNs ? lo : -1;
}

// ゲームイベントの補足情報テキストを生成
function getEventDetails(eventData, fieldName) {
  if (!eventData) return null;
  if (eventData.initialBallSpeed != null) return `${eventData.initialBallSpeed.toFixed(1)}m/s`;
  if (eventData.speed != null) return `${eventData.speed.toFixed(1)}m/s`;
  if (eventData.crashSpeed != null) return `${eventData.crashSpeed.toFixed(1)}m/s`;
  if (eventData.pushedDistance != null) return `${eventData.pushedDistance.toFixed(2)}m`;
  if (eventData.timeTaken != null) {
    const prec = eventData.precision != null ? ` 精度${eventData.precision.toFixed(3)}m` : '';
    return `${eventData.timeTaken.toFixed(1)}秒${prec}`;
  }
  if (eventData.distance != null) return `${eventData.distance.toFixed(2)}m`;
  if (eventData.maxBallHeight != null) return `高さ${eventData.maxBallHeight.toFixed(2)}m`;
  if (eventData.numRobotsOnField != null) return `${eventData.numRobotsOnField}/${eventData.numRobotsAllowed ?? '?'}台`;
  return null;
}

// ============================================================
// トースト通知マネージャー
// ============================================================
class ToastManager {
  constructor(containerId) {
    this._container = document.getElementById(containerId);
    this._maxVisible = 5;
  }

  show(title, message = null, level = 'info', duration = 4000) {
    if (!this._container) return;

    // 最大件数超過時は古いものを削除
    const existing = this._container.querySelectorAll('.toast-item');
    if (existing.length >= this._maxVisible) {
      existing[existing.length - 1].remove();
    }

    const item = document.createElement('div');
    item.className = `toast-item toast-${level}`;

    const titleEl = document.createElement('div');
    titleEl.className = 'toast-title';
    titleEl.textContent = title;
    item.appendChild(titleEl);

    if (message) {
      const msgEl = document.createElement('div');
      msgEl.className = 'toast-message';
      msgEl.textContent = message;
      item.appendChild(msgEl);
    }

    // 閉じるボタン
    const closeBtn = document.createElement('button');
    closeBtn.className = 'toast-close';
    closeBtn.textContent = '×';
    closeBtn.addEventListener('click', () => this._dismiss(item));
    item.appendChild(closeBtn);

    this._container.prepend(item);

    // auto dismiss
    if (duration > 0) {
      setTimeout(() => this._dismiss(item), duration);
    }
  }

  _dismiss(item) {
    if (!item.parentNode) return;
    item.classList.add('toast-removing');
    item.addEventListener('animationend', () => item.remove(), { once: true });
  }
}

// ============================================================
// LogPlayer
// ============================================================
class LogPlayer {
  constructor() {
    this.frames = [];
    this.visionFrames = [];
    this.trackerFrames = [];
    this.dataSource = 'tracker';
    this.refereeSnapshots = [];
    this.goalMarkers = [];
    this.gameEvents = [];
    this.teamNames = { yellow: 'Yellow', blue: 'Blue' };
    this.durationNs = BigInt(0);
    this.startNs = BigInt(0);

    this.currentFrameIdx = 0;
    this.playing = false;
    this.speed = 1.0;
    this._rafId = null;
    this._lastTime = null;
    this._elapsedNs = 0;

    this.robotElements = null;
    this.ballEl = null;
    this.eventOverlay = null;
    this._zoomPan = null;
    this._boundLoop = this._loop.bind(this);

    this._lastCommandCounter = -1;
    this._shownEventIds = new Set();

    // シークスロットル用
    this._seekThrottleTimer = null;
    this._pendingSeekNs = null;
    this._isDragging = false;

    this._toast = new ToastManager('toast-container');

    this._buildUI();
    this._setupDropZone();
    this._setupKeyboard();
  }

  get hasTracker() { return this.trackerFrames.length > 0; }
  get hasVision()  { return this.visionFrames.length > 0; }

  // --- UI 構築 ---

  _buildUI() {
    this._dropZone    = document.getElementById('drop-zone');
    this._playerBody  = document.getElementById('player-body');
    this._fieldCont   = document.getElementById('field-container');
    this._parseProgress = document.getElementById('parse-progress');
    this._parseBar    = document.getElementById('parse-progress-bar');

    this._btnPlay     = document.getElementById('btn-play');
    this._playIcon    = document.getElementById('play-icon');
    this._pauseIcon   = document.getElementById('pause-icon');
    this._btnStepBack = document.getElementById('btn-step-back');
    this._btnStepFwd  = document.getElementById('btn-step-fwd');
    this._btnChange   = document.getElementById('btn-change-file');
    this._rateSelect  = document.getElementById('rate-select');
    this._replayBar   = document.getElementById('replay-bar');
    this._replaySpinner = document.getElementById('replay-spinner');
    this._replayLoadingText = document.getElementById('replay-loading-text');
    this._replayFilename = document.getElementById('replay-filename');

    this._timeline    = document.getElementById('timeline-track');
    this._tlProgress  = document.getElementById('timeline-progress');
    this._tlThumb     = document.getElementById('timeline-thumb');
    this._timeCurrent = document.getElementById('time-current');
    this._timeTotal   = document.getElementById('time-total');

    this._scoreYellow = document.getElementById('score-yellow');
    this._scoreBlue   = document.getElementById('score-blue');
    this._teamNameYellow = document.getElementById('team-name-yellow');
    this._teamNameBlue   = document.getElementById('team-name-blue');
    this._refStage    = document.getElementById('ref-stage');
    this._refCommand  = document.getElementById('ref-command');
    this._goalLog     = document.getElementById('goal-log-list');
    this._gameEventLog = document.getElementById('game-event-log-list');

    // データソース切り替えボタン
    this._dataSourceWrap = document.getElementById('data-source-wrap');
    this._dataSourceBtns = document.querySelectorAll('.data-source-btn');
    this._dataSourceBtns.forEach(btn => {
      btn.addEventListener('click', () => this.switchDataSource(btn.dataset.source));
    });

    // ボタンイベント
    this._btnPlay.addEventListener('click', () => this.togglePlay());
    this._btnStepBack.addEventListener('click', () => this.stepFrames(-1));
    this._btnStepFwd.addEventListener('click', () => this.stepFrames(1));
    this._btnChange.addEventListener('click', () => this._showDropZone());
    this._rateSelect.addEventListener('change', e => this.setSpeed(parseFloat(e.target.value)));

    // タイムラインドラッグ
    this._timeline.addEventListener('pointerdown', e => {
      this._isDragging = true;
      this._timeline.setPointerCapture(e.pointerId);
      this._seekFromPointer(e);
    });
    this._timeline.addEventListener('pointermove', e => {
      if (!this._isDragging) return;
      this._seekFromPointer(e);
    });
    this._timeline.addEventListener('pointerup', e => {
      this._isDragging = false;
      this._timeline.releasePointerCapture(e.pointerId);
      this._flushPendingSeek();
    });
    this._timeline.addEventListener('pointercancel', e => {
      this._isDragging = false;
      this._flushPendingSeek();
    });
  }

  _seekFromPointer(e) {
    const rect = this._timeline.getBoundingClientRect();
    const ratio = Math.min(1, Math.max(0, (e.clientX - rect.left) / rect.width));

    // Optimistic UI: 視覚位置を即座に更新
    const pct = ratio * 100;
    this._tlProgress.style.width = `${pct}%`;
    this._tlThumb.style.left = `${pct}%`;
    this._timeCurrent.textContent = formatTimeNs(BigInt(Math.round(Number(this.durationNs) * ratio)));

    const targetNs = this.startNs + BigInt(Math.round(Number(this.durationNs) * ratio));
    this._throttledSeek(targetNs);
  }

  _throttledSeek(targetNs) {
    this._pendingSeekNs = targetNs;
    if (this._seekThrottleTimer !== null) return;
    this._seekToNs(targetNs);
    this._seekThrottleTimer = setTimeout(() => {
      this._seekThrottleTimer = null;
      if (this._pendingSeekNs !== null) {
        this._seekToNs(this._pendingSeekNs);
        this._pendingSeekNs = null;
      }
    }, 50);
  }

  _flushPendingSeek() {
    if (this._seekThrottleTimer !== null) {
      clearTimeout(this._seekThrottleTimer);
      this._seekThrottleTimer = null;
    }
    if (this._pendingSeekNs !== null) {
      this._seekToNs(this._pendingSeekNs);
      this._pendingSeekNs = null;
    }
  }

  // --- データソース切り替え ---

  switchDataSource(source) {
    if (source === this.dataSource) return;
    if (source === 'tracker' && this.trackerFrames.length === 0) return;
    if (source === 'vision'  && this.visionFrames.length === 0) return;

    const currentNs = this.frames.length > 0
      ? this.frames[this.currentFrameIdx].timestampNs
      : this.startNs;
    const wasPlaying = this.playing;
    if (wasPlaying) this.pause();

    this.dataSource = source;
    this.frames = source === 'tracker' ? this.trackerFrames : this.visionFrames;
    this.startNs = this.frames.length > 0 ? this.frames[0].timestampNs : BigInt(0);
    this.durationNs = this.frames.length > 1
      ? this.frames[this.frames.length - 1].timestampNs - this.frames[0].timestampNs
      : BigInt(0);

    this._timeTotal.textContent = formatTimeNs(this.durationNs);
    this._buildGoalMarkers();
    this._buildEventMarkers();

    this._shownEventIds.clear();
    this._updateDataSourceUI();

    this._seekToFrameIdx(findFrameIndex(this.frames, currentNs));
    if (wasPlaying) this.play();
  }

  _updateDataSourceUI() {
    this._dataSourceBtns.forEach(btn => {
      btn.classList.toggle('active', btn.dataset.source === this.dataSource);
      btn.disabled = btn.dataset.source === 'tracker' ? !this.hasTracker : !this.hasVision;
    });
  }

  _setupDropZone() {
    const zone = this._dropZone;
    const input = document.getElementById('file-input');

    zone.addEventListener('dragover', e => {
      e.preventDefault();
      zone.classList.add('drag-over');
    });
    zone.addEventListener('dragleave', () => zone.classList.remove('drag-over'));
    zone.addEventListener('drop', e => {
      e.preventDefault();
      zone.classList.remove('drag-over');
      const file = e.dataTransfer.files[0];
      if (file) this.loadFile(file);
    });

    input.addEventListener('change', () => {
      if (input.files[0]) this.loadFile(input.files[0]);
    });
  }

  _setupKeyboard() {
    document.addEventListener('keydown', e => {
      if (['INPUT', 'SELECT', 'TEXTAREA'].includes(document.activeElement.tagName)) return;
      if (this.frames.length === 0) return;

      switch (e.key) {
        case ' ':
          e.preventDefault();
          if (!e.repeat) this.togglePlay();
          break;
        case 'ArrowLeft':
          e.preventDefault();
          e.shiftKey ? this.stepSeconds(-1) : this.stepFrames(-1);
          break;
        case 'ArrowRight':
          e.preventDefault();
          e.shiftKey ? this.stepSeconds(1) : this.stepFrames(1);
          break;
        case '[':
          this._changeSpeedStep(-1);
          break;
        case ']':
          this._changeSpeedStep(1);
          break;
        case 'Home':
          e.preventDefault();
          this._seekToFrameIdx(0);
          break;
        case 'End':
          e.preventDefault();
          this._seekToFrameIdx(this.frames.length - 1);
          break;
        case 'v':
        case 'V':
          this.switchDataSource(this.dataSource === 'tracker' ? 'vision' : 'tracker');
          break;
      }
    });
  }

  _changeSpeedStep(dir) {
    const speeds = [0.25, 0.5, 1, 1.5, 2, 4];
    const idx = speeds.indexOf(this.speed);
    const newIdx = Math.max(0, Math.min(speeds.length - 1, idx + dir));
    this.setSpeed(speeds[newIdx]);
    this._rateSelect.value = String(speeds[newIdx]);
  }

  // --- ファイル読み込み ---

  async loadFile(file) {
    this.pause();
    this._setLoadingState(true);

    try {
      const parser = await createSSLLogParser('./proto/ssl_combined.json');
      const result = await parser.parse(file, ratio => {
        this._parseBar.style.width = `${Math.round(ratio * 100)}%`;
      }, {
        replayFps: 10,
        preferSource: 'tracker',
        keepAlternateSource: false,
      });

      this.visionFrames = result.visionFrames;
      this.trackerFrames = result.trackerFrames;
      this.dataSource = this.hasTracker ? 'tracker' : 'vision';
      this.frames = result.frames;
      this.refereeSnapshots = result.refereeSnapshots;
      this.goalMarkers = result.goalMarkers;
      this.gameEvents = result.gameEvents;
      this.teamNames = result.teamNames;
      this.durationNs = result.durationNs;
      this.startNs = this.frames.length > 0 ? this.frames[0].timestampNs : BigInt(0);

      if (this.frames.length === 0) {
        throw new Error('再生可能な Vision / Tracker フレームが見つかりませんでした');
      }

      this._setLoadingState(false);
      this._showPlayer(file.name);
    } catch (err) {
      this._setLoadingState(false);
      alert(`ログの読み込みに失敗しました:\n${err.message}`);
      console.error(err);
    }
  }

  _setLoadingState(loading) {
    this._parseProgress.style.display = loading ? 'block' : 'none';
    if (!loading) this._parseBar.style.width = '0%';

    if (this._replaySpinner) this._replaySpinner.style.display = loading ? 'inline-block' : 'none';
    if (this._replayLoadingText) this._replayLoadingText.style.display = loading ? 'inline' : 'none';

    if (this._replayBar) {
      this._replayBar.classList.toggle('loading', loading);
    }
    [this._btnPlay, this._btnStepBack, this._btnStepFwd, this._rateSelect].forEach(el => {
      if (el) el.disabled = loading;
    });
  }

  _showPlayer(fileName) {
    this._dropZone.style.display = 'none';
    this._playerBody.style.display = 'block';

    // フィールドSVG を構築（初回のみ）
    if (!this.robotElements) {
      const { svg, robotElements, ballEl, eventOverlay } = buildFieldSVG();
      this._fieldCont.innerHTML = '';
      this._fieldCont.appendChild(svg);
      this.robotElements = robotElements;
      this.ballEl = ballEl;
      this.eventOverlay = eventOverlay;
      this._zoomPan = new SvgZoomPan(svg);
    }

    // チーム名の反映
    this._teamNameYellow.textContent = this.teamNames.yellow;
    this._teamNameBlue.textContent   = this.teamNames.blue;
    if (this._replayFilename) this._replayFilename.textContent = fileName;

    // データソース切り替えUI（両方存在する場合のみ表示）
    if (this._dataSourceWrap) {
      this._dataSourceWrap.style.display = (this.hasTracker && this.hasVision) ? '' : 'none';
      this._updateDataSourceUI();
    }

    // ゴールマーカー・ログ構築
    this._buildGoalMarkers();
    this._buildGoalLog();
    this._buildEventMarkers();
    this._buildGameEventLog();

    this._timeTotal.textContent = formatTimeNs(this.durationNs);

    // 状態リセット
    this._lastCommandCounter = -1;
    this._shownEventIds.clear();

    this._seekToFrameIdx(0);
  }

  _showDropZone() {
    this.pause();
    this._dropZone.style.display = '';
    this._playerBody.style.display = 'none';
  }

  // --- マーカー・ログ構築 ---

  _buildGoalMarkers() {
    this._timeline.querySelectorAll('.player-timeline-marker').forEach(el => el.remove());
    const durNum = Number(this.durationNs);
    if (durNum <= 0) return;

    for (const gm of this.goalMarkers) {
      const relNs = Number(gm.timestampNs - this.startNs);
      const pct = Math.min(100, Math.max(0, (relNs / durNum) * 100));
      const marker = document.createElement('div');
      marker.className = `player-timeline-marker ${gm.scoredBy}`;
      marker.style.left = `${pct}%`;
      const name = this.teamNames[gm.scoredBy] || gm.scoredBy;
      marker.title = `${name} ゴール (${gm.score.yellow}-${gm.score.blue})`;
      this._timeline.appendChild(marker);
    }
  }

  _buildEventMarkers() {
    this._timeline.querySelectorAll('.player-timeline-event-marker').forEach(el => el.remove());
    const durNum = Number(this.durationNs);
    if (durNum <= 0) return;

    for (const ev of this.gameEvents) {
      const relNs = Number(ev.timestampNs - this.startNs);
      if (relNs < 0) continue;
      const pct = Math.min(100, Math.max(0, (relNs / durNum) * 100));
      const marker = document.createElement('div');
      marker.className = 'player-timeline-event-marker';
      marker.style.left = `${pct}%`;
      const info = GAME_EVENT_INFO[ev.type];
      marker.title = info ? info.label : `イベント (type=${ev.type})`;
      this._timeline.appendChild(marker);
    }
  }

  _buildGoalLog() {
    this._goalLog.innerHTML = '';
    if (this.goalMarkers.length === 0) {
      this._goalLog.innerHTML = '<span class="no-data-text">ゴールなし</span>';
      return;
    }
    for (const gm of this.goalMarkers) {
      const relNs = gm.timestampNs - this.startNs;
      const name = this.teamNames[gm.scoredBy] || gm.scoredBy;
      const item = document.createElement('div');
      item.className = 'event-log-item';
      item.innerHTML = `
        <span class="event-log-badge ${gm.scoredBy}"></span>
        <span class="event-log-time">${formatTimeNs(relNs)}</span>
        <span class="event-log-label">${name} ゴール (${gm.score.yellow}-${gm.score.blue})</span>
      `;
      item.addEventListener('click', () => this._seekToNs(gm.timestampNs));
      this._goalLog.appendChild(item);
    }
  }

  _buildGameEventLog() {
    this._gameEventLog.innerHTML = '';
    if (this.gameEvents.length === 0) {
      this._gameEventLog.innerHTML = '<span class="no-data-text">なし</span>';
      return;
    }
    for (const ev of this.gameEvents) {
      const relNs = ev.timestampNs - this.startNs;
      if (relNs < BigInt(0)) continue;
      const info = GAME_EVENT_INFO[ev.type];
      const label = info ? info.label : `イベント(${ev.type})`;
      const icon  = info ? info.icon  : '⚠️';
      const item = document.createElement('div');
      item.className = 'event-log-item';
      const teamBadge = ev.byTeam ? `<span class="event-log-badge ${ev.byTeam}"></span>` : '';
      item.innerHTML = `
        ${teamBadge}
        <span class="event-log-time">${formatTimeNs(relNs)}</span>
        <span class="event-log-icon">${icon}</span>
        <span class="event-log-label">${label}</span>
      `;
      item.addEventListener('click', () => this._seekToNs(ev.timestampNs));
      this._gameEventLog.appendChild(item);
    }
  }

  // --- 再生制御 ---

  togglePlay() {
    if (this.playing) this.pause();
    else this.play();
  }

  play() {
    if (this.playing || this.frames.length === 0) return;
    if (this.currentFrameIdx >= this.frames.length - 1) this.currentFrameIdx = 0;
    this.playing = true;
    this._lastTime = null;
    this._elapsedNs = 0;
    this._updatePlayButton(true);
    this._rafId = requestAnimationFrame(this._boundLoop);
  }

  pause() {
    this.playing = false;
    if (this._rafId) cancelAnimationFrame(this._rafId);
    this._rafId = null;
    this._updatePlayButton(false);
  }

  _updatePlayButton(playing) {
    if (!this._playIcon || !this._pauseIcon) return;
    this._playIcon.style.display  = playing ? 'none' : '';
    this._pauseIcon.style.display = playing ? '' : 'none';
  }

  setSpeed(s) {
    this.speed = s;
  }

  stepFrames(delta) {
    this.pause();
    this._seekToFrameIdx(this.currentFrameIdx + delta);
  }

  stepSeconds(deltaSec) {
    this.pause();
    if (this.frames.length === 0) return;
    const currentNs = this.frames[this.currentFrameIdx].timestampNs;
    const targetNs = currentNs + BigInt(Math.round(deltaSec * 1e9));
    this._seekToNsWithReset(targetNs);
  }

  _seekToFrameIdx(idx) {
    const prev = this.currentFrameIdx;
    const clamped = Math.max(0, Math.min(idx, this.frames.length - 1));
    this.currentFrameIdx = clamped;
    this._elapsedNs = 0;
    // 大きなシークならイベントIDリセット
    if (this.frames.length > 0 && Math.abs(idx - prev) > 100) {
      this._shownEventIds.clear();
    }
    this._render();
  }

  _seekToNs(targetNs) {
    if (this.frames.length === 0) return;
    const clamped = targetNs < this.startNs ? this.startNs
      : targetNs > this.startNs + this.durationNs ? this.startNs + this.durationNs
      : targetNs;
    this.currentFrameIdx = findFrameIndex(this.frames, clamped);
    this._elapsedNs = 0;
    this._render();
  }

  _seekToNsWithReset(targetNs) {
    const prevNs = this.frames.length > 0 ? this.frames[this.currentFrameIdx].timestampNs : BigInt(0);
    const diff = targetNs > prevNs ? targetNs - prevNs : prevNs - targetNs;
    if (diff > BigInt(2e9)) this._shownEventIds.clear();
    this._seekToNs(targetNs);
  }

  // --- requestAnimationFrame ループ ---

  _loop(now) {
    if (!this.playing) return;

    if (this._lastTime !== null) {
      const wallDeltaNs = (now - this._lastTime) * 1e6 * this.speed;
      this._elapsedNs += wallDeltaNs;

      while (this.currentFrameIdx < this.frames.length - 1) {
        const nextDeltaNs = Number(
          this.frames[this.currentFrameIdx + 1].timestampNs -
          this.frames[this.currentFrameIdx].timestampNs
        );
        if (this._elapsedNs < nextDeltaNs) break;
        this._elapsedNs -= nextDeltaNs;
        this.currentFrameIdx++;
      }

      if (this.currentFrameIdx >= this.frames.length - 1) {
        this.currentFrameIdx = this.frames.length - 1;
        this._render();
        this.pause();
        return;
      }
    }

    this._lastTime = now;
    this._render();
    this._rafId = requestAnimationFrame(this._boundLoop);
  }

  // --- 描画 ---

  _render() {
    if (this.frames.length === 0 || !this.robotElements) return;

    const frame = this.frames[this.currentFrameIdx];
    updateFrame(frame, this.robotElements, this.ballEl);

    // タイムライン更新（ドラッグ中でなければ）
    if (!this._isDragging) {
      const relNs = frame.timestampNs - this.startNs;
      const durNum = Number(this.durationNs);
      const pct = durNum > 0 ? Math.min(100, Number(relNs) / durNum * 100) : 0;
      this._tlProgress.style.width = `${pct}%`;
      this._tlThumb.style.left = `${pct}%`;
      this._timeCurrent.textContent = formatTimeNs(relNs);
    }

    // レフェリー状態更新
    const refIdx = findRefereeIndex(this.refereeSnapshots, frame.timestampNs);
    if (refIdx >= 0) {
      const { ref } = this.refereeSnapshots[refIdx];
      this._scoreYellow.textContent = (ref.yellow && ref.yellow.score != null) ? ref.yellow.score : '0';
      this._scoreBlue.textContent   = (ref.blue   && ref.blue.score   != null) ? ref.blue.score   : '0';
      this._refStage.textContent    = STAGE_NAMES[ref.stage]   ?? String(ref.stage);
      this._refCommand.textContent  = COMMAND_NAMES[ref.command] ?? String(ref.command);

      // コマンド変更トースト
      const counter = ref.commandCounter != null ? ref.commandCounter : -1;
      if (this._lastCommandCounter !== -1 && counter !== this._lastCommandCounter) {
        const cmdName = COMMAND_NAMES[ref.command] ?? `コマンド(${ref.command})`;
        this._toast.show(cmdName, null, 'info', 3000);
      }
      this._lastCommandCounter = counter;
    }

    // ゲームイベントオーバーレイ更新
    this._updateVisibleGameEvents(frame.timestampNs);
  }

  _updateVisibleGameEvents(currentTimestampNs) {
    const DISPLAY_NS = BigInt(10e9);   // 10秒表示
    const FADEOUT_NS = BigInt(1e9);    // 最後1秒でフェードアウト

    const visibleEvents = [];
    for (const ev of this.gameEvents) {
      const elapsed = currentTimestampNs - ev.timestampNs;
      if (elapsed < BigInt(0) || elapsed > DISPLAY_NS) continue;

      const info = GAME_EVENT_INFO[ev.type];
      const label = info ? info.label : `イベント(${ev.type})`;
      const icon  = info ? info.icon  : '⚠️';
      const details = getEventDetails(ev.eventData, ev.fieldName);

      // フェードアウト計算
      let opacity = 1;
      const remaining = DISPLAY_NS - elapsed;
      if (remaining < FADEOUT_NS) {
        opacity = Number(remaining) / Number(FADEOUT_NS);
      }

      // location: メートル → ミリメートル
      let location = null;
      if (ev.location) {
        location = { x: ev.location.x * 1000, y: ev.location.y * 1000 };
      }

      visibleEvents.push({ ...ev, label, icon, details, opacity, location });

      // 新規イベントのトースト通知
      if (!this._shownEventIds.has(ev.id)) {
        this._shownEventIds.add(ev.id);
        const category = info ? info.category : 'foul';
        const level = category === 'goal' ? 'goal' : category === 'foul' ? 'foul' : 'info';
        const teamName = ev.byTeam ? this.teamNames[ev.byTeam] : null;
        const msg = [teamName, ev.byBot != null ? `#${ev.byBot}` : null, details].filter(Boolean).join(' ');
        this._toast.show(label, msg || null, level, 4000);
      }
    }

    if (this.eventOverlay) {
      updateGameEvents(this.eventOverlay, visibleEvents);
    }
  }
}

// --- ページ初期化 ---
const player = new LogPlayer();
