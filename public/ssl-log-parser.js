/**
 * SSL game log parser (browser)
 *
 * ssl_log_parser.py のブラウザ版移植。
 * .log / .log.gz ファイルをストリーミングで読み込み、再生用に間引いた
 * フレーム、レフェリー状態、ゴールマーカー、ゲームイベント、チーム名を返す。
 *
 * SSL log format:
 *   Header : "SSL_LOG_FILE" (12 bytes) + version (int32 big-endian)
 *   Messages: timestamp_ns (int64 BE) + msg_type (int32 BE) + size (int32 BE) + data
 *   Message types: 2=Vision2010, 3=Referee, 4=Vision2014, 5=Tracker
 */

const MSG_TYPE_VISION_2010 = 2;
const MSG_TYPE_REFEREE    = 3;
const MSG_TYPE_VISION_2014 = 4;
const MSG_TYPE_TRACKER    = 5;
const SSL_LOG_HEADER      = 'SSL_LOG_FILE';
const SSL_LOG_HEADER_BYTES = 12;
const SSL_LOG_PREAMBLE_BYTES = 16;
const DEFAULT_REPLAY_FPS = 10;

const GAME_EVENT_ONEOF_FIELDS = [
  'ballLeftFieldTouchLine', 'ballLeftFieldGoalLine', 'aimlessKick',
  'attackerTooCloseToDefenseArea', 'defenderInDefenseArea', 'boundaryCrossing',
  'keeperHeldBall', 'botDribbledBallTooFar', 'botPushedBot', 'botHeldBallDeliberately',
  'botTippedOver', 'botDroppedParts', 'attackerTouchedBallInDefenseArea',
  'botKickedBallTooFast', 'botCrashUnique', 'botCrashDrawn',
  'defenderTooCloseToKickPoint', 'botTooFastInStop', 'botInterferedPlacement',
  'possibleGoal', 'goal', 'invalidGoal', 'attackerDoubleTouchedBall',
  'placementSucceeded', 'penaltyKickFailed', 'noProgressInGame', 'placementFailed',
  'multipleCards', 'multipleFouls', 'botSubstitution', 'excessiveBotSubstitution',
  'tooManyRobots', 'challengeFlag', 'challengeFlagHandled', 'emergencyStop',
  'unsportingBehaviorMinor', 'unsportingBehaviorMajor',
  'indirectGoal', 'chippedGoal', 'kickTimeout',
  'attackerTouchedOpponentInDefenseArea', 'attackerTouchedOpponentInDefenseAreaSkipped',
  'botCrashUniqueSkipped', 'botPushedBotSkipped', 'defenderInDefenseAreaPartially',
  'multiplePlacementFailures',
];

class SSLLogParser {
  /**
   * @param {object} pbRoot - protobuf.Root インスタンス（protobufjs v7）
   */
  constructor(pbRoot) {
    this._root = pbRoot;
    this._SSL_WrapperPacket      = pbRoot.lookupType('SSL_WrapperPacket');
    this._TrackerWrapperPacket   = pbRoot.lookupType('TrackerWrapperPacket');
    this._Referee                = pbRoot.lookupType('Referee');
  }

  /**
   * .log / .log.gz ファイルをパースする
   * @param {File|Blob|ArrayBuffer|ArrayBufferView} input
   * @param {function} onProgress - (ratio: 0..1) コールバック（任意）
   * @param {{ replayFps?: number, preferSource?: 'tracker'|'vision', keepAlternateSource?: boolean }} options
   * @returns {{ frames, visionFrames, trackerFrames, hasTracker, hasVision, refereeSnapshots, goalMarkers, gameEvents, teamNames, durationNs }}
   */
  async parse(input, onProgress, options = {}) {
    const replayFps = this._normalizeReplayFps(options.replayFps);
    const preferSource = options.preferSource === 'vision' ? 'vision' : 'tracker';
    const keepAlternateSource = options.keepAlternateSource === true;
    const frameIntervalNs = BigInt(Math.round(1e9 / replayFps));

    if (onProgress) onProgress(0);

    const streamInfo = await this._openLogStream(input, onProgress);
    const state = this._createParseState(frameIntervalNs, preferSource, keepAlternateSource);
    let msgCount = 0;

    try {
      for await (const { timestampNs, msgType, raw } of this._iterMessagesFromStream(streamInfo.stream)) {
        msgCount++;
        this._processMessage(timestampNs, msgType, raw, state);

        if (msgCount % 5000 === 0) {
          await new Promise(resolve => setTimeout(resolve, 0));
        }
      }
    } catch (err) {
      if (streamInfo.compressed && !err.isSSLLogFormatError) {
        throw new Error(`gzip展開に失敗しました: ${err.message || err}`);
      }
      throw err;
    }

    this._appendFinalFrame(state.trackerFrames, state.lastSeenTrackerFrame);
    if (!state.hasTracker || keepAlternateSource || preferSource === 'vision') {
      this._appendFinalFrame(state.visionFrames, state.lastSeenVisionFrame);
    }

    state.gameEvents.sort((a, b) => this._compareBigInt(a.timestampNs, b.timestampNs));

    const useTracker = preferSource === 'tracker'
      ? state.trackerFrames.length > 0
      : state.visionFrames.length === 0 && state.trackerFrames.length > 0;
    const frames = useTracker ? state.trackerFrames : state.visionFrames;

    const durationNs = frames.length > 1
      ? frames[frames.length - 1].timestampNs - frames[0].timestampNs
      : BigInt(0);

    if (onProgress) onProgress(1.0);
    return {
      frames,
      visionFrames: state.visionFrames,
      trackerFrames: state.trackerFrames,
      hasTracker: state.hasTracker,
      hasVision: state.hasVision,
      refereeSnapshots: state.refereeSnapshots,
      goalMarkers: state.goalMarkers,
      gameEvents: state.gameEvents,
      teamNames: state.teamNames,
      durationNs,
    };
  }

  // --- 内部メソッド ---

  _normalizeReplayFps(value) {
    const fps = Number(value || DEFAULT_REPLAY_FPS);
    if (!Number.isFinite(fps) || fps <= 0) return DEFAULT_REPLAY_FPS;
    return Math.min(60, Math.max(1, fps));
  }

  async _openLogStream(input, onProgress) {
    const source = this._normalizeInput(input);
    const peek = await this._peekBytes(source, SSL_LOG_HEADER_BYTES);
    const rawStream = this._withReadProgress(source.stream, source.size, onProgress);

    if (this._isGzip(peek)) {
      return {
        compressed: true,
        stream: this._gunzipStream(rawStream),
      };
    }

    if (this._isSSLLogHeader(peek)) {
      return {
        compressed: false,
        stream: rawStream,
      };
    }

    const headerPreview = new TextDecoder().decode(peek);
    throw this._formatError(`無効なSSLログヘッダー: "${headerPreview}"`);
  }

  _normalizeInput(input) {
    if (typeof Blob !== 'undefined' && input instanceof Blob) {
      return {
        size: input.size,
        stream: input.stream(),
        slice: (start, end) => input.slice(start, end).arrayBuffer(),
      };
    }

    if (input instanceof ArrayBuffer) {
      return this._bytesSource(new Uint8Array(input));
    }

    if (ArrayBuffer.isView(input)) {
      return this._bytesSource(new Uint8Array(input.buffer, input.byteOffset, input.byteLength));
    }

    throw new Error('ログ入力は File、Blob、ArrayBuffer のいずれかで指定してください');
  }

  _bytesSource(bytes) {
    return {
      size: bytes.byteLength,
      stream: new ReadableStream({
        start(controller) {
          controller.enqueue(bytes);
          controller.close();
        },
      }),
      slice: (start, end) => Promise.resolve(bytes.slice(start, end).buffer),
    };
  }

  async _peekBytes(source, length) {
    const buffer = await source.slice(0, length);
    return new Uint8Array(buffer);
  }

  _isGzip(bytes) {
    return bytes.length >= 2 && bytes[0] === 0x1f && bytes[1] === 0x8b;
  }

  _isSSLLogHeader(bytes) {
    if (bytes.length < SSL_LOG_HEADER_BYTES) return false;
    return new TextDecoder().decode(bytes.subarray(0, SSL_LOG_HEADER_BYTES)) === SSL_LOG_HEADER;
  }

  _withReadProgress(stream, totalBytes, onProgress) {
    if (!onProgress || !Number.isFinite(totalBytes) || totalBytes <= 0) return stream;

    const reader = stream.getReader();
    let loaded = 0;
    let lastEmit = 0;
    return new ReadableStream({
      async pull(controller) {
        const { done, value } = await reader.read();
        if (done) {
          onProgress(0.98);
          controller.close();
          return;
        }

        loaded += value.byteLength || value.length || 0;
        const now = typeof performance !== 'undefined' ? performance.now() : Date.now();
        if (now - lastEmit > 100 || loaded >= totalBytes) {
          lastEmit = now;
          onProgress(Math.min(0.98, loaded / totalBytes * 0.98));
        }
        controller.enqueue(value);
      },
      cancel(reason) {
        return reader.cancel(reason);
      },
    });
  }

  _gunzipStream(stream) {
    if (typeof DecompressionStream !== 'undefined') {
      return stream.pipeThrough(new DecompressionStream('gzip'));
    }
    return this._pakoInflateStream(stream);
  }

  _pakoInflateStream(stream) {
    if (typeof pako === 'undefined' || typeof pako.Inflate !== 'function') {
      throw new Error('gzip展開に失敗しました: DecompressionStream非対応で、pakoも読み込まれていません');
    }
    if (typeof TransformStream === 'undefined') {
      throw new Error('gzip展開に失敗しました: このブラウザはストリーミング展開に対応していません');
    }

    const inflator = new pako.Inflate();
    return stream.pipeThrough(new TransformStream({
      start(controller) {
        inflator.onData = chunk => {
          controller.enqueue(chunk instanceof Uint8Array ? chunk : new Uint8Array(chunk));
        };
      },
      transform(chunk) {
        inflator.push(chunk, false);
        if (inflator.err) {
          throw new Error(inflator.msg || `pako inflate error: ${inflator.err}`);
        }
      },
      flush() {
        inflator.push(new Uint8Array(0), true);
        if (inflator.err) {
          throw new Error(inflator.msg || `pako inflate error: ${inflator.err}`);
        }
      },
    }));
  }

  async * _iterMessagesFromStream(stream) {
    const reader = stream.getReader();
    let pending = new Uint8Array(0);
    let offset = 0;
    let headerRead = false;

    while (true) {
      if (!headerRead) {
        pending = await this._fillPending(reader, pending, SSL_LOG_PREAMBLE_BYTES);
        if (pending.byteLength < SSL_LOG_PREAMBLE_BYTES) {
          throw this._formatError('SSLログヘッダーが途中で終了しました');
        }

        const header = new TextDecoder().decode(pending.subarray(0, SSL_LOG_HEADER_BYTES));
        if (header !== SSL_LOG_HEADER) {
          throw this._formatError(`無効なSSLログヘッダー: "${header}"`);
        }
        pending = pending.subarray(SSL_LOG_PREAMBLE_BYTES);
        offset = SSL_LOG_PREAMBLE_BYTES;
        headerRead = true;
      }

      pending = await this._fillPending(reader, pending, 16);
      if (pending.byteLength === 0) return;
      if (pending.byteLength < 16) {
        throw this._formatError('SSLログメッセージヘッダーが途中で終了しました');
      }

      const headerView = new DataView(pending.buffer, pending.byteOffset, 16);
      const timestampNs = headerView.getBigInt64(0, false);
      const msgType = headerView.getInt32(8, false);
      const msgSize = headerView.getInt32(12, false);
      if (msgSize < 0) {
        throw this._formatError(`無効なSSLログメッセージサイズ: ${msgSize}`);
      }

      pending = await this._fillPending(reader, pending, 16 + msgSize);
      if (pending.byteLength < 16 + msgSize) {
        throw this._formatError('SSLログメッセージ本体が途中で終了しました');
      }

      const raw = pending.subarray(16, 16 + msgSize);
      offset += 16 + msgSize;
      yield { timestampNs, msgType, raw, byteOffset: offset };
      pending = pending.subarray(16 + msgSize);
    }
  }

  async _fillPending(reader, pending, minBytes) {
    while (pending.byteLength < minBytes) {
      const { done, value } = await reader.read();
      if (done) break;
      pending = this._concatPending(pending, value);
    }
    return pending;
  }

  _concatPending(pending, chunk) {
    if (!chunk || chunk.byteLength === 0) return pending;
    const bytes = chunk instanceof Uint8Array ? chunk : new Uint8Array(chunk);
    if (pending.byteLength === 0) return bytes;
    const combined = new Uint8Array(pending.byteLength + bytes.byteLength);
    combined.set(pending, 0);
    combined.set(bytes, pending.byteLength);
    return combined;
  }

  _formatError(message) {
    const err = new Error(message);
    err.isSSLLogFormatError = true;
    return err;
  }

  _createParseState(frameIntervalNs, preferSource, keepAlternateSource) {
    return {
      frameIntervalNs,
      preferSource,
      keepAlternateSource,
      visionFrames: [],
      trackerFrames: [],
      hasVision: false,
      hasTracker: false,
      lastKeptVisionNs: null,
      lastKeptTrackerNs: null,
      lastSeenVisionFrame: null,
      lastSeenTrackerFrame: null,
      refereeSnapshots: [],
      lastRefereeState: null,
      prevYellowScore: 0,
      prevBlueScore: 0,
      goalMarkers: [],
      gameEvents: [],
      seenEventIds: new Set(),
      teamNames: { yellow: 'Yellow', blue: 'Blue' },
    };
  }

  _processMessage(timestampNs, msgType, raw, state) {
    if (msgType === MSG_TYPE_REFEREE) {
      try {
        const ref = this._Referee.decode(raw);
        this._processReferee(timestampNs, ref, state);
      } catch (_) {}
      return;
    }

    if (msgType === MSG_TYPE_VISION_2010 || msgType === MSG_TYPE_VISION_2014) {
      state.hasVision = true;
      if (state.hasTracker && state.preferSource === 'tracker' && !state.keepAlternateSource) return;
      try {
        const wrapper = this._SSL_WrapperPacket.decode(raw);
        const frame = this._visionToFrame(timestampNs, wrapper);
        if (frame) this._processFrame(frame, 'vision', state);
      } catch (_) {}
      return;
    }

    if (msgType === MSG_TYPE_TRACKER) {
      state.hasTracker = true;
      if (state.preferSource === 'tracker' && !state.keepAlternateSource && state.visionFrames.length > 0) {
        state.visionFrames = [];
        state.lastKeptVisionNs = null;
      }
      try {
        const wrapper = this._TrackerWrapperPacket.decode(raw);
        const frame = this._trackerToFrame(timestampNs, wrapper);
        if (frame) this._processFrame(frame, 'tracker', state);
      } catch (_) {}
    }
  }

  _processFrame(frame, source, state) {
    const framesKey = source === 'tracker' ? 'trackerFrames' : 'visionFrames';
    const lastKeptKey = source === 'tracker' ? 'lastKeptTrackerNs' : 'lastKeptVisionNs';
    const lastSeenKey = source === 'tracker' ? 'lastSeenTrackerFrame' : 'lastSeenVisionFrame';

    state[lastSeenKey] = frame;
    if (
      state[lastKeptKey] === null ||
      frame.timestampNs - state[lastKeptKey] >= state.frameIntervalNs
    ) {
      state[framesKey].push(frame);
      state[lastKeptKey] = frame.timestampNs;
    }
  }

  _appendFinalFrame(frames, frame) {
    if (!frame) return;
    if (frames.length === 0 || frames[frames.length - 1].timestampNs !== frame.timestampNs) {
      frames.push(frame);
    }
  }

  _processReferee(timestampNs, ref, state) {
    const snapshot = this._refereeSnapshot(ref);
    this._updateTeamNames(snapshot, state.teamNames);
    this._updateGoalMarkers(timestampNs, snapshot, state);
    this._appendGameEvents(timestampNs, ref, state.seenEventIds, state.gameEvents);

    if (!state.lastRefereeState || !this._sameRefereeSnapshot(state.lastRefereeState, snapshot)) {
      state.refereeSnapshots.push({ timestampNs, ref: snapshot });
      state.lastRefereeState = snapshot;
    }
  }

  _refereeSnapshot(ref) {
    return {
      stage: ref.stage ?? 0,
      command: ref.command ?? 0,
      commandCounter: ref.commandCounter ?? 0,
      yellow: {
        score: ref.yellow && ref.yellow.score != null ? ref.yellow.score : 0,
        name: ref.yellow && ref.yellow.name ? ref.yellow.name : '',
      },
      blue: {
        score: ref.blue && ref.blue.score != null ? ref.blue.score : 0,
        name: ref.blue && ref.blue.name ? ref.blue.name : '',
      },
    };
  }

  _sameRefereeSnapshot(a, b) {
    return a.stage === b.stage &&
      a.command === b.command &&
      a.commandCounter === b.commandCounter &&
      a.yellow.score === b.yellow.score &&
      a.blue.score === b.blue.score &&
      a.yellow.name === b.yellow.name &&
      a.blue.name === b.blue.name;
  }

  _updateTeamNames(ref, teamNames) {
    if (ref.yellow.name) teamNames.yellow = ref.yellow.name;
    if (ref.blue.name) teamNames.blue = ref.blue.name;
  }

  _updateGoalMarkers(timestampNs, ref, state) {
    const yellow = ref.yellow.score;
    const blue = ref.blue.score;

    if (yellow > state.prevYellowScore) {
      state.goalMarkers.push({ timestampNs, scoredBy: 'yellow', score: { yellow, blue } });
    } else if (blue > state.prevBlueScore) {
      state.goalMarkers.push({ timestampNs, scoredBy: 'blue', score: { yellow, blue } });
    }

    state.prevYellowScore = yellow;
    state.prevBlueScore = blue;
  }

  _appendGameEvents(timestampNs, ref, seenIds, events) {
    if (!ref.gameEvents || ref.gameEvents.length === 0) return;

    for (const ge of ref.gameEvents) {
      const id = ge.id || null;
      if (id && seenIds.has(id)) continue;
      if (id) seenIds.add(id);

      let eventData = null;
      let fieldName = null;
      for (const field of GAME_EVENT_ONEOF_FIELDS) {
        if (ge[field] != null) {
          eventData = ge[field];
          fieldName = field;
          break;
        }
      }

      let byTeam = null;
      let byBot = null;
      let location = null;

      if (eventData) {
        if (eventData.byTeam != null) {
          byTeam = eventData.byTeam === 1 ? 'yellow' : eventData.byTeam === 2 ? 'blue' : null;
        }
        if (eventData.byBot != null) byBot = eventData.byBot;
        if (eventData.kickingBot != null && byBot == null) byBot = eventData.kickingBot;

        if (eventData.location) {
          location = { x: eventData.location.x || 0, y: eventData.location.y || 0 };
        } else if (eventData.ballLocation) {
          location = { x: eventData.ballLocation.x || 0, y: eventData.ballLocation.y || 0 };
        }
      }

      let eventTimestampNs = timestampNs;
      if (ge.createdTimestamp != null) {
        try {
          eventTimestampNs = BigInt(ge.createdTimestamp.toString());
        } catch (_) {}
      }

      events.push({
        id: id || `${String(timestampNs)}_${ge.type}_${events.length}`,
        timestampNs: eventTimestampNs,
        type: ge.type || 0,
        fieldName,
        byTeam,
        byBot,
        location,
        eventData,
      });
    }
  }

  _compareBigInt(a, b) {
    return a < b ? -1 : a > b ? 1 : 0;
  }

  _visionToFrame(timestampNs, wrapper) {
    if (!wrapper.detection) return null;
    const det = wrapper.detection;
    return {
      timestampNs,
      ball: det.balls && det.balls.length > 0
        ? { x: Math.round(det.balls[0].x), y: Math.round(det.balls[0].y) }
        : null,
      robots_yellow: (det.robotsYellow || []).map(r => ({
        id: r.robotId, x: Math.round(r.x), y: Math.round(r.y), theta: r.orientation || 0,
      })),
      robots_blue: (det.robotsBlue || []).map(r => ({
        id: r.robotId, x: Math.round(r.x), y: Math.round(r.y), theta: r.orientation || 0,
      })),
    };
  }

  _trackerToFrame(timestampNs, wrapper) {
    if (!wrapper.trackedFrame) return null;
    const tf = wrapper.trackedFrame;

    const ball = tf.balls && tf.balls.length > 0 && tf.balls[0].pos
      ? { x: Math.round(tf.balls[0].pos.x * 1000), y: Math.round(tf.balls[0].pos.y * 1000) }
      : null;

    const robots_yellow = [];
    const robots_blue = [];
    for (const r of (tf.robots || [])) {
      if (!r.robotId || !r.pos) continue;
      const entry = {
        id: r.robotId.id,
        x: Math.round(r.pos.x * 1000),
        y: Math.round(r.pos.y * 1000),
        theta: r.orientation || 0,
      };
      if (r.robotId.teamColor === 0) robots_yellow.push(entry);
      else robots_blue.push(entry);
    }

    return { timestampNs, ball, robots_yellow, robots_blue };
  }

  _detectGoals(refereeSnapshots) {
    const markers = [];
    let prevYellow = 0;
    let prevBlue = 0;

    for (const { timestampNs, ref } of refereeSnapshots) {
      const yellow = (ref.yellow && ref.yellow.score != null) ? ref.yellow.score : 0;
      const blue   = (ref.blue   && ref.blue.score   != null) ? ref.blue.score   : 0;

      if (yellow > prevYellow) {
        markers.push({ timestampNs, scoredBy: 'yellow', score: { yellow, blue } });
      } else if (blue > prevBlue) {
        markers.push({ timestampNs, scoredBy: 'blue', score: { yellow, blue } });
      }

      prevYellow = yellow;
      prevBlue   = blue;
    }

    return markers;
  }

  /**
   * refereeスナップショットからゲームイベントを抽出・重複排除する
   * @returns {Array} 正規化済みゲームイベント配列
   */
  _extractGameEvents(refereeSnapshots) {
    const seenIds = new Set();
    const events = [];
    for (const { timestampNs, ref } of refereeSnapshots) {
      this._appendGameEvents(timestampNs, ref, seenIds, events);
    }
    events.sort((a, b) => (a.timestampNs < b.timestampNs ? -1 : a.timestampNs > b.timestampNs ? 1 : 0));
    return events;
  }

  /**
   * refereeスナップショットから最初のチーム名を取得する
   * @returns {{ yellow: string, blue: string }}
   */
  _extractTeamNames(refereeSnapshots) {
    for (const { ref } of refereeSnapshots) {
      const yellowName = ref.yellow && ref.yellow.name;
      const blueName   = ref.blue   && ref.blue.name;
      if (yellowName || blueName) {
        return {
          yellow: yellowName || 'Yellow',
          blue:   blueName   || 'Blue',
        };
      }
    }
    return { yellow: 'Yellow', blue: 'Blue' };
  }
}

// protobuf.json を読み込んで SSLLogParser インスタンスを生成するファクトリ
async function createSSLLogParser(protoJsonUrl) {
  const res = await fetch(protoJsonUrl);
  if (!res.ok) throw new Error(`proto JSON 読み込み失敗: ${res.status}`);
  const jsonDescriptor = await res.json();
  const root = protobuf.Root.fromJSON(jsonDescriptor);
  return new SSLLogParser(root);
}
