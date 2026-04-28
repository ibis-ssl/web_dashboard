"""SSL game log parser.

Parses standard SSL binary log format (.log.gz) and extracts goal scenes.
Each goal scene contains ball/robot positions for the 10 seconds before the goal.

SSL log format:
  Header: "SSL_LOG_FILE" (12 bytes) + version (int32, big-endian)
  Messages: timestamp_ns (int64) + message_type (int32) + size (int32) + data
  Message types:
    2: SSL_WrapperPacket (vision 2010)
    3: Referee
    4: SSL_WrapperPacket (vision 2014)
    5: TrackerWrapperPacket (tracker 2020)
"""

import bisect
from collections import deque
import gzip
import io
import math
import re
import struct
import sys
import os
import zlib
from typing import Iterator

# protoモジュールをパスに追加
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_SCRIPT_DIR, "proto"))

from state import ssl_gc_referee_message_pb2
from vision import ssl_vision_wrapper_pb2
from messages_robocup_ssl_wrapper_tracked_pb2 import TrackerWrapperPacket

MSG_TYPE_VISION_2010 = 2
MSG_TYPE_REFEREE = 3
MSG_TYPE_VISION_2014 = 4
MSG_TYPE_TRACKER = 5

SSL_LOG_HEADER = b"SSL_LOG_FILE"
SCENE_DURATION_SEC = 10.0
OUTPUT_FPS = 10

POSSIBLE_GOAL_TYPE = 39  # GameEvent.Type.POSSIBLE_GOAL
_TEAM_YELLOW = 1         # sslgc.Team.YELLOW
_TEAM_BLUE = 2           # sslgc.Team.BLUE
# POSSIBLE_GOALは同一停止中に複数のRefメッセージに現れる。
# 同チームの連続イベントをまとめるクールダウン（秒）。
_POSSIBLE_GOAL_COOLDOWN_NS = int(5 * 1e9)


def _collect_possible_goals(referee_snapshots: list) -> list[tuple[int, int]]:
    """referee_snapshots から POSSIBLE_GOAL イベントを収集する（重複排除済み）。

    Returns:
        [(timestamp_ns, by_team_int), ...]  時系列順
        by_team_int: 1=YELLOW, 2=BLUE
    """
    result: list[tuple[int, int]] = []
    seen_ids: set[str] = set()
    last_ts_by_team: dict[int, int] = {}

    for timestamp_ns, ref in referee_snapshots:
        for ge in ref.game_events:
            if ge.type != POSSIBLE_GOAL_TYPE:
                continue
            try:
                by_team = ge.possible_goal.by_team
            except AttributeError:
                continue

            # IDがあれば ID ベースで重複排除
            eid = ge.id if ge.HasField("id") else None
            if eid:
                if eid in seen_ids:
                    continue
                seen_ids.add(eid)
            else:
                # ID なし: 同チームのクールダウン内なら重複とみなす
                last = last_ts_by_team.get(by_team)
                if last is not None and timestamp_ns - last < _POSSIBLE_GOAL_COOLDOWN_NS:
                    continue

            last_ts_by_team[by_team] = timestamp_ns
            result.append((timestamp_ns, by_team))

    return result


def _find_possible_goal_time(possible_goals: list[tuple[int, int]], goal_time_ns: int, team_int: int) -> int:
    """スコア変化（goal_time_ns）直前で by_team が一致する POSSIBLE_GOAL のタイムスタンプを返す。

    見つからない場合は goal_time_ns をそのまま返す（フォールバック）。
    """
    for ts, bt in reversed(possible_goals):
        if ts <= goal_time_ns and bt == team_int:
            return ts
    return goal_time_ns


def _iter_messages(data: bytes) -> Iterator[tuple[int, int, bytes]]:
    """(timestamp_ns, message_type, raw_data) を順に返すイテレータ。"""
    offset = 0

    # ヘッダー検証
    header = data[offset : offset + 12]
    if header != SSL_LOG_HEADER:
        raise ValueError(f"Invalid SSL log header: {header!r}")
    offset += 12
    _version = struct.unpack_from(">i", data, offset)[0]
    offset += 4

    while offset < len(data):
        if offset + 16 > len(data):
            break
        timestamp_ns, msg_type, msg_size = struct.unpack_from(">qii", data, offset)
        offset += 16
        if msg_size < 0 or offset + msg_size > len(data):
            break
        yield timestamp_ns, msg_type, data[offset : offset + msg_size]
        offset += msg_size


def _detection_to_frame(timestamp_ns: int, wrapper) -> dict | None:
    """SSL_WrapperPacket の detection フレームを dict に変換。"""
    if not wrapper.HasField("detection"):
        return None
    det = wrapper.detection
    return {
        "t_ns": timestamp_ns,
        "ball": {"x": int(det.balls[0].x), "y": int(det.balls[0].y)} if det.balls else None,
        "robots_yellow": [
            {"id": r.robot_id, "x": int(r.x), "y": int(r.y), "theta": round(r.orientation, 1)}
            for r in det.robots_yellow
            if r.HasField("x") and r.HasField("y")
        ],
        "robots_blue": [
            {"id": r.robot_id, "x": int(r.x), "y": int(r.y), "theta": round(r.orientation, 1)}
            for r in det.robots_blue
            if r.HasField("x") and r.HasField("y")
        ],
    }


def _tracker_to_frame(timestamp_ns: int, wrapper) -> dict | None:
    """TrackerWrapperPacket のフレームを dict に変換。"""
    if not wrapper.HasField("tracked_frame"):
        return None
    tf = wrapper.tracked_frame
    ball = None
    if tf.balls:
        b = tf.balls[0]
        ball = {"x": int(b.pos.x * 1000), "y": int(b.pos.y * 1000)}
        if b.HasField("vel"):
            ball["vel_ms"] = math.hypot(b.vel.x, b.vel.y)

    robots_yellow = []
    robots_blue = []
    from messages_robocup_ssl_detection_tracked_pb2 import TeamColor
    for r in tf.robots:
        team_color = r.robot_id.team_color
        entry = {
            "id": r.robot_id.id,
            "x": int(r.pos.x * 1000),
            "y": int(r.pos.y * 1000),
            "theta": round(r.orientation, 1),
            "theta_rad": r.orientation,
        }
        if r.HasField("vel"):
            entry["vel_ms"] = math.hypot(r.vel.x, r.vel.y)
            entry["vel_x"] = r.vel.x
            entry["vel_y"] = r.vel.y
        if team_color == TeamColor.Value("TEAM_COLOR_YELLOW"):
            robots_yellow.append(entry)
        else:
            robots_blue.append(entry)

    return {
        "t_ns": timestamp_ns,
        "ball": ball,
        "robots_yellow": robots_yellow,
        "robots_blue": robots_blue,
    }


def _downsample_frames(
    frames: list[dict], goal_time_ns: int, duration_sec: float, fps: int
) -> list[dict]:
    """ゴール前 duration_sec 秒を fps フレーム/秒にダウンサンプリング。"""
    start_ns = goal_time_ns - int(duration_sec * 1e9)
    interval_ns = int(1e9 / fps)

    # start_ns 以降のフレームのみ対象にして bisect で高速探索
    timestamps = [f["t_ns"] for f in frames]
    base_idx = bisect.bisect_left(timestamps, start_ns)
    candidates = frames[base_idx:]
    candidate_ts = timestamps[base_idx:]

    result = []
    for i in range(int(duration_sec * fps)):
        target_ns = start_ns + i * interval_ns
        if not candidates:
            break
        # target_ns 以上で最も近いフレームのインデックスを bisect で探す
        idx = bisect.bisect_left(candidate_ts, target_ns)
        # idx-1 と idx の前後2候補から近い方を選択
        best = None
        best_diff = float("inf")
        for ci in (idx - 1, idx):
            if 0 <= ci < len(candidates):
                diff = abs(candidates[ci]["t_ns"] - target_ns)
                if diff < best_diff:
                    best_diff = diff
                    best = candidates[ci]
        if best is not None:
            result.append({
                "t": round(i / fps, 1),
                "ball": best["ball"],
                "robots_yellow": best["robots_yellow"],
                "robots_blue": best["robots_blue"],
            })

    return result


def extract_goal_scenes(log_gz_bytes: bytes) -> list[dict]:
    """SSL game log (.log.gz) からゴールシーンを抽出する。

    Returns:
        各ゴールのシーンデータ（dict）のリスト。
        各 dict のキー: goal_index, scored_by, score_after, duration_sec, fps, frames
    """
    # gzip展開（不完全なgzipストリームにも対応するため zlib を直接使用）
    try:
        data = gzip.decompress(log_gz_bytes)
    except EOFError:
        dec = zlib.decompressobj(47)  # 47 = zlib+gzip自動検出
        data = dec.decompress(log_gz_bytes)
        data += dec.flush()

    # 全メッセージを走査してRefereeとフレームデータを収集
    referee_snapshots: list[tuple[int, object]] = []  # (timestamp_ns, Referee)
    position_frames: list[dict] = []  # {t_ns, ball, robots_yellow, robots_blue}
    has_tracker = False

    for timestamp_ns, msg_type, raw in _iter_messages(data):
        if msg_type == MSG_TYPE_REFEREE:
            try:
                ref = ssl_gc_referee_message_pb2.Referee()
                ref.ParseFromString(raw)
                referee_snapshots.append((timestamp_ns, ref))
            except Exception:
                pass

        elif msg_type in (MSG_TYPE_VISION_2010, MSG_TYPE_VISION_2014) and not has_tracker:
            try:
                wrapper = ssl_vision_wrapper_pb2.SSL_WrapperPacket()
                wrapper.ParseFromString(raw)
                frame = _detection_to_frame(timestamp_ns, wrapper)
                if frame:
                    position_frames.append(frame)
            except Exception:
                pass

        elif msg_type == MSG_TYPE_TRACKER:
            try:
                wrapper = TrackerWrapperPacket()
                wrapper.ParseFromString(raw)
                frame = _tracker_to_frame(timestamp_ns, wrapper)
                if frame:
                    if not has_tracker:
                        # Trackerデータが存在 → Visionデータを破棄してTrackerを使用
                        has_tracker = True
                        position_frames = []
                    position_frames.append(frame)
            except Exception:
                pass

    return _goal_scenes_from_parsed(position_frames, referee_snapshots)


# ============================================================
# フル試合分析 (extract_full_analysis)
# ============================================================

GAME_EVENT_LABELS: dict[int, str] = {
    6: 'ボールアウト(タッチ)',
    7: 'ボールアウト(ゴール)',
    11: 'エイムレスキック',
    13: 'キーパー保持',
    14: 'ダブルタッチ',
    17: 'オーバードリブル',
    19: 'エリア接近',
    24: 'プッシング',
    26: 'ホールディング',
    27: '転倒',
    31: 'マルチプルDF',
    43: '境界線横断',
    51: '部品脱落',
    15: 'エリア内タッチ',
    18: 'ボール速度超過',
    21: '衝突(引分)',
    22: '衝突',
    20: '配置妨害',
    28: 'STOP中速度超過',
    29: 'ボール接近',
    52: '交代回数超過',
    39: 'ゴール(確認中)',
    8:  'ゴール',
    44: '無効ゴール',
    45: 'PK失敗',
    2:  '試合の停滞',
    38: 'ロボット数超過',
    3:  '配置失敗',
    5:  '配置成功',
}

STAGE_NAMES: dict[int, str] = {
    0:  'プレゲーム',
    1:  '前半',
    2:  'ハーフタイム',
    3:  '後半プレゲーム',
    4:  '後半',
    5:  'オーバータイム休憩',
    6:  'OT前半プレゲーム',
    7:  'OT前半',
    8:  'OTハーフタイム',
    9:  'OT後半プレゲーム',
    10: 'OT後半',
    11: 'PK戦休憩',
    12: 'PK戦',
    13: '試合終了',
}

COMMAND_NAMES: dict[int, str] = {
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
}

_HEATMAP_BIN_SIZE_MM = 100
_FIELD_X_MIN = -6000
_FIELD_Y_MIN = -4500
_HEATMAP_BINS_X = 120  # 12000 / 100
_HEATMAP_BINS_Y = 90   # 9000 / 100
REPLAY_FPS = 3

# 統計計算用定数
_STATS_MIN_DT_NS = int(5e6)       # 5ms未満のΔtはスキップ（ノイズ排除）
_STATS_MAX_DT_NS = int(500e6)     # 500ms超のΔtはスキップ（フレーム欠落）
_KICK_DETECT_THRESHOLD = 1.5      # m/s の速度増加でキック検出
_SPRINT_THRESHOLD = 2.0           # m/s 以上でスプリント判定
_SPRINT_COOLDOWN_NS = int(500e6)  # 同一ロボットのスプリント再検出間隔


def _compute_match_stats(frames: list[dict]) -> dict:
    """ボール・ロボットの速度・距離・キック・スプリントを1パスで計算する。"""
    ball_max_speed = 0.0
    ball_speed_sum = 0.0
    ball_speed_count = 0
    ball_total_dist_mm = 0.0
    kick_count = 0
    ball_pos_count = 0
    ball_neg_count = 0
    prev_ball_speed = 0.0

    # {(team, id): {"dist_mm": float, "max_spd": float, "sprint_count": int, "last_sprint_ns": int}}
    robot_stats: dict[tuple[str, int], dict] = {}

    prev_frame: dict | None = None
    prev_yellow_map: dict[int, dict] = {}
    prev_blue_map: dict[int, dict] = {}

    for frame in frames:
        ball = frame.get("ball")
        t_ns = frame["t_ns"]

        # ボール陣地カウント
        if ball:
            if ball["x"] > 0:
                ball_pos_count += 1
            else:
                ball_neg_count += 1

        if prev_frame is not None:
            dt_ns = t_ns - prev_frame["t_ns"]
            if _STATS_MIN_DT_NS <= dt_ns <= _STATS_MAX_DT_NS:
                dt_sec = dt_ns / 1e9

                # ボール速度・距離（Tracker の vel のみ使用）
                prev_ball = prev_frame.get("ball")
                if ball and prev_ball:
                    dx = ball["x"] - prev_ball["x"]
                    dy = ball["y"] - prev_ball["y"]
                    dist_mm = math.hypot(dx, dy)
                    speed_ms = ball.get("vel_ms")
                    if speed_ms is not None:
                        if speed_ms > ball_max_speed:
                            ball_max_speed = speed_ms
                        ball_speed_sum += speed_ms
                        ball_speed_count += 1
                        ball_total_dist_mm += dist_mm
                        if speed_ms - prev_ball_speed >= _KICK_DETECT_THRESHOLD:
                            kick_count += 1
                        prev_ball_speed = speed_ms
                    else:
                        prev_ball_speed = 0.0
                else:
                    prev_ball_speed = 0.0

                # ロボット速度・距離
                for team, cur_robots, prev_map in (
                    ("yellow", frame.get("robots_yellow", []), prev_yellow_map),
                    ("blue",   frame.get("robots_blue",   []), prev_blue_map),
                ):
                    for r in cur_robots:
                        rid = r["id"]
                        key = (team, rid)
                        if key not in robot_stats:
                            robot_stats[key] = {"dist_mm": 0.0, "max_spd": 0.0,
                                                "sprint_count": 0, "last_sprint_ns": -_SPRINT_COOLDOWN_NS}
                        pr = prev_map.get(rid)
                        if pr is not None:
                            rdx = r["x"] - pr["x"]
                            rdy = r["y"] - pr["y"]
                            rdist = math.hypot(rdx, rdy)
                            rspd = r.get("vel_ms")  # Tracker の vel のみ使用
                            if rspd is None:
                                continue
                            rs = robot_stats[key]
                            rs["dist_mm"] += rdist
                            if rspd > rs["max_spd"]:
                                rs["max_spd"] = rspd
                            if rspd >= _SPRINT_THRESHOLD and (t_ns - rs["last_sprint_ns"]) >= _SPRINT_COOLDOWN_NS:
                                rs["sprint_count"] += 1
                                rs["last_sprint_ns"] = t_ns

        prev_frame = frame
        prev_yellow_map = {r["id"]: r for r in frame.get("robots_yellow", [])}
        prev_blue_map   = {r["id"]: r for r in frame.get("robots_blue",   [])}

    # 集計
    ball_total = ball_pos_count + ball_neg_count
    ball_stat = {
        "max_speed_ms":    round(ball_max_speed, 2),
        "avg_speed_ms":    round(ball_speed_sum / ball_speed_count if ball_speed_count > 0 else 0.0, 2),
        "total_distance_m": round(ball_total_dist_mm / 1000.0, 1),
        "kick_count": kick_count,
        "territory": {
            "positive_pct": round(ball_pos_count / ball_total * 100, 1) if ball_total > 0 else 50.0,
            "negative_pct": round(ball_neg_count / ball_total * 100, 1) if ball_total > 0 else 50.0,
        },
    }

    def _team_stats(team: str) -> dict:
        items = [(rid, s) for (t, rid), s in robot_stats.items() if t == team]
        if not items:
            return {"total_distance_m": 0.0, "fastest": {"id": -1, "max_speed_ms": 0.0},
                    "total_sprint_count": 0, "robots": []}
        fastest_id, fastest_s = max(items, key=lambda x: x[1]["max_spd"])
        return {
            "total_distance_m": round(sum(s["dist_mm"] for _, s in items) / 1000.0, 1),
            "fastest": {"id": fastest_id, "max_speed_ms": round(fastest_s["max_spd"], 2)},
            "total_sprint_count": sum(s["sprint_count"] for _, s in items),
            "robots": sorted([
                {"id": rid, "max_speed_ms": round(s["max_spd"], 2),
                 "total_distance_m": round(s["dist_mm"] / 1000.0, 1),
                 "sprint_count": s["sprint_count"]}
                for rid, s in items
            ], key=lambda x: x["id"]),
        }

    return {
        "sprint_threshold_ms": _SPRINT_THRESHOLD,
        "ball": ball_stat,
        "robots": {"yellow": _team_stats("yellow"), "blue": _team_stats("blue")},
    }


# 動作特性分析定数 (TIGERs Mannheim ETDP 2026 §3)
_MOTION_MIN_SPEED_MS = 0.1       # 静止ロボット除外閾値 (m/s)
_MOTION_FRAME_OFFSET = 5         # 加速度推定のフレームオフセット (ノイズ低減)
_MOTION_MIN_SAMPLES = 2000       # 動作限界推定に必要な最低サンプル数
_MOTION_SPEED_MAX = 5.0          # 速度ヒストグラム最大値 (m/s)
_MOTION_SPEED_BIN = 0.1          # 速度ビン幅 (m/s)
_MOTION_ACCEL_MIN = -8.0         # 加速度ヒストグラム最小値 (m/s²)
_MOTION_ACCEL_MAX = 8.0          # 加速度ヒストグラム最大値 (m/s²)
_MOTION_ACCEL_BIN = 0.25         # 加速度ビン幅 (m/s²)

# リプレイフレームから除外するロボットフィールド (モーション解析専用・JSONサイズ削減)
_REPLAY_FRAME_EXCLUDE = frozenset({"vel_x", "vel_y", "vel_ms", "theta_rad"})


def _compute_motion_analysis(frames: list[dict]) -> dict:
    """TIGERs Mannheim ETDP 2026 §3 の手法でロボット動作特性を分析する。

    - 速度ヒストグラム: speed > 0.1 m/s のフレームのみ
    - 加速度推定: 速度ベクトルの差分を5フレームオフセットで計算
    - 速度-加速度 2D ヒストグラム (Fig 4 相当)
    - 動作限界推定: パーセンタイルベース (速度:0.995, 加速:0.75, 減速:0.95)
    """
    _SPEED_BINS = int(_MOTION_SPEED_MAX / _MOTION_SPEED_BIN)
    _ACCEL_BINS = int((_MOTION_ACCEL_MAX - _MOTION_ACCEL_MIN) / _MOTION_ACCEL_BIN)

    def _empty_team() -> dict:
        return {
            "speed_hist": [0] * _SPEED_BINS,
            "sa_grid": {},
            "da_grid": {},
            "speed_samples": [],
            "accel_samples": [],
            "decel_samples": [],
        }

    teams = {"yellow": _empty_team(), "blue": _empty_team()}
    robot_history: dict[tuple[str, int], deque] = {}

    for frame in frames:
        t_ns = frame["t_ns"]
        for team_key, robot_list in (("yellow", frame.get("robots_yellow", [])),
                                      ("blue",   frame.get("robots_blue", []))):
            td = teams[team_key]
            for r in robot_list:
                spd = r.get("vel_ms")
                if spd is None:
                    continue

                # 速度ヒストグラム (speed > 閾値のみ)
                if spd >= _MOTION_MIN_SPEED_MS:
                    sbin = min(int(spd / _MOTION_SPEED_BIN), _SPEED_BINS - 1)
                    td["speed_hist"][sbin] += 1
                    td["speed_samples"].append(spd)

                # 加速度推定: 速度ベクトルの差分をフレームオフセットで計算
                vx = r.get("vel_x")
                vy = r.get("vel_y")
                theta = r.get("theta_rad", r.get("theta", 0.0))
                if vx is None or vy is None:
                    continue
                key = (team_key, r["id"])
                if key not in robot_history:
                    robot_history[key] = deque(maxlen=_MOTION_FRAME_OFFSET + 1)
                hist = robot_history[key]
                hist.append((t_ns, vx, vy, spd, theta))

                if len(hist) < _MOTION_FRAME_OFFSET + 1:
                    continue

                t_old, vx_old, vy_old, spd_old, theta_old = hist[0]
                dt_ns = t_ns - t_old
                if not (_STATS_MIN_DT_NS <= dt_ns <= _STATS_MAX_DT_NS * 10):
                    continue
                dt_sec = dt_ns / 1e9

                ax = (vx - vx_old) / dt_sec
                ay = (vy - vy_old) / dt_sec
                acc_mag = math.hypot(ax, ay)
                acc_signed = acc_mag if spd >= spd_old else -acc_mag

                if acc_signed > 0:
                    td["accel_samples"].append(acc_signed)
                elif acc_signed < 0:
                    td["decel_samples"].append(-acc_signed)

                mid_spd = (spd + spd_old) / 2.0
                if mid_spd >= _MOTION_MIN_SPEED_MS:
                    sbin = min(int(mid_spd / _MOTION_SPEED_BIN), _SPEED_BINS - 1)
                    if _MOTION_ACCEL_MIN <= acc_signed <= _MOTION_ACCEL_MAX:
                        abin = min(
                            int((acc_signed - _MOTION_ACCEL_MIN) / _MOTION_ACCEL_BIN),
                            _ACCEL_BINS - 1,
                        )
                        td["sa_grid"][(sbin, abin)] = td["sa_grid"].get((sbin, abin), 0) + 1

                # ロボットローカル座標での加速方向 (ロボット正面=X軸)
                theta_mid = (theta + theta_old) / 2.0
                cos_t, sin_t = math.cos(theta_mid), math.sin(theta_mid)
                ax_local =  ax * cos_t + ay * sin_t   # 前後方向 (正=前)
                ay_local = -ax * sin_t + ay * cos_t   # 左右方向 (正=左)
                if _MOTION_ACCEL_MIN <= ax_local <= _MOTION_ACCEL_MAX and \
                   _MOTION_ACCEL_MIN <= ay_local <= _MOTION_ACCEL_MAX:
                    axb = min(int((ax_local - _MOTION_ACCEL_MIN) / _MOTION_ACCEL_BIN), _ACCEL_BINS - 1)
                    ayb = min(int((ay_local - _MOTION_ACCEL_MIN) / _MOTION_ACCEL_BIN), _ACCEL_BINS - 1)
                    td["da_grid"][(axb, ayb)] = td["da_grid"].get((axb, ayb), 0) + 1

    def _build_team_result(td: dict) -> dict:
        n_speed = len(td["speed_samples"])
        valid = n_speed >= _MOTION_MIN_SAMPLES
        limits: dict = {"sample_count": n_speed, "valid": valid}
        if valid:
            # 各リストを1回だけソートしてから複数パーセンタイルを取得
            def _pct(samples: list[float], p: float) -> float:
                s = sorted(samples)
                return round(s[min(int(len(s) * p), len(s) - 1)], 2)
            limits["velocity_limit"] = _pct(td["speed_samples"], 0.995)
            limits["accel_limit"]    = _pct(td["accel_samples"], 0.75) if td["accel_samples"] else 0.0
            limits["decel_limit"]    = _pct(td["decel_samples"], 0.95) if td["decel_samples"] else 0.0

        return {
            "speed_histogram": {"bin_width": _MOTION_SPEED_BIN, "bins": td["speed_hist"]},
            "speed_accel_heatmap": {
                "speed_bin_width": _MOTION_SPEED_BIN,
                "speed_max": _MOTION_SPEED_MAX,
                "accel_bin_width": _MOTION_ACCEL_BIN,
                "accel_min": _MOTION_ACCEL_MIN,
                "accel_max": _MOTION_ACCEL_MAX,
                "data": [[sb, ab, cnt] for (sb, ab), cnt in td["sa_grid"].items()],
            },
            "directional_accel": {
                "bin_width": _MOTION_ACCEL_BIN,
                "accel_min": _MOTION_ACCEL_MIN,
                "accel_max": _MOTION_ACCEL_MAX,
                "data": [[axb, ayb, cnt] for (axb, ayb), cnt in td["da_grid"].items()],
            },
            "limits": limits,
        }

    return {
        "yellow": _build_team_result(teams["yellow"]),
        "blue":   _build_team_result(teams["blue"]),
    }


def _compute_all_heatmaps(
    frames: list[dict],
) -> tuple[list[list[int]], list[list[int]], list[list[int]]]:
    """ボール・黄ロボット・青ロボットのヒートマップを1パスで計算する。

    Returns: (ball_bins, yellow_bins, blue_bins)
        各要素: [[x_bin, y_bin, count], ...] 非ゼロのみ
    """
    ball_grid:   dict[tuple[int, int], int] = {}
    yellow_grid: dict[tuple[int, int], int] = {}
    blue_grid:   dict[tuple[int, int], int] = {}

    def _add(grid: dict, x: float, y: float) -> None:
        bx = max(0, min(_HEATMAP_BINS_X - 1, int((x - _FIELD_X_MIN) / _HEATMAP_BIN_SIZE_MM)))
        by = max(0, min(_HEATMAP_BINS_Y - 1, int((y - _FIELD_Y_MIN) / _HEATMAP_BIN_SIZE_MM)))
        grid[(bx, by)] = grid.get((bx, by), 0) + 1

    for frame in frames:
        ball = frame.get('ball')
        if ball:
            _add(ball_grid, ball.get('x', 0), ball.get('y', 0))
        for r in frame.get('robots_yellow', []):
            _add(yellow_grid, r.get('x', 0), r.get('y', 0))
        for r in frame.get('robots_blue', []):
            _add(blue_grid, r.get('x', 0), r.get('y', 0))

    def _to_list(grid: dict) -> list[list[int]]:
        return [[bx, by, cnt] for (bx, by), cnt in grid.items()]

    return _to_list(ball_grid), _to_list(yellow_grid), _to_list(blue_grid)


def _compute_possession(frames: list[dict], interval_sec: float = 5.0) -> dict:
    """ポゼッション分析: interval_sec 秒ごとに Yellow チームの占有率を返す。"""
    if not frames:
        return {"timestamps": [], "yellow_ratio": []}

    start_ns = frames[0]["t_ns"]
    end_ns = frames[-1]["t_ns"]
    interval_ns = int(interval_sec * 1e9)

    timestamps: list[float] = []
    yellow_ratios: list[float] = []

    t_win_start = start_ns
    fi = 0  # frame index
    n = len(frames)

    while t_win_start < end_ns:
        t_win_end = t_win_start + interval_ns
        yellow_count = 0
        total_count = 0

        while fi < n and frames[fi]["t_ns"] < t_win_end:
            f = frames[fi]
            fi += 1
            ball = f.get("ball")
            if not ball:
                continue
            bx, by = ball["x"], ball["y"]
            min_d2 = float("inf")
            closest = None
            for r in f.get("robots_yellow", []):
                d2 = (r["x"] - bx) ** 2 + (r["y"] - by) ** 2
                if d2 < min_d2:
                    min_d2 = d2
                    closest = "yellow"
            for r in f.get("robots_blue", []):
                d2 = (r["x"] - bx) ** 2 + (r["y"] - by) ** 2
                if d2 < min_d2:
                    min_d2 = d2
                    closest = "blue"
            if closest:
                total_count += 1
                if closest == "yellow":
                    yellow_count += 1

        t_sec = round((t_win_start - start_ns) / 1e9, 1)
        timestamps.append(t_sec)
        yellow_ratios.append(round(yellow_count / total_count, 3) if total_count > 0 else 0.5)

        t_win_start = t_win_end

    return {"timestamps": timestamps, "yellow_ratio": yellow_ratios}


def _extract_score_timeline(referee_snapshots: list, start_ns: int) -> list[dict]:
    """スコア変化点のタイムライン。"""
    timeline: list[dict] = []
    prev_y = prev_b = -1

    for ts_ns, ref in referee_snapshots:
        y, b = ref.yellow.score, ref.blue.score
        if y != prev_y or b != prev_b:
            timeline.append({
                "t_sec": round((ts_ns - start_ns) / 1e9, 1),
                "yellow": int(y),
                "blue": int(b),
            })
            prev_y, prev_b = y, b

    return timeline


_PLACEHOLDER_TEAM_NAMES = frozenset({"", "unknown"})
_FILENAME_MATCH_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}_\d{2}-\d{2}_"
    r"(?:UNKNOWN_MATCH|GROUP_PHASE|ELIMINATION_PHASE|FRIENDLY)_"
    r"(?P<yellow>.+)-vs-(?P<blue>.+?)(?:_SSL\d+)?$"
)


def _base_from_log_filename(filename: str) -> str:
    base = os.path.splitext(os.path.basename(filename))[0]
    if base.endswith(".log"):
        base = base[:-4]
    return base


def _normalize_team_name(name: str) -> str:
    return name.strip().replace("_", " ")


def _is_placeholder_team_name(name: str) -> bool:
    return name.strip().lower() in _PLACEHOLDER_TEAM_NAMES


def _team_name_from_ref(ref, team_key: str) -> str:
    try:
        team = getattr(ref, team_key)
        if team.HasField("name"):
            return _normalize_team_name(team.name)
    except Exception:
        pass
    return ""


def _score_from_ref(ref) -> tuple[int, int]:
    try:
        return int(ref.yellow.score), int(ref.blue.score)
    except Exception:
        return 0, 0


def _has_match_identity(ref) -> bool:
    yellow_name = _team_name_from_ref(ref, "yellow")
    blue_name = _team_name_from_ref(ref, "blue")
    yellow_score, blue_score = _score_from_ref(ref)
    return (
        not _is_placeholder_team_name(yellow_name)
        or not _is_placeholder_team_name(blue_name)
        or yellow_score != 0
        or blue_score != 0
    )


def _is_terminal_reset_referee(ref) -> bool:
    yellow_name = _team_name_from_ref(ref, "yellow")
    blue_name = _team_name_from_ref(ref, "blue")
    yellow_score, blue_score = _score_from_ref(ref)
    return (
        _is_placeholder_team_name(yellow_name)
        and _is_placeholder_team_name(blue_name)
        and yellow_score == 0
        and blue_score == 0
    )


def _filter_referee_terminal_resets(referee_snapshots: list) -> list:
    """試合後に混入する Unknown/Unknown 0-0 のリセット状態を除外する。"""
    filtered: list = []
    seen_match_identity = False

    for timestamp_ns, ref in referee_snapshots:
        if seen_match_identity and _is_terminal_reset_referee(ref):
            continue

        filtered.append((timestamp_ns, ref))
        if _has_match_identity(ref):
            seen_match_identity = True

    return filtered or referee_snapshots


def _team_names_from_filename(filename: str) -> tuple[str, str] | None:
    match = _FILENAME_MATCH_RE.match(_base_from_log_filename(filename))
    if not match:
        return None
    return (
        _normalize_team_name(match.group("yellow")),
        _normalize_team_name(match.group("blue")),
    )


def _extract_team_names(referee_snapshots: list, filename: str) -> tuple[str, str]:
    team_yellow, team_blue = "", ""

    for _timestamp_ns, ref in referee_snapshots:
        yellow_name = _team_name_from_ref(ref, "yellow")
        blue_name = _team_name_from_ref(ref, "blue")
        if not _is_placeholder_team_name(yellow_name):
            team_yellow = yellow_name
        if not _is_placeholder_team_name(blue_name):
            team_blue = blue_name

    filename_names = _team_names_from_filename(filename)
    if filename_names:
        if not team_yellow:
            team_yellow = filename_names[0]
        if not team_blue:
            team_blue = filename_names[1]

    return team_yellow or "Yellow", team_blue or "Blue"


def _extract_events_timeline(referee_snapshots: list, start_ns: int) -> list[dict]:
    """ゲームイベントのタイムライン。game_events は累積なので差分のみ処理。"""
    events: list[dict] = []
    prev_len = 0

    for ts_ns, ref in referee_snapshots:
        cur_len = len(ref.game_events)
        for i in range(prev_len, cur_len):
            ev = ref.game_events[i]
            try:
                type_val = int(ev.type)
            except Exception:
                continue

            by_team = None
            location = None
            by_bot = None

            try:
                field_name = ev.WhichOneof("event")
                if field_name:
                    sub = getattr(ev, field_name)
                    try:
                        if sub.HasField("by_team"):
                            tv = int(sub.by_team)
                            by_team = "yellow" if tv == 1 else ("blue" if tv == 2 else None)
                    except Exception:
                        pass
                    try:
                        if sub.HasField("location"):
                            location = {
                                "x": round(sub.location.x * 1000),
                                "y": round(sub.location.y * 1000),
                            }
                    except Exception:
                        pass
                    try:
                        if sub.HasField("by_bot"):
                            by_bot = int(sub.by_bot)
                    except Exception:
                        pass
            except Exception:
                pass

            t_sec = round((ts_ns - start_ns) / 1e9, 1)
            events.append({
                "t_sec": t_sec,
                "type": type_val,
                "label": GAME_EVENT_LABELS.get(type_val, f"イベント({type_val})"),
                "by_team": by_team,
                "by_bot": by_bot,
                "location": location,
            })
        prev_len = cur_len

    return events


def _extract_referee_commands(referee_snapshots: list, start_ns: int) -> list[dict]:
    """ステージ・コマンド変化のタイムライン。"""
    timeline: list[dict] = []
    prev_stage = prev_command = object()  # sentinel

    for ts_ns, ref in referee_snapshots:
        stage = int(ref.stage)
        command = int(ref.command)
        if stage != prev_stage or command != prev_command:
            timeline.append({
                "t_sec": round((ts_ns - start_ns) / 1e9, 1),
                "stage": STAGE_NAMES.get(stage, f"ステージ({stage})"),
                "command": COMMAND_NAMES.get(command, f"コマンド({command})"),
            })
            prev_stage, prev_command = stage, command

    return timeline


def _downsample_replay_frames(frames: list[dict], start_ns: int, fps: int) -> list[dict]:
    """フルマッチフレームを fps にダウンサンプリング。t_ns の代わりに t_sec を格納。"""
    if not frames:
        return []

    end_ns = frames[-1]["t_ns"]
    interval_ns = int(1e9 / fps)
    result: list[dict] = []
    fi = 0
    n = len(frames)
    t_ns = start_ns

    while t_ns <= end_ns:
        # t_ns 以降で最も近いフレームを探す
        while fi + 1 < n and frames[fi + 1]["t_ns"] <= t_ns:
            fi += 1
        # 前後のフレームから近い方を選択
        best_fi = fi
        if fi + 1 < n:
            d_curr = abs(frames[fi]["t_ns"] - t_ns)
            d_next = abs(frames[fi + 1]["t_ns"] - t_ns)
            if d_next < d_curr:
                best_fi = fi + 1
        f = frames[best_fi]
        result.append({
            "t_sec": round((t_ns - start_ns) / 1e9, 2),
            "ball": f["ball"],
            "robots_yellow": [
                {k: v for k, v in r.items() if k not in _REPLAY_FRAME_EXCLUDE}
                for r in f["robots_yellow"]
            ],
            "robots_blue": [
                {k: v for k, v in r.items() if k not in _REPLAY_FRAME_EXCLUDE}
                for r in f["robots_blue"]
            ],
        })
        t_ns += interval_ns

    return result


def _goal_scenes_from_parsed(position_frames: list[dict], referee_snapshots: list) -> list[dict]:
    """パース済みデータからゴールシーンを抽出 (extract_goal_scenes の内部版)。"""
    possible_goals = _collect_possible_goals(referee_snapshots)
    scenes: list[dict] = []
    prev_y = prev_b = 0
    goal_index = 0

    for goal_time_ns, ref in referee_snapshots:
        y, b = ref.yellow.score, ref.blue.score
        scored_by = None
        team_int = None
        if y > prev_y:
            scored_by = "yellow"
            team_int = _TEAM_YELLOW
        elif b > prev_b:
            scored_by = "blue"
            team_int = _TEAM_BLUE

        if scored_by:
            scene_end_ns = _find_possible_goal_time(possible_goals, goal_time_ns, team_int)
            start_ns = scene_end_ns - int(SCENE_DURATION_SEC * 1e9)
            raw = [f for f in position_frames if start_ns <= f["t_ns"] <= scene_end_ns]
            if raw:
                frames = _downsample_frames(raw, scene_end_ns, SCENE_DURATION_SEC, OUTPUT_FPS)
                scenes.append({
                    "goal_index": goal_index,
                    "scored_by": scored_by,
                    "score_after": {"yellow": int(y), "blue": int(b)},
                    "duration_sec": SCENE_DURATION_SEC,
                    "fps": OUTPUT_FPS,
                    "frames": frames,
                })
                goal_index += 1

        prev_y, prev_b = y, b

    return scenes


def extract_full_analysis(log_gz_bytes: bytes, filename: str = "") -> dict:
    """SSL game log (.log.gz) をフル解析して詳細分析データを返す。

    Returns:
        meta, replay_frames, ball_heatmap, robot_heatmaps,
        goal_scenes, events, possession, score_timeline, referee_commands
    """

    # gzip 展開
    try:
        data = gzip.decompress(log_gz_bytes)
    except EOFError:
        dec = zlib.decompressobj(47)
        data = dec.decompress(log_gz_bytes)
        data += dec.flush()

    referee_snapshots: list[tuple[int, object]] = []
    position_frames: list[dict] = []
    has_tracker = False

    for timestamp_ns, msg_type, raw in _iter_messages(data):
        if msg_type == MSG_TYPE_REFEREE:
            try:
                ref = ssl_gc_referee_message_pb2.Referee()
                ref.ParseFromString(raw)
                referee_snapshots.append((timestamp_ns, ref))
            except Exception:
                pass
        elif msg_type in (MSG_TYPE_VISION_2010, MSG_TYPE_VISION_2014) and not has_tracker:
            try:
                wrapper = ssl_vision_wrapper_pb2.SSL_WrapperPacket()
                wrapper.ParseFromString(raw)
                frame = _detection_to_frame(timestamp_ns, wrapper)
                if frame:
                    position_frames.append(frame)
            except Exception:
                pass
        elif msg_type == MSG_TYPE_TRACKER:
            try:
                wrapper = TrackerWrapperPacket()
                wrapper.ParseFromString(raw)
                frame = _tracker_to_frame(timestamp_ns, wrapper)
                if frame:
                    if not has_tracker:
                        has_tracker = True
                        position_frames = []
                    position_frames.append(frame)
            except Exception:
                pass

    referee_snapshots = _filter_referee_terminal_resets(referee_snapshots)

    # 開始時刻の基準
    start_ns = position_frames[0]["t_ns"] if position_frames else (
        referee_snapshots[0][0] if referee_snapshots else 0
    )
    end_ns = position_frames[-1]["t_ns"] if position_frames else (
        referee_snapshots[-1][0] if referee_snapshots else 0
    )
    duration_sec = round((end_ns - start_ns) / 1e9, 1)

    # チーム名・最終スコア
    team_yellow, team_blue = _extract_team_names(referee_snapshots, filename)
    final_score = {"yellow": 0, "blue": 0}
    if referee_snapshots:
        last_ref = referee_snapshots[-1][1]
        yellow_score, blue_score = _score_from_ref(last_ref)
        final_score = {
            "yellow": yellow_score,
            "blue": blue_score,
        }

    # ファイル名からID生成
    base = _base_from_log_filename(filename)
    match_id = base.replace(" ", "_").replace("/", "_").replace("\\", "_")

    ball_heatmap, yellow_heatmap, blue_heatmap = _compute_all_heatmaps(position_frames)
    match_stats = _compute_match_stats(position_frames)
    motion_analysis = _compute_motion_analysis(position_frames)

    return {
        "meta": {
            "id": match_id,
            "filename": os.path.basename(filename),
            "teams": {"yellow": team_yellow, "blue": team_blue},
            "final_score": final_score,
            "duration_sec": duration_sec,
        },
        "match_stats": match_stats,
        "motion_analysis": motion_analysis,
        "replay_frames": _downsample_replay_frames(position_frames, start_ns, REPLAY_FPS),
        "ball_heatmap": ball_heatmap,
        "robot_heatmaps": {"yellow": yellow_heatmap, "blue": blue_heatmap},
        "goal_scenes": _goal_scenes_from_parsed(position_frames, referee_snapshots),
        "events": _extract_events_timeline(referee_snapshots, start_ns),
        "possession": _compute_possession(position_frames),
        "score_timeline": _extract_score_timeline(referee_snapshots, start_ns),
        "referee_commands": _extract_referee_commands(referee_snapshots, start_ns),
    }
