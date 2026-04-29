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
from messages_robocup_ssl_detection_tracked_pb2 import TeamColor as _TeamColor
_TRACKER_TEAM_YELLOW = _TeamColor.Value("TEAM_COLOR_YELLOW")

MSG_TYPE_VISION_2010 = 2
MSG_TYPE_REFEREE = 3
MSG_TYPE_VISION_2014 = 4
MSG_TYPE_TRACKER = 5

SSL_LOG_HEADER = b"SSL_LOG_FILE"
# extract_full_analysis() の出力スキーマや解析ロジックを実質的に変えたら必ず bump する。
ANALYSIS_VERSION = 4
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
            ball["vel_x"] = b.vel.x
            ball["vel_y"] = b.vel.y

    robots_yellow = []
    robots_blue = []
    for r in tf.robots:
        entry = {
            "id": r.robot_id.id,
            "x": int(r.pos.x * 1000),
            "y": int(r.pos.y * 1000),
            "theta": r.orientation,      # replay + motion 共用 (round なし)
        }
        if r.HasField("vel"):
            entry["vel_ms"] = math.hypot(r.vel.x, r.vel.y)
            entry["vel_x"] = r.vel.x
            entry["vel_y"] = r.vel.y
        if r.robot_id.team_color == _TRACKER_TEAM_YELLOW:
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
    # ゴール関連
    8:  'ゴール',
    39: 'ゴール(確認中)',
    42: '無効ゴール',
    43: 'PKキック失敗',
    # ファウル
    13: 'キーパー保持',
    14: 'ダブルタッチ',
    15: 'エリア内タッチ',
    17: 'オーバードリブル',
    18: 'ボール速度超過',
    19: 'エリア接近',
    20: '配置妨害',
    21: '衝突(引分)',
    22: '衝突',
    24: 'プッシング',
    26: 'ホールディング',
    27: '転倒',
    28: 'STOP中速度超過',
    29: 'ボール接近',
    31: 'マルチプルDF',
    32: 'マルチプルカード',
    34: 'マルチプルファウル',
    35: '非紳士的行為(軽)',
    36: '非紳士的行為(重)',
    47: '部品脱落',
    48: '交代回数超過',
    # ボール関連
    6:  'ボールアウト(タッチ)',
    7:  'ボールアウト(ゴール)',
    11: 'エイムレスキック',
    41: '境界線横断',
    # 管理・その他
    2:  '試合の停滞',
    3:  '配置失敗',
    5:  '配置成功',
    37: 'ロボット交代',
    38: 'ロボット数超過',
    44: 'チャレンジフラッグ',
    45: 'エマージェンシーストップ',
    46: 'チャレンジ処理完了',
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
    15: 'ゴール (Blue)',
    16: 'ボールプレースメント (Yellow)',
    17: 'ボールプレースメント (Blue)',
}

_HEATMAP_BIN_SIZE_MM = 100
_FIELD_X_MIN = -6000
_FIELD_Y_MIN = -4500
_HEATMAP_BINS_X = 120  # 12000 / 100
_HEATMAP_BINS_Y = 90   # 9000 / 100
_HEATMAP_BINS_X1 = _HEATMAP_BINS_X - 1
_HEATMAP_BINS_Y1 = _HEATMAP_BINS_Y - 1
REPLAY_FPS = 3

# 統計計算用定数
_STATS_MIN_DT_NS = int(5e6)       # 5ms未満のΔtはスキップ（ノイズ排除）
_STATS_MAX_DT_NS = int(500e6)     # 500ms超のΔtはスキップ（フレーム欠落）
_KICK_DETECT_THRESHOLD = 1.5      # m/s の速度増加でキック検出
_SPRINT_THRESHOLD = 2.0           # m/s 以上でスプリント判定
_SPRINT_COOLDOWN_NS = int(500e6)  # 同一ロボットのスプリント再検出間隔
_SHOT_SPEED_THRESHOLD_MS = 3.0    # シュート判定の最低ボール速度 (m/s)
_SHOT_COOLDOWN_NS = int(1500e6)   # 同一チームの連続シュート判定の最小間隔
_SHOT_MAX_ROBOT_DIST_MM = 600     # キック帰属の最大ロボット距離 (mm)


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
_MOTION_MIN_SAMPLES = 1000       # 動作限界推定に必要な最低サンプル数 (speed_hist の合計)
_MOTION_SPEED_MAX = 5.0          # 速度ヒストグラム最大値 (m/s)
_MOTION_SPEED_BIN = 0.1          # 速度ビン幅 (m/s)
_MOTION_ACCEL_MIN = -8.0         # 加速度ヒストグラム最小値 (m/s²)
_MOTION_ACCEL_MAX = 8.0          # 加速度ヒストグラム最大値 (m/s²)
_MOTION_ACCEL_BIN = 0.25         # 加速度ビン幅 (m/s²)
_MOTION_SUBSAMPLE = 3            # モーション加速度計算のサブサンプリング間隔 (Nフレームに1回)
_POSS_SUBSAMPLE = 3              # ポゼッション計算のサブサンプリング間隔
_AN_POS = int(_MOTION_ACCEL_MAX / _MOTION_ACCEL_BIN)  # 加速度/減速度ヒストグラムビン数 (0〜8)

# リプレイフレームから除外するロボットフィールド (モーション解析専用・JSONサイズ削減)
# theta_rad を theta に統合したため除外リストから外した
_REPLAY_FRAME_EXCLUDE = frozenset({"vel_x", "vel_y", "vel_ms"})


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


def _build_robot_team_stats(team: str, robot_stats: dict) -> dict:
    """_compute_match_stats の _team_stats を外部関数として切り出し。"""
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


def _hist_pct(hist: list[int], bin_start: float, bin_width: float, p: float) -> float:
    """ヒストグラムからパーセンタイル値を推定する。sorted() 不要で O(bins)。"""
    total = sum(hist)
    if total == 0:
        return 0.0
    target = total * p
    cumulative = 0
    for i, count in enumerate(hist):
        cumulative += count
        if cumulative >= target:
            return round(bin_start + (i + 0.5) * bin_width, 3)
    return round(bin_start + len(hist) * bin_width, 3)


def _build_motion_team_result(td: dict) -> dict:
    """_compute_motion_analysis の _build_team_result を外部関数として切り出し。"""
    n_speed = sum(td["speed_hist"])
    valid = n_speed >= _MOTION_MIN_SAMPLES
    limits: dict = {"sample_count": n_speed, "valid": valid}
    if valid:
        limits["velocity_limit"] = _hist_pct(td["speed_hist"], 0.0, _MOTION_SPEED_BIN, 0.995)
        limits["accel_limit"]    = _hist_pct(td["accel_hist"], 0.0, _MOTION_ACCEL_BIN, 0.75)
        limits["decel_limit"]    = _hist_pct(td["decel_hist"], 0.0, _MOTION_ACCEL_BIN, 0.95)
    return {
        "speed_histogram": {"bin_width": _MOTION_SPEED_BIN, "bins": td["speed_hist"]},
        "speed_accel_heatmap": {
            "speed_bin_width": _MOTION_SPEED_BIN, "speed_max": _MOTION_SPEED_MAX,
            "accel_bin_width": _MOTION_ACCEL_BIN, "accel_min": _MOTION_ACCEL_MIN,
            "accel_max": _MOTION_ACCEL_MAX,
            "data": [[sb, ab, cnt] for (sb, ab), cnt in td["sa_grid"].items()],
        },
        "directional_accel": {
            "bin_width": _MOTION_ACCEL_BIN, "accel_min": _MOTION_ACCEL_MIN,
            "accel_max": _MOTION_ACCEL_MAX,
            "data": [[axb, ayb, cnt] for (axb, ayb), cnt in td["da_grid"].items()],
        },
        "limits": limits,
    }


def _scan_has_tracker(data: bytes) -> bool:
    """ログにTrackerデータが含まれるか高速判定 (protobufパースなし・ヘッダーのみ走査)。"""
    offset = 16  # SSL_LOG_HEADER(12) + version(4)
    while offset + 16 <= len(data):
        msg_type = struct.unpack_from(">i", data, offset + 8)[0]
        msg_size = struct.unpack_from(">i", data, offset + 12)[0]
        if msg_type == MSG_TYPE_TRACKER:
            return True
        if msg_size < 0:
            break
        offset += 16 + msg_size
    return False


def extract_full_analysis(log_gz_bytes: bytes, filename: str = "") -> dict:
    """SSL game log (.log.gz) をフル解析。position_frames を蓄積せず1パスでストリーミング処理する。

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

    # TrackerとVisionが混在するログに備え、事前スキャンでmessage typeを確定
    use_tracker = _scan_has_tracker(data)
    _accept = {MSG_TYPE_TRACKER} if use_tracker else {MSG_TYPE_VISION_2010, MSG_TYPE_VISION_2014}

    # ストリーミング状態
    referee_snapshots: list[tuple[int, object]] = []
    first_frame_ns: int | None = None
    last_frame_ns: int = 0

    # ヒートマップ集計
    _ball_grid:   dict[tuple[int, int], int] = {}
    _yell_grid:   dict[tuple[int, int], int] = {}
    _blue_grid:   dict[tuple[int, int], int] = {}

    # マッチ統計集計
    _bmax = 0.0; _bsum = 0.0; _bcnt = 0; _bdist = 0.0; _bkick = 0
    _bpos = 0; _bneg = 0; _prev_bspd = 0.0
    _robot_stats: dict[tuple[str, int], dict] = {}
    _shot_counts: dict[str, int] = {"yellow": 0, "blue": 0}
    _last_shot_ns: dict[str, int] = {"yellow": -int(2e18), "blue": -int(2e18)}
    _pf: dict | None = None          # prev frame
    _pym: dict[int, dict] = {}       # prev yellow map
    _pbm: dict[int, dict] = {}       # prev blue map

    # モーション分析集計
    _SN = int(_MOTION_SPEED_MAX / _MOTION_SPEED_BIN)
    _AN = int((_MOTION_ACCEL_MAX - _MOTION_ACCEL_MIN) / _MOTION_ACCEL_BIN)
    def _new_td() -> dict:
        return {"speed_hist": [0] * _SN, "sa_grid": {}, "da_grid": {},
                "accel_hist": [0] * _AN_POS, "decel_hist": [0] * _AN_POS}
    _mt = {"yellow": _new_td(), "blue": _new_td()}
    _rh: dict[tuple[str, int], deque] = {}
    _motion_fc: int = 0   # モーション加速度計算のフレームカウンタ
    _poss_fc: int = 0     # ポゼッション計算のフレームカウンタ

    # リプレイサンプリング
    _riv = int(1e9 / REPLAY_FPS)
    _rnext: int | None = None
    _rprev: dict | None = None
    _replay: list[dict] = []

    # ポゼッション集計
    _PIV = int(5.0 * 1e9)
    _pw_start: int | None = None
    _pyc = 0; _ptc = 0
    _pts: list[float] = []; _prs: list[float] = []

    # ゴールシーン用循環バッファ (SCENE_DURATION_SEC + 余裕5秒分)
    _BUF_MAX = int((SCENE_DURATION_SEC + 5.0) * 150 + 1)
    _sbuf: deque[dict] = deque(maxlen=_BUF_MAX)
    # ゴールスナップショット: (goal_ts_ns, team_int, scored_by, y_after, b_after, frames)
    _gsnaps: list[tuple[int, int, str, int, int, list[dict]]] = []
    _pys = -1; _pbs = -1  # prev yellow/blue score

    def _on_frame(frame: dict) -> None:
        nonlocal first_frame_ns, last_frame_ns, _rnext, _rprev
        nonlocal _bmax, _bsum, _bcnt, _bdist, _bkick, _bpos, _bneg, _prev_bspd
        nonlocal _pf, _pym, _pbm, _pw_start, _pyc, _ptc
        nonlocal _motion_fc, _poss_fc

        t = frame["t_ns"]
        if first_frame_ns is None:
            first_frame_ns = t; _rnext = t; _pw_start = t
        last_frame_ns = t

        ball = frame.get("ball")
        yr = frame.get("robots_yellow", [])
        br = frame.get("robots_blue", [])

        # ヒートマップ
        if ball:
            bx = int((ball["x"] - _FIELD_X_MIN) / _HEATMAP_BIN_SIZE_MM)
            by = int((ball["y"] - _FIELD_Y_MIN) / _HEATMAP_BIN_SIZE_MM)
            if bx < 0: bx = 0
            elif bx > _HEATMAP_BINS_X1: bx = _HEATMAP_BINS_X1
            if by < 0: by = 0
            elif by > _HEATMAP_BINS_Y1: by = _HEATMAP_BINS_Y1
            _ball_grid[(bx, by)] = _ball_grid.get((bx, by), 0) + 1
        for r in yr:
            bx = int((r["x"] - _FIELD_X_MIN) / _HEATMAP_BIN_SIZE_MM)
            by = int((r["y"] - _FIELD_Y_MIN) / _HEATMAP_BIN_SIZE_MM)
            if bx < 0: bx = 0
            elif bx > _HEATMAP_BINS_X1: bx = _HEATMAP_BINS_X1
            if by < 0: by = 0
            elif by > _HEATMAP_BINS_Y1: by = _HEATMAP_BINS_Y1
            _yell_grid[(bx, by)] = _yell_grid.get((bx, by), 0) + 1
        for r in br:
            bx = int((r["x"] - _FIELD_X_MIN) / _HEATMAP_BIN_SIZE_MM)
            by = int((r["y"] - _FIELD_Y_MIN) / _HEATMAP_BIN_SIZE_MM)
            if bx < 0: bx = 0
            elif bx > _HEATMAP_BINS_X1: bx = _HEATMAP_BINS_X1
            if by < 0: by = 0
            elif by > _HEATMAP_BINS_Y1: by = _HEATMAP_BINS_Y1
            _blue_grid[(bx, by)] = _blue_grid.get((bx, by), 0) + 1

        # マッチ統計
        if ball:
            if ball.get("x", 0) > 0: _bpos += 1
            else: _bneg += 1
        if _pf is not None:
            dt = t - _pf["t_ns"]
            if _STATS_MIN_DT_NS <= dt <= _STATS_MAX_DT_NS:
                pb = _pf.get("ball")
                if ball and pb:
                    spd = ball.get("vel_ms")
                    if spd is not None:
                        if spd > _bmax: _bmax = spd
                        _bsum += spd; _bcnt += 1
                        _bdist += math.hypot(ball.get("x", 0) - pb.get("x", 0),
                                             ball.get("y", 0) - pb.get("y", 0))
                        if spd - _prev_bspd >= _KICK_DETECT_THRESHOLD:
                            _bkick += 1
                            if spd >= _SHOT_SPEED_THRESHOLD_MS:
                                bxk = ball.get("x", 0); byk = ball.get("y", 0)
                                md_y = min(((r["x"]-bxk)**2+(r["y"]-byk)**2 for r in _pym.values()), default=float("inf"))
                                md_b = min(((r["x"]-bxk)**2+(r["y"]-byk)**2 for r in _pbm.values()), default=float("inf"))
                                shot_team = None
                                if md_y < md_b and md_y <= _SHOT_MAX_ROBOT_DIST_MM**2:
                                    shot_team = "yellow"
                                elif md_b < md_y and md_b <= _SHOT_MAX_ROBOT_DIST_MM**2:
                                    shot_team = "blue"
                                if shot_team and t - _last_shot_ns[shot_team] >= _SHOT_COOLDOWN_NS:
                                    _shot_counts[shot_team] += 1
                                    _last_shot_ns[shot_team] = t
                        _prev_bspd = spd
                    else:
                        _prev_bspd = 0.0
                else:
                    _prev_bspd = 0.0
                for tk, cur, prev_map in (("yellow", yr, _pym), ("blue", br, _pbm)):
                    for r in cur:
                        rid = r["id"]; key = (tk, rid)
                        if key not in _robot_stats:
                            _robot_stats[key] = {"dist_mm": 0.0, "max_spd": 0.0,
                                                  "sprint_count": 0, "last_sprint_ns": -_SPRINT_COOLDOWN_NS}
                        pr = prev_map.get(rid)
                        if pr is None: continue
                        rspd = r.get("vel_ms")
                        if rspd is None: continue
                        rs = _robot_stats[key]
                        rs["dist_mm"] += math.hypot(r["x"] - pr["x"], r["y"] - pr["y"])
                        if rspd > rs["max_spd"]: rs["max_spd"] = rspd
                        if rspd >= _SPRINT_THRESHOLD and t - rs["last_sprint_ns"] >= _SPRINT_COOLDOWN_NS:
                            rs["sprint_count"] += 1; rs["last_sprint_ns"] = t
        _pf = frame
        _pym = {r["id"]: r for r in yr}
        _pbm = {r["id"]: r for r in br}

        # モーション分析
        _motion_fc += 1
        do_accel = (_motion_fc % _MOTION_SUBSAMPLE == 0)
        for tk, robots in (("yellow", yr), ("blue", br)):
            td = _mt[tk]
            for r in robots:
                spd = r.get("vel_ms")
                if spd is None: continue
                if spd >= _MOTION_MIN_SPEED_MS:
                    sb = min(int(spd / _MOTION_SPEED_BIN), _SN - 1)
                    td["speed_hist"][sb] += 1
                if not do_accel: continue
                vx = r.get("vel_x"); vy = r.get("vel_y")
                theta = r.get("theta", 0.0)
                if vx is None or vy is None: continue
                key = (tk, r["id"])
                if key not in _rh: _rh[key] = deque(maxlen=_MOTION_FRAME_OFFSET + 1)
                hist = _rh[key]; hist.append((t, vx, vy, spd, theta))
                if len(hist) < _MOTION_FRAME_OFFSET + 1: continue
                t0, vx0, vy0, spd0, th0 = hist[0]
                dt2 = t - t0
                if not (_STATS_MIN_DT_NS <= dt2 <= _STATS_MAX_DT_NS * 10): continue
                dt2s = dt2 / 1e9
                ax = (vx - vx0) / dt2s; ay = (vy - vy0) / dt2s
                amag = math.hypot(ax, ay)
                asig = amag if spd >= spd0 else -amag
                if asig > 0:
                    ab = min(int(asig / _MOTION_ACCEL_BIN), _AN_POS - 1)
                    td["accel_hist"][ab] += 1
                elif asig < 0:
                    db = min(int(-asig / _MOTION_ACCEL_BIN), _AN_POS - 1)
                    td["decel_hist"][db] += 1
                mspd = (spd + spd0) / 2.0
                if mspd >= _MOTION_MIN_SPEED_MS:
                    sb2 = min(int(mspd / _MOTION_SPEED_BIN), _SN - 1)
                    if _MOTION_ACCEL_MIN <= asig <= _MOTION_ACCEL_MAX:
                        ab2 = min(int((asig - _MOTION_ACCEL_MIN) / _MOTION_ACCEL_BIN), _AN - 1)
                        td["sa_grid"][(sb2, ab2)] = td["sa_grid"].get((sb2, ab2), 0) + 1
                thm = (theta + th0) / 2.0; ct = math.cos(thm); st = math.sin(thm)
                axl = ax * ct + ay * st; ayl = -ax * st + ay * ct
                if _MOTION_ACCEL_MIN <= axl <= _MOTION_ACCEL_MAX and \
                   _MOTION_ACCEL_MIN <= ayl <= _MOTION_ACCEL_MAX:
                    axb = min(int((axl - _MOTION_ACCEL_MIN) / _MOTION_ACCEL_BIN), _AN - 1)
                    ayb = min(int((ayl - _MOTION_ACCEL_MIN) / _MOTION_ACCEL_BIN), _AN - 1)
                    td["da_grid"][(axb, ayb)] = td["da_grid"].get((axb, ayb), 0) + 1

        # リプレイサンプリング (REPLAY_FPS 間隔で最近傍フレームを選択)
        assert _rnext is not None
        while _rnext <= t:
            best = frame
            if _rprev is not None and abs(_rprev["t_ns"] - _rnext) < abs(t - _rnext):
                best = _rprev
            _replay.append({
                "t_sec": round((_rnext - first_frame_ns) / 1e9, 2),
                "ball": best.get("ball"),
                "robots_yellow": [{k: v for k, v in r.items() if k not in _REPLAY_FRAME_EXCLUDE}
                                   for r in best.get("robots_yellow", [])],
                "robots_blue":   [{k: v for k, v in r.items() if k not in _REPLAY_FRAME_EXCLUDE}
                                   for r in best.get("robots_blue", [])],
            })
            _rnext += _riv
        _rprev = frame

        # ポゼッション集計 (5秒ウィンドウ、サブサンプリング)
        _poss_fc += 1
        assert _pw_start is not None
        while t >= _pw_start + _PIV:
            _pts.append(round((_pw_start - first_frame_ns) / 1e9, 1))
            _prs.append(round(_pyc / _ptc, 3) if _ptc > 0 else 0.5)
            _pyc = 0; _ptc = 0; _pw_start += _PIV
        if ball and _poss_fc % _POSS_SUBSAMPLE == 0:
            bxv = ball.get("x", 0); byv = ball.get("y", 0)
            md2 = float("inf"); cl = None
            for r in yr:
                d2 = (r["x"] - bxv) ** 2 + (r["y"] - byv) ** 2
                if d2 < md2: md2 = d2; cl = "yellow"
            for r in br:
                d2 = (r["x"] - bxv) ** 2 + (r["y"] - byv) ** 2
                if d2 < md2: md2 = d2; cl = "blue"
            if cl:
                _ptc += 1
                if cl == "yellow": _pyc += 1

        # ゴールシーン用循環バッファ
        _sbuf.append(frame)

    def _on_referee(ts: int, ref) -> None:
        nonlocal _pys, _pbs
        referee_snapshots.append((ts, ref))
        try:
            y = int(ref.yellow.score); b = int(ref.blue.score)
        except Exception:
            return
        if _pys < 0:
            _pys = y; _pbs = b; return
        scored_by = None; team_int = None
        if y > _pys:   scored_by = "yellow"; team_int = _TEAM_YELLOW
        elif b > _pbs: scored_by = "blue";   team_int = _TEAM_BLUE
        if scored_by and _sbuf:
            _gsnaps.append((ts, team_int, scored_by, y, b, list(_sbuf)))
        _pys = y; _pbs = b

    # メインパース (1パス・position_frames を蓄積しない)
    for timestamp_ns, msg_type, raw in _iter_messages(data):
        if msg_type == MSG_TYPE_REFEREE:
            try:
                ref = ssl_gc_referee_message_pb2.Referee()
                ref.ParseFromString(raw)
                _on_referee(timestamp_ns, ref)
            except Exception:
                pass
        elif msg_type in _accept:
            try:
                if use_tracker:
                    wrapper = TrackerWrapperPacket()
                    wrapper.ParseFromString(raw)
                    frame = _tracker_to_frame(timestamp_ns, wrapper)
                else:
                    wrapper = ssl_vision_wrapper_pb2.SSL_WrapperPacket()
                    wrapper.ParseFromString(raw)
                    frame = _detection_to_frame(timestamp_ns, wrapper)
                if frame:
                    _on_frame(frame)
            except Exception:
                pass

    del data  # 展開済みバイト列を解放

    referee_snapshots = _filter_referee_terminal_resets(referee_snapshots)

    start_ns = first_frame_ns or (referee_snapshots[0][0] if referee_snapshots else 0)
    end_ns   = last_frame_ns  or (referee_snapshots[-1][0] if referee_snapshots else 0)
    duration_sec = round((end_ns - start_ns) / 1e9, 1)

    team_yellow, team_blue = _extract_team_names(referee_snapshots, filename)
    final_score = {"yellow": 0, "blue": 0}
    if referee_snapshots:
        ys, bs = _score_from_ref(referee_snapshots[-1][1])
        final_score = {"yellow": ys, "blue": bs}

    base = _base_from_log_filename(filename)
    match_id = base.replace(" ", "_").replace("/", "_").replace("\\", "_")

    # ヒートマップ出力
    def _to_list(g: dict) -> list[list[int]]:
        return [[bx, by, cnt] for (bx, by), cnt in g.items()]

    # マッチ統計出力
    bt = _bpos + _bneg
    match_stats = {
        "sprint_threshold_ms": _SPRINT_THRESHOLD,
        "ball": {
            "max_speed_ms": round(_bmax, 2),
            "avg_speed_ms": round(_bsum / _bcnt if _bcnt > 0 else 0.0, 2),
            "total_distance_m": round(_bdist / 1000.0, 1),
            "kick_count": _bkick,
            "territory": {
                "positive_pct": round(_bpos / bt * 100, 1) if bt > 0 else 50.0,
                "negative_pct": round(_bneg / bt * 100, 1) if bt > 0 else 50.0,
            },
        },
        "robots": {
            "yellow": _build_robot_team_stats("yellow", _robot_stats),
            "blue":   _build_robot_team_stats("blue",   _robot_stats),
        },
    }

    # ゴールシーン出力 (循環バッファのスナップショットから抽出)
    possible_goals = _collect_possible_goals(referee_snapshots)
    goal_scenes: list[dict] = []
    for gi, (goal_ts, team_int, scored_by, y_aft, b_aft, frames_snap) in enumerate(_gsnaps):
        scene_end = _find_possible_goal_time(possible_goals, goal_ts, team_int)
        scene_start = scene_end - int(SCENE_DURATION_SEC * 1e9)
        raw_sc = [f for f in frames_snap if scene_start <= f["t_ns"] <= scene_end]
        if raw_sc:
            goal_scenes.append({
                "goal_index": gi,
                "scored_by": scored_by,
                "score_after": {"yellow": int(y_aft), "blue": int(b_aft)},
                "duration_sec": SCENE_DURATION_SEC,
                "fps": OUTPUT_FPS,
                "frames": _downsample_frames(raw_sc, scene_end, SCENE_DURATION_SEC, OUTPUT_FPS),
            })

    # ポゼッション出力 (最終ウィンドウのフラッシュ)
    if _ptc > 0 and _pw_start is not None and first_frame_ns is not None:
        _pts.append(round((_pw_start - first_frame_ns) / 1e9, 1))
        _prs.append(round(_pyc / _ptc, 3))

    events = _extract_events_timeline(referee_snapshots, start_ns)

    # ボールプレースメント統計
    _placement: dict[str, dict[str, int]] = {
        "yellow": {"succeeded": 0, "failed": 0},
        "blue":   {"succeeded": 0, "failed": 0},
    }
    for _ev in events:
        _tm = _ev["by_team"]
        if _tm in _placement:
            if _ev["type"] == 5:   # PLACEMENT_SUCCEEDED
                _placement[_tm]["succeeded"] += 1
            elif _ev["type"] == 3: # PLACEMENT_FAILED
                _placement[_tm]["failed"] += 1

    def _prate(s: dict) -> float | None:
        total = s["succeeded"] + s["failed"]
        return round(s["succeeded"] / total * 100, 1) if total > 0 else None

    team_stats = {
        "yellow": {
            "goals": final_score["yellow"],
            "shots": _shot_counts["yellow"],
            "placement_succeeded": _placement["yellow"]["succeeded"],
            "placement_failed":    _placement["yellow"]["failed"],
            "placement_success_rate": _prate(_placement["yellow"]),
        },
        "blue": {
            "goals": final_score["blue"],
            "shots": _shot_counts["blue"],
            "placement_succeeded": _placement["blue"]["succeeded"],
            "placement_failed":    _placement["blue"]["failed"],
            "placement_success_rate": _prate(_placement["blue"]),
        },
    }

    return {
        "meta": {
            "id": match_id,
            "analysis_version": ANALYSIS_VERSION,
            "filename": os.path.basename(filename),
            "teams": {"yellow": team_yellow, "blue": team_blue},
            "final_score": final_score,
            "duration_sec": duration_sec,
            "team_stats": team_stats,
        },
        "team_stats": team_stats,
        "match_stats": match_stats,
        "motion_analysis": {
            "yellow": _build_motion_team_result(_mt["yellow"]),
            "blue":   _build_motion_team_result(_mt["blue"]),
        },
        "replay_frames": _replay,
        "ball_heatmap":  _to_list(_ball_grid),
        "robot_heatmaps": {"yellow": _to_list(_yell_grid), "blue": _to_list(_blue_grid)},
        "goal_scenes": goal_scenes,
        "events": events,
        "possession": {"timestamps": _pts, "yellow_ratio": _prs},
        "score_timeline": _extract_score_timeline(referee_snapshots, start_ns),
        "referee_commands": _extract_referee_commands(referee_snapshots, start_ns),
    }
