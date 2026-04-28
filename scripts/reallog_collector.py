"""実試合ログ収集・解析スクリプト。

Google Drive の公開フォルダから .log.gz ファイルをダウンロードし、
ssl_log_parser.extract_full_analysis() でフル解析して JSON を出力する。

出力:
  public/analysis-data/{id}.json  — 個別試合データ
  public/analysis-folders/{id}.json — フォルダ別一覧メタデータ
  public/analysis-index.json      — 一覧メタデータ
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import re
import sys

import ssl_log_parser

GDRIVE_FOLDER_ID = "1Z_kMspBYE7Cj15tJSIKDWFPdfwco7nED"

# スクリプトはリポジトリルートから実行されることを前提とするが、
# scripts/ ディレクトリから実行された場合でも正しく動作するよう CWD を基準にする
_REPO_ROOT = pathlib.Path(os.getcwd())
CACHE_DIR = _REPO_ROOT / "cache" / "reallog"
OUTPUT_DATA_DIR = _REPO_ROOT / "public" / "analysis-data"
OUTPUT_FOLDER_INDEX_DIR = _REPO_ROOT / "public" / "analysis-folders"
OUTPUT_INDEX = _REPO_ROOT / "public" / "analysis-index.json"


# ---------------------------------------------------------------------------
# 引数解析
# ---------------------------------------------------------------------------
parser = argparse.ArgumentParser(description="Download and analyze real game logs from Google Drive.")
parser.add_argument(
    "--incremental",
    action="store_true",
    help="インクリメンタルモード: 既存の解析済みIDはスキップ",
)
parser.add_argument(
    "--folder-id",
    default=GDRIVE_FOLDER_ID,
    help="Google Drive フォルダID (デフォルト: %(default)s)",
)
args = parser.parse_args()

# ---------------------------------------------------------------------------
# ディレクトリ準備
# ---------------------------------------------------------------------------
CACHE_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DATA_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FOLDER_INDEX_DIR.mkdir(parents=True, exist_ok=True)



def _relative_log_path(log_path: pathlib.Path) -> pathlib.Path:
    """CACHE_DIR からの相対パスを返す。失敗時はファイル名のみを返す。"""
    try:
        return log_path.relative_to(CACHE_DIR)
    except ValueError:
        return pathlib.Path(log_path.name)


def _drive_key(path: pathlib.Path | str) -> str:
    """Drive/gdown 由来のパスを POSIX 形式の相対キーに正規化する。"""
    return pathlib.PurePosixPath(str(path).replace("\\", "/")).as_posix()


def _match_id_from_relative_path(rel_path: pathlib.Path) -> str:
    """ログの相対パスから衝突しにくい JSON ID を生成する。

    既存互換のためトップレベルのログは従来どおりファイル名ベースにする。
    サブフォルダ配下のログは相対パス由来の短い hash を付け、同名ログの衝突を避ける。
    """
    name = rel_path.name
    if name.endswith(".log.gz"):
        base = name[: -len(".log.gz")]
    else:
        base = rel_path.stem
        if base.endswith(".log"):
            base = base[:-4]

    def clean(part: str) -> str:
        return part.replace(" ", "_").replace("/", "_").replace("\\", "_")

    if rel_path.parent == pathlib.Path("."):
        return clean(base)

    path_parts = [clean(part) for part in rel_path.parent.parts] + [clean(base)]
    path_hash = hashlib.sha1(_drive_key(rel_path).encode("utf-8")).hexdigest()[:8]
    return f"{'_'.join(path_parts)}_{path_hash}"


def _base_from_log_filename(filename: str) -> str:
    if filename.endswith(".log.gz"):
        return filename[: -len(".log.gz")]
    base = pathlib.Path(filename).stem
    if base.endswith(".log"):
        return base[:-4]
    return base.replace(" ", "_").replace("/", "_").replace("\\", "_")


def _clean_id_part(part: str) -> str:
    return part.replace(" ", "_").replace("/", "_").replace("\\", "_")


def _legacy_match_ids_from_filename(filename: str) -> list[str]:
    """フォルダ分割前のファイル名ベース ID 候補を返す。既存 artifact の移行用。"""
    base = _base_from_log_filename(filename)
    bases = [base]

    without_ssl_suffix = re.sub(r"_SSL\d+$", "", base)
    if without_ssl_suffix != base:
        bases.append(without_ssl_suffix)

    ids: list[str] = []
    for candidate in bases:
        legacy_id = _clean_id_part(candidate)
        if legacy_id not in ids:
            ids.append(legacy_id)
    return ids


def _folder_id_from_path(folder_path: str) -> str:
    """フォルダ相対パスから URL/ファイル名に使える安定 ID を生成する。"""
    if not folder_path:
        return "root"

    folder_name = pathlib.PurePosixPath(folder_path).name
    slug = re.sub(r"[^A-Za-z0-9]+", "-", folder_name).strip("-").lower()
    if not slug:
        slug = "folder"
    path_hash = hashlib.sha1(folder_path.encode("utf-8")).hexdigest()[:8]
    return f"{slug}-{path_hash}"


def _folder_label(folder_name: str) -> str:
    return folder_name or "未分類"


def _folder_meta_from_relative_path(rel_path: pathlib.Path) -> tuple[str, str, str]:
    """ログのフォルダ ID、直接の親フォルダ名、フォルダ相対パスを返す。"""
    if rel_path.parent == pathlib.Path("."):
        return "root", "", ""
    folder_path = _drive_key(rel_path.parent)
    return _folder_id_from_path(folder_path), rel_path.parent.name, folder_path


def _folder_summary(folder_id: str, matches: list[dict]) -> dict:
    first = matches[0]
    total_goals = sum(
        (m.get("final_score", {}).get("yellow") or 0)
        + (m.get("final_score", {}).get("blue") or 0)
        for m in matches
    )
    avg_duration_sec = (
        sum(m.get("duration_sec") or 0 for m in matches) / len(matches)
        if matches
        else 0
    )
    return {
        "id": folder_id,
        "name": _folder_label(first.get("gdrive_folder", "")),
        "path": first.get("gdrive_folder_path", ""),
        "match_count": len(matches),
        "total_goals": total_goals,
        "avg_duration_sec": round(avg_duration_sec, 1),
    }


def download_folder_and_get_ids(folder_id: str) -> tuple[list[pathlib.Path], dict[str, str]]:
    """フォルダをダウンロードし、ファイルパス一覧と {relative_path: file_id} を返す。"""

    try:
        import gdown
    except ImportError:
        print("ERROR: gdown がインストールされていません。pip install gdown を実行してください。")
        sys.exit(1)

    print(f"Google Drive フォルダ {folder_id} をダウンロード中...")

    gdrive_files: dict[str, str] = {}
    try:
        planned_files = gdown.download_folder(
            id=folder_id,
            output=str(CACHE_DIR),
            quiet=False,
            use_cookies=False,
            skip_download=True,
        )
    except Exception as e:
        print(f"フォルダ構造取得失敗: {e}")
        planned_files = []

    for planned in planned_files or []:
        file_id = getattr(planned, "id", "")
        rel_path = _drive_key(getattr(planned, "path", ""))
        if not file_id or not rel_path.endswith(".log.gz"):
            continue
        gdrive_files[rel_path] = file_id

    if gdrive_files:
        print(f"  {len(gdrive_files)} 件のファイルIDを取得")
    else:
        print("  ファイルID取得失敗 (ダウンロードリンクなし)")

    try:
        downloaded_files = gdown.download_folder(
            id=folder_id,
            output=str(CACHE_DIR),
            quiet=False,
            use_cookies=False,
        )
    except Exception as e:
        print(f"フォルダダウンロード失敗: {e}")
        downloaded_files = []

    log_files = [
        pathlib.Path(path)
        for path in downloaded_files or []
        if str(path).endswith(".log.gz")
    ]

    if not log_files:
        log_files = sorted(CACHE_DIR.rglob("*.log.gz"))

    return sorted(log_files), gdrive_files


def _load_meta_from_json(
    in_path: pathlib.Path,
    meta_updates: dict,
    write_path: pathlib.Path | None = None,
) -> dict | None:
    """JSON を読み込み、必要なら meta を更新して返す。失敗時または古いバージョンは None。"""
    try:
        with open(in_path, "r", encoding="utf-8") as f:
            d = json.load(f)
        meta = d["meta"]
        cached_version = meta.get("analysis_version")
        if not isinstance(cached_version, int) or cached_version < ssl_log_parser.ANALYSIS_VERSION:
            return None
        changed = False
        for key, value in meta_updates.items():
            if value is not None and meta.get(key) != value:
                meta[key] = value
                changed = True
        out_path = write_path or in_path
        if changed or out_path != in_path:
            with open(out_path, "w", encoding="utf-8") as fw:
                json.dump(d, fw, ensure_ascii=False, separators=(",", ":"))
        return meta
    except Exception:
        return None


def _load_cached_meta(out_path: pathlib.Path, legacy_out_paths: list[pathlib.Path], meta_updates: dict) -> dict | None:
    """現行 ID の JSON、なければ旧 ID の JSON を読み込んで現行 ID に移行する。"""
    if out_path.exists():
        return _load_meta_from_json(out_path, meta_updates)
    for legacy_out_path in legacy_out_paths:
        if legacy_out_path != out_path and legacy_out_path.exists():
            return _load_meta_from_json(legacy_out_path, meta_updates, write_path=out_path)
    return None


# ---------------------------------------------------------------------------
# インクリメンタルモード: 既存の解析済みIDを読み込む
# ---------------------------------------------------------------------------
existing_ids: set[str] = set()
if args.incremental and OUTPUT_INDEX.exists():
    try:
        with open(OUTPUT_INDEX, "r", encoding="utf-8") as f:
            prev = json.load(f)
        existing_ids = {m["id"] for m in prev.get("matches", [])}
        print(f"インクリメンタルモード: 既存 {len(existing_ids)} 試合をスキップ")
    except Exception as e:
        print(f"既存インデックス読み込み失敗: {e}")

# ---------------------------------------------------------------------------
# フォルダをダウンロードしてファイル一覧と file_id マッピングを取得
# ---------------------------------------------------------------------------
log_files, gdrive_files = download_folder_and_get_ids(args.folder_id)

if not log_files:
    # フォールバック: キャッシュディレクトリにある既存ファイルのみ処理
    log_files = sorted(CACHE_DIR.rglob("*.log.gz"))
    if not log_files:
        print("処理対象のログファイルが見つかりません。")
        sys.exit(0)

print(f"\n{len(log_files)} 件のログファイルを処理します。")

# ---------------------------------------------------------------------------
# 各ログを解析して JSON 出力
# ---------------------------------------------------------------------------
matches_meta: list[dict] = []
processed = 0
skipped = 0
errors = 0

for log_path in log_files:
    filename = log_path.name
    rel_path = _relative_log_path(log_path)
    rel_key = _drive_key(rel_path)
    match_id = _match_id_from_relative_path(rel_path)
    legacy_match_ids = _legacy_match_ids_from_filename(filename)
    folder_id, folder_name, folder_path = _folder_meta_from_relative_path(rel_path)

    file_id = gdrive_files.get(rel_key)
    gdrive_url = f"https://drive.google.com/file/d/{file_id}/view" if file_id else None
    meta_updates = {
        "id": match_id,
        "filename": filename,
        "gdrive_folder_id": folder_id,
        "gdrive_folder": folder_name,
        "gdrive_folder_path": folder_path,
        "gdrive_url": gdrive_url,
    }
    out_path = OUTPUT_DATA_DIR / f"{match_id}.json"
    legacy_out_paths = [
        OUTPUT_DATA_DIR / f"{legacy_match_id}.json"
        for legacy_match_id in legacy_match_ids
    ]

    if args.incremental and (
        match_id in existing_ids or any(legacy_match_id in existing_ids for legacy_match_id in legacy_match_ids)
    ):
        print(f"スキップ (既存): {rel_key}")
        meta = _load_cached_meta(out_path, legacy_out_paths, meta_updates)
        if meta:
            matches_meta.append(meta)
            skipped += 1
            continue
        print("  既存メタデータのキャッシュが見つからないため再解析します")

    # JSON キャッシュが既に存在する場合はスキップ
    meta = _load_cached_meta(out_path, legacy_out_paths, meta_updates)
    if meta and not args.incremental:
        print(f"解析済みキャッシュ使用: {rel_key}")
        matches_meta.append(meta)
        skipped += 1
        continue

    print(f"解析中: {rel_key}")
    try:
        log_gz_bytes = log_path.read_bytes()
        analysis = ssl_log_parser.extract_full_analysis(log_gz_bytes, filename=filename)
    except Exception as e:
        print(f"  解析失敗: {e}")
        errors += 1
        continue

    analysis["meta"].update({k: v for k, v in meta_updates.items() if v is not None})

    # 個別 JSON を出力
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(analysis, f, ensure_ascii=False, separators=(",", ":"))
    print(
        f"  完了: {analysis['meta']['teams']['yellow']} {analysis['meta']['final_score']['yellow']}"
        f" - {analysis['meta']['final_score']['blue']} {analysis['meta']['teams']['blue']}"
        f"  ({analysis['meta']['duration_sec']:.0f}s, "
        f"{len(analysis['replay_frames'])}フレーム, "
        f"{len(analysis['goal_scenes'])}ゴール)"
    )

    matches_meta.append(analysis["meta"])
    processed += 1

# ---------------------------------------------------------------------------
# インデックス JSON を出力
# ---------------------------------------------------------------------------
# 日付でソート (ファイル名に日付が含まれる場合)
matches_meta.sort(key=lambda m: m.get("filename", ""), reverse=True)

folder_groups: dict[str, list[dict]] = {}
folder_order: list[str] = []
for meta in matches_meta:
    folder_id = meta.get("gdrive_folder_id") or "root"
    if folder_id not in folder_groups:
        folder_groups[folder_id] = []
        folder_order.append(folder_id)
    folder_groups[folder_id].append(meta)

folder_summaries = [
    _folder_summary(folder_id, folder_groups[folder_id])
    for folder_id in folder_order
]

for old_index in OUTPUT_FOLDER_INDEX_DIR.glob("*.json"):
    old_index.unlink()

for folder in folder_summaries:
    folder_matches = folder_groups[folder["id"]]
    out_path = OUTPUT_FOLDER_INDEX_DIR / f"{folder['id']}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(
            {"folder": folder, "matches": folder_matches},
            f,
            ensure_ascii=False,
            indent=2,
        )

with open(OUTPUT_INDEX, "w", encoding="utf-8") as f:
    json.dump(
        {"matches": matches_meta, "folders": folder_summaries},
        f,
        ensure_ascii=False,
        indent=2,
    )

print(
    f"\n完了: 新規解析 {processed} 試合, スキップ {skipped} 試合, エラー {errors} 試合"
)
print(f"インデックス出力: {OUTPUT_INDEX} ({len(matches_meta)} 試合)")
print(f"フォルダ別インデックス出力: {OUTPUT_FOLDER_INDEX_DIR} ({len(folder_summaries)} フォルダ)")
