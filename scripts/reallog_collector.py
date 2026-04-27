"""実試合ログ収集・解析スクリプト。

Google Drive の公開フォルダから .log.gz ファイルをダウンロードし、
ssl_log_parser.extract_full_analysis() でフル解析して JSON を出力する。

出力:
  public/analysis-data/{id}.json  — 個別試合データ
  public/analysis-index.json      — 一覧メタデータ
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import sys

import ssl_log_parser

GDRIVE_FOLDER_ID = "1Z_kMspBYE7Cj15tJSIKDWFPdfwco7nED"

# スクリプトはリポジトリルートから実行されることを前提とするが、
# scripts/ ディレクトリから実行された場合でも正しく動作するよう CWD を基準にする
_REPO_ROOT = pathlib.Path(os.getcwd())
CACHE_DIR = _REPO_ROOT / "cache" / "reallog"
OUTPUT_DATA_DIR = _REPO_ROOT / "public" / "analysis-data"
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


def _folder_meta_from_relative_path(rel_path: pathlib.Path) -> tuple[str, str]:
    """ログの直接の親フォルダ名とフォルダ相対パスを返す。"""
    if rel_path.parent == pathlib.Path("."):
        return "", ""
    return rel_path.parent.name, _drive_key(rel_path.parent)


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


def _load_meta_from_json(out_path: pathlib.Path, meta_updates: dict) -> dict | None:
    """JSON を読み込み、必要なら meta を更新して返す。失敗時は None。"""
    try:
        with open(out_path, "r", encoding="utf-8") as f:
            d = json.load(f)
        meta = d["meta"]
        changed = False
        for key, value in meta_updates.items():
            if value is not None and meta.get(key) != value:
                meta[key] = value
                changed = True
        if changed:
            with open(out_path, "w", encoding="utf-8") as fw:
                json.dump(d, fw, ensure_ascii=False, separators=(",", ":"))
        return meta
    except Exception:
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
    folder_name, folder_path = _folder_meta_from_relative_path(rel_path)

    file_id = gdrive_files.get(rel_key)
    gdrive_url = f"https://drive.google.com/file/d/{file_id}/view" if file_id else None
    meta_updates = {
        "id": match_id,
        "gdrive_folder": folder_name,
        "gdrive_folder_path": folder_path,
        "gdrive_url": gdrive_url,
    }

    if args.incremental and match_id in existing_ids:
        print(f"スキップ (既存): {rel_key}")
        out_path = OUTPUT_DATA_DIR / f"{match_id}.json"
        if out_path.exists():
            meta = _load_meta_from_json(out_path, meta_updates)
            if meta:
                matches_meta.append(meta)
        skipped += 1
        continue

    # JSON キャッシュが既に存在する場合はスキップ
    out_path = OUTPUT_DATA_DIR / f"{match_id}.json"
    if out_path.exists() and not args.incremental:
        print(f"解析済みキャッシュ使用: {rel_key}")
        meta = _load_meta_from_json(out_path, meta_updates)
        if meta:
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

with open(OUTPUT_INDEX, "w", encoding="utf-8") as f:
    json.dump({"matches": matches_meta}, f, ensure_ascii=False, indent=2)

print(
    f"\n完了: 新規解析 {processed} 試合, スキップ {skipped} 試合, エラー {errors} 試合"
)
print(f"インデックス出力: {OUTPUT_INDEX} ({len(matches_meta)} 試合)")
