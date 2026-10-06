"""纪元（epoch）存储层：工作区、记录、页面围栏与迁移状态机。

设计约束（对应野外实验站离线记录页的升级要求）：

- 读取永远只来自工作区指针指向的「已发布」纪元；候选纪元从不对外提供读取，
  因此读结果只能是完整旧纪元或完整新纪元。
- 迁移状态存放在工作区行上（唯一槽位），并发迁移不可能创建第二个候选纪元。
- 每次迁移拥有全局唯一的 migration_id，其冻结清单（manifest）按迁移绑定到
  「本工作区 + 发起时的源纪元」：多个工作区升级到同一目标版本也各自携带自己
  发起时冻结的完整记录，绝不共享清单。
- 迁移协议：复制到候选纪元 -> 校验 -> 单事务原子发布（切换指针 + 作废旧纪元
  + 失效旧页面一次提交）。发布前后，旧页面的迟到保存一律被拒绝并提示重新载入。
- 页面在复制/校验/发布之间关闭时，依据持久化阶段恢复：
  复制中 -> 安全回收候选；校验中 -> 保留同一候选等待续用；发布中 -> 立即完成发布。
- 启动恢复还会收敛已发布纪元：若新纪元冻结段与本工作区源纪元快照不一致（例如
  旧版本缺陷使工作区误读到他区清单），按源纪元冻结快照校正，顺序/内容/时间
  原样保留，迁移后新增记录顺延不丢失；纪元不回退，旧页面不恢复写入资格。
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

# 迁移阶段：idle -> copying -> validating -> publishing -> published
# 异常终态：failed（校验失败）、aborted（复制中断后候选被回收）
ACTIVE_PHASES = ("copying", "validating", "publishing")
TERMINAL_PHASES = ("idle", "published", "failed", "aborted")

SCHEMA = """
CREATE TABLE IF NOT EXISTS workspaces (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  created_at TEXT NOT NULL,
  current_epoch_id TEXT,
  -- 迁移唯一槽位：同一工作区至多一个进行中的迁移，天然杜绝第二个候选纪元
  migration_phase TEXT NOT NULL DEFAULT 'idle',
  migration_id TEXT,                    -- 本次迁移的全局唯一标识（绑定冻结清单）
  migration_target_version TEXT,
  migration_candidate_epoch_id TEXT,
  migration_source_epoch_id TEXT,
  migration_owner_page_id TEXT,
  migration_copied INTEGER NOT NULL DEFAULT 0,
  migration_total INTEGER NOT NULL DEFAULT 0,
  migration_started_at TEXT,
  migration_updated_at TEXT,
  migration_error TEXT,
  published_frozen_count INTEGER              -- 已发布纪元冻结段长度；NULL=遗留发布待对账
);
CREATE TABLE IF NOT EXISTS epochs (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  number INTEGER NOT NULL,
  version TEXT NOT NULL,
  kind TEXT NOT NULL,              -- published | candidate | superseded
  created_at TEXT NOT NULL,
  UNIQUE (workspace_id, number)
);
CREATE TABLE IF NOT EXISTS records (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  epoch_id TEXT NOT NULL,
  seq INTEGER NOT NULL,
  content TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_records_epoch ON records(epoch_id, seq);
CREATE TABLE IF NOT EXISTS migration_manifests (
  -- 每次迁移一行：migration_id 全局唯一，清单只属于发起时的（工作区, 源纪元）。
  -- 同一目标版本可被多个工作区并行使用，彼此不共享冻结记录。
  migration_id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  target_version TEXT NOT NULL,
  source_epoch_id TEXT NOT NULL,
  record_count INTEGER NOT NULL,
  record_checksum TEXT NOT NULL,
  records_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE (workspace_id, target_version)
);
CREATE TABLE IF NOT EXISTS pages (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  epoch_id TEXT NOT NULL,          -- 页面持有的围栏：打开时锁定的纪元
  state TEXT NOT NULL,             -- active | invalidated | closed
  created_at TEXT NOT NULL,
  last_seen TEXT NOT NULL,
  closed_at TEXT,
  invalidated_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_pages_ws ON pages(workspace_id);
"""


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


class ApiError(Exception):
    """携带 HTTP 状态码与业务错误码的异常，extra 会并入错误响应体。"""

    def __init__(self, status: int, code: str, message: str, extra: dict | None = None):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.extra = extra or {}


class Store:
    def __init__(self, path: str, page_ttl_seconds: int = 45):
        self.path = path
        self.page_ttl = timedelta(seconds=page_ttl_seconds)
        self._lock = threading.RLock()
        if path != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self.conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA busy_timeout=5000")
        with self._lock:
            self.conn.executescript(SCHEMA)
            self._migrate_schema_locked()
        # 进程重启 = 所有页面连接已断开：关闭遗留活动页面并恢复未完成的迁移
        self.recover_all()

    def close(self):
        with self._lock:
            self.conn.close()

    # ---------------------------------------------------------------- 基础工具

    @contextmanager
    def _tx(self):
        """BEGIN IMMEDIATE：立即取得写锁，事务内的 检查-再写入 不会被并发穿插。"""
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield
        except Exception:
            self.conn.execute("ROLLBACK")
            raise
        else:
            self.conn.execute("COMMIT")

    def _ws(self, ws_id: str) -> sqlite3.Row:
        row = self.conn.execute("SELECT * FROM workspaces WHERE id=?", (ws_id,)).fetchone()
        if row is None:
            raise ApiError(404, "workspace_not_found", "工作区不存在")
        return row

    def _page(self, page_id: str | None) -> sqlite3.Row | None:
        if not page_id:
            return None
        return self.conn.execute("SELECT * FROM pages WHERE id=?", (page_id,)).fetchone()

    def _epoch(self, epoch_id: str | None) -> sqlite3.Row | None:
        if not epoch_id:
            return None
        return self.conn.execute("SELECT * FROM epochs WHERE id=?", (epoch_id,)).fetchone()

    def _checksum(self, epoch_id: str) -> tuple[int, str]:
        """纪元的 (记录数, 内容摘要)，用于复制后校验与发布前复核。"""
        rows = self.conn.execute(
            "SELECT seq, content FROM records WHERE epoch_id=? ORDER BY seq", (epoch_id,)
        ).fetchall()
        h = hashlib.sha256()
        for r in rows:
            h.update(f"{r['seq']}|".encode())
            h.update(r["content"].encode())
            h.update(b"\n")
        return (len(rows), h.hexdigest())

    def _snapshot_records(self, epoch_id: str) -> list[dict]:
        rows = self.conn.execute(
            "SELECT seq, content, created_at FROM records WHERE epoch_id=? ORDER BY seq", (epoch_id,)
        ).fetchall()
        return [dict(row) for row in rows]

    # ------------------------------------------------- 旧版本库结构升级与数据收敛

    def _column_names_locked(self, table: str) -> set[str]:
        return {r["name"] for r in self.conn.execute(
            f"PRAGMA table_info({table})").fetchall()}

    def _migrate_schema_locked(self):
        """把旧版本库升级到当前结构。

        旧表 migration_manifests 仅按 target_version 存一行，多个工作区迁移到
        同一版本时共享了首个工作区的冻结清单；workspaces 也没有 migration_id。
        升级时按（工作区, 目标版本）重建清单：能与现存源纪元对上的保留，对不上
        的（已被跨区污染的发布）在 recover_all 中依据源纪元收敛。
        """
        ws_cols = self._column_names_locked("workspaces")
        if "migration_id" not in ws_cols:
            self.conn.execute("ALTER TABLE workspaces ADD COLUMN migration_id TEXT")
        if "published_frozen_count" not in ws_cols:
            # 遗留已发布纪元一律置 NULL：等待与本工作区源冻结快照对账收敛
            self.conn.execute(
                "ALTER TABLE workspaces ADD COLUMN published_frozen_count INTEGER")

        mcols = self._column_names_locked("migration_manifests")
        if "migration_id" in mcols and "workspace_id" in mcols:
            return  # 已是当前结构

        old_rows = self.conn.execute(
            "SELECT target_version, source_epoch_id, record_count, record_checksum, "
            "records_json, created_at FROM migration_manifests"
        ).fetchall()
        self.conn.execute("DROP TABLE migration_manifests")
        self.conn.execute(
            "CREATE TABLE migration_manifests ("
            "migration_id TEXT PRIMARY KEY, workspace_id TEXT NOT NULL, "
            "target_version TEXT NOT NULL, source_epoch_id TEXT NOT NULL, "
            "record_count INTEGER NOT NULL, record_checksum TEXT NOT NULL, "
            "records_json TEXT NOT NULL, created_at TEXT NOT NULL, "
            "UNIQUE (workspace_id, target_version))")
        ws_rows = self.conn.execute(
            "SELECT id, migration_phase, migration_target_version AS tv, "
            "migration_source_epoch_id AS src, migration_candidate_epoch_id AS cand "
            "FROM workspaces").fetchall()
        by_target: dict[str, list[sqlite3.Row]] = {}
        for w in ws_rows:
            if w["tv"]:
                by_target.setdefault(w["tv"], []).append(w)
        for m in old_rows:
            tv = m["target_version"]
            owners = by_target.get(tv, [])
            # 源纪元与清单吻合的工作区是这份冻结清单的真正主人
            owner = next((w for w in owners if w["src"] == m["source_epoch_id"]), None)
            if owner is None and len(owners) == 1:
                owner = owners[0]  # 源纪元可能已被清理，唯一持有者认领
            if owner is None:
                continue  # 无主的跨区共享清单直接丢弃
            mid = new_id("mig")
            self.conn.execute(
                "INSERT INTO migration_manifests(migration_id,workspace_id,target_version,"
                "source_epoch_id,record_count,record_checksum,records_json,created_at) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (mid, owner["id"], tv, m["source_epoch_id"], m["record_count"],
                 m["record_checksum"], m["records_json"], m["created_at"]))
            self.conn.execute(
                "UPDATE workspaces SET migration_id=? WHERE id=?", (mid, owner["id"]))

        # 用错清单（源纪元对不上）的在途迁移携带的是他区记录。复制中的会由
        # recover_all 按既有规则回收；校验中/待发布必须在此拦截，否则会通过校验
        # 并原子发布出他区记录。
        for w in ws_rows:
            if w["migration_phase"] not in ("validating", "publishing") or not w["tv"]:
                continue
            manifest = self.conn.execute(
                "SELECT * FROM migration_manifests WHERE workspace_id=? AND target_version=?",
                (w["id"], w["tv"]),
            ).fetchone()
            mismatch = manifest is None or manifest["source_epoch_id"] != w["src"]
            if mismatch:
                if w["cand"]:
                    self.conn.execute("DELETE FROM records WHERE epoch_id=?", (w["cand"],))
                    self.conn.execute("DELETE FROM epochs WHERE id=?", (w["cand"],))
                self.conn.execute(
                    "UPDATE workspaces SET migration_phase='aborted', "
                    "migration_candidate_epoch_id=NULL, migration_source_epoch_id=NULL, "
                    "migration_owner_page_id=NULL, migration_copied=0, migration_total=0, "
                    "migration_error=? WHERE id=?",
                    ("升级后发现迁移清单与本工作区不匹配，候选纪元已安全回收", w["id"]))
                continue
            if w["migration_phase"] == "publishing" and w["cand"]:
                count, checksum = self._checksum(w["cand"])
                if (count, checksum) != (manifest["record_count"], manifest["record_checksum"]):
                    self.conn.execute(
                        "UPDATE workspaces SET migration_phase='failed', "
                        "migration_error=? WHERE id=?",
                        ("升级后发布前复核不一致，已中止发布", w["id"]))

    # ---------------------------------------------------------------- 冻结清单

    def _manifest_for_locked(self, ws_id: str, migration_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM migration_manifests WHERE migration_id=? AND workspace_id=?",
            (migration_id, ws_id),
        ).fetchone()

    def _create_manifest_locked(self, ws_id: str, migration_id: str,
                                source_epoch_id: str, target_version: str) -> sqlite3.Row:
        snapshot = self._snapshot_records(source_epoch_id)
        count, checksum = self._checksum(source_epoch_id)
        self.conn.execute(
            "INSERT INTO migration_manifests(migration_id,workspace_id,target_version,"
            "source_epoch_id,record_count,record_checksum,records_json,created_at) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (migration_id, ws_id, target_version, source_epoch_id, count, checksum,
             json.dumps(snapshot, ensure_ascii=False, separators=(",", ":")), utcnow()),
        )
        manifest = self._manifest_for_locked(ws_id, migration_id)
        assert manifest is not None
        return manifest

    def _manifest_records(self, manifest: sqlite3.Row) -> list[dict]:
        records = json.loads(manifest["records_json"])
        if not isinstance(records, list):
            raise ApiError(409, "manifest_invalid", "迁移复制清单已损坏")
        return records

    def _require_driver(self, ws: sqlite3.Row, page: sqlite3.Row | None):
        """迁移操作只能由持有当前纪元围栏的活动页面发起。"""
        if page is None or page["workspace_id"] != ws["id"]:
            raise ApiError(404, "page_not_found", "页面不存在，请重新载入")
        if page["state"] != "active":
            raise ApiError(409, "page_not_active", "本页面已失效或已关闭，请重新载入",
                           {"page_state": page["state"]})
        if page["epoch_id"] != ws["current_epoch_id"]:
            raise ApiError(409, "stale_epoch", "页面围栏已过期，请重新载入")

    # ---------------------------------------------------------- 维护与崩溃恢复

    def _maintenance_locked(self, ws_id: str):
        """惰性维护：过期心跳页面置为已关闭；属主页面失联的迁移按持久化阶段恢复。"""
        with self._tx():
            cutoff = (datetime.now(timezone.utc) - self.page_ttl).isoformat(timespec="milliseconds")
            self.conn.execute(
                "UPDATE pages SET state='closed', closed_at=? "
                "WHERE workspace_id=? AND state='active' AND last_seen < ?",
                (utcnow(), ws_id, cutoff),
            )
            ws = self._ws(ws_id)
            if ws["migration_phase"] in ACTIVE_PHASES:
                owner = self._page(ws["migration_owner_page_id"])
                if owner is None or owner["state"] != "active":
                    self._recover_locked(ws)
            elif ws["migration_phase"] == "published":
                # 每次打开/心跳也做一次幂等对账：受污染已发布工作区安全收敛
                self._reconcile_published_locked(ws)

    def _recover_locked(self, ws: sqlite3.Row):
        """依据持久化阶段恢复：复制中->回收候选；校验中->续用同一候选；发布中->完成发布。"""
        phase = ws["migration_phase"]
        cand = ws["migration_candidate_epoch_id"]
        now = utcnow()
        if phase == "copying":
            # 复制可能只完成了一部分：安全回收候选，绝不展示部分复制的数据
            if cand:
                self.conn.execute("DELETE FROM records WHERE epoch_id=?", (cand,))
                self.conn.execute("DELETE FROM epochs WHERE id=?", (cand,))
            self.conn.execute(
                "UPDATE workspaces SET migration_phase='aborted', "
                "migration_candidate_epoch_id=NULL, migration_source_epoch_id=NULL, "
                "migration_owner_page_id=NULL, migration_copied=0, migration_total=0, "
                "migration_error=?, migration_updated_at=? WHERE id=?",
                ("负责迁移的页面在复制阶段关闭，候选纪元已安全回收", now, ws["id"]),
            )
        elif phase == "validating":
            # 复制已完成：保留同一候选纪元，等待任一活动页面继续校验/发布
            self.conn.execute(
                "UPDATE workspaces SET migration_owner_page_id=NULL, migration_updated_at=? WHERE id=?",
                (now, ws["id"]),
            )
        elif phase == "publishing":
            # 发布事务未提交即中断：候选已校验，恢复时把发布补齐（幂等）
            self._publish_locked(ws)

    def _frozen_reference_locked(self, ws: sqlite3.Row) -> list[dict] | None:
        """本次已发布迁移应携带的冻结段（本工作区发起时源纪元的完整快照）。"""
        if ws["migration_id"]:
            manifest = self._manifest_for_locked(ws["id"], ws["migration_id"])
            # 清单的源纪元必须就是本工作区行记录的源，否则不采信（旧库升级兜底）
            if manifest is not None and \
                    manifest["source_epoch_id"] == ws["migration_source_epoch_id"]:
                return self._manifest_records(manifest)
        # 无清单可查：直接从仍然留存的、本工作区的源纪元取冻结快照
        src_id = ws["migration_source_epoch_id"]
        if src_id:
            src = self.conn.execute(
                "SELECT id FROM epochs WHERE id=? AND workspace_id=?",
                (src_id, ws["id"]),
            ).fetchone()
            if src is not None:
                return self._snapshot_records(src_id)
        return None

    def _reconcile_published_locked(self, ws: sqlite3.Row) -> bool:
        """已发布纪元与本工作区冻结快照对账；被跨区污染时安全收敛。

        返回是否发生了修复。修复只重写冻结段：顺序/内容/时间按源快照逐字恢复，
        迁移后新增记录顺延保留；纪元指针不回退，旧页面不复活，仍打开的页面失效
        以强制重新载入。
        """
        if ws["migration_phase"] != "published" or not ws["current_epoch_id"]:
            return False
        # 已对账/修复后代码发布的纪元（计数可能为 0）直接放行；只有遗留发布需收敛
        if ws["published_frozen_count"] is not None:
            return False
        frozen = self._frozen_reference_locked(ws)
        if frozen is None:
            return False
        cur_id = ws["current_epoch_id"]
        rows = self.conn.execute(
            "SELECT seq, content, created_at FROM records WHERE epoch_id=? ORDER BY seq",
            (cur_id,),
        ).fetchall()
        current = [dict(r) for r in rows]
        frozen_n = len(frozen)
        # 复制步骤实际写入的条数持久化在 migration_total：冻结副本严格占据
        # seq 1..total，发布后新增只能接在其后。这比时间戳边界可靠（毫秒精度下
        # 迁移发起与新增时间戳可能相同），且能正确处理他区清单更长/更短的情形。
        total = ws["migration_total"] if ws["migration_total"] is not None else frozen_n
        copied_part = current[:total]
        tail = current[total:]

        def healthy() -> bool:
            # 条数对不上（他区清单长度不同）或冻结段任一记录顺序/内容/时间不符
            if total != frozen_n or len(copied_part) != frozen_n:
                return False
            for i, want in enumerate(frozen):
                got = copied_part[i]
                if got["seq"] != i + 1 or got["content"] != want["content"] \
                        or got["created_at"] != want["created_at"]:
                    return False
            return True

        if healthy():
            self.conn.execute(
                "UPDATE workspaces SET published_frozen_count=? WHERE id=?",
                (frozen_n, ws["id"]))
            return False

        now = utcnow()
        # 调用方始终持有事务（recover_all / _maintenance_locked），整段修复原子提交
        self.conn.execute("DELETE FROM records WHERE epoch_id=?", (cur_id,))
        for i, r in enumerate(frozen):
            self.conn.execute(
                "INSERT INTO records(id,workspace_id,epoch_id,seq,content,created_at) "
                "VALUES(?,?,?,?,?,?)",
                (new_id("rec"), ws["id"], cur_id, i + 1, r["content"], r["created_at"]),
            )
        for j, r in enumerate(tail):
            self.conn.execute(
                "INSERT INTO records(id,workspace_id,epoch_id,seq,content,created_at) "
                "VALUES(?,?,?,?,?,?)",
                (new_id("rec"), ws["id"], cur_id, frozen_n + j + 1,
                 r["content"], r["created_at"]),
            )
        # 仍以为当前内容有效的活动页面必须重新载入；旧页面绝不因此复活
        self.conn.execute(
            "UPDATE pages SET state='invalidated', invalidated_at=? "
            "WHERE workspace_id=? AND state='active' AND epoch_id=?",
            (now, ws["id"], cur_id),
        )
        self.conn.execute(
            "UPDATE workspaces SET published_frozen_count=?, migration_updated_at=? WHERE id=?",
            (frozen_n, now, ws["id"]),
        )
        return True

    def recover_all(self):
        """进程启动时调用：上一生命周期的页面全部视为已关闭，并恢复所有迁移。"""
        with self._lock:
            with self._tx():
                self.conn.execute(
                    "UPDATE pages SET state='closed', closed_at=? WHERE state='active'",
                    (utcnow(),),
                )
                rows = self.conn.execute("SELECT * FROM workspaces").fetchall()
                for ws in rows:
                    if ws["migration_phase"] in ACTIVE_PHASES:
                        self._recover_locked(ws)
                    elif ws["migration_phase"] == "published":
                        # 收敛旧版本可能已发布的跨区污染（幂等，无偏差时不动数据）
                        ws = self._ws(ws["id"])
                        if self._reconcile_published_locked(ws):
                            print(f"[db] 工作区 {ws['id']} 已发布纪元与源冻结快照不一致，"
                                  "已按本工作区记录安全收敛", flush=True)

    # ---------------------------------------------------------------- 工作区

    def create_workspace(self, name: str) -> dict:
        name = (name or "").strip()
        if not name:
            raise ApiError(400, "bad_name", "工作区名称不能为空")
        if len(name) > 80:
            raise ApiError(400, "bad_name", "工作区名称过长")
        with self._lock:
            with self._tx():
                ws_id = new_id("ws")
                epoch_id = new_id("ep")
                now = utcnow()
                try:
                    self.conn.execute(
                        "INSERT INTO workspaces(id,name,created_at,current_epoch_id,migration_phase) "
                        "VALUES(?,?,?,?,'idle')",
                        (ws_id, name, now, epoch_id),
                    )
                except sqlite3.IntegrityError:
                    raise ApiError(409, "name_taken", "同名工作区已存在")
                self.conn.execute(
                    "INSERT INTO epochs(id,workspace_id,number,version,kind,created_at) "
                    "VALUES(?,?,?,?,'published',?)",
                    (epoch_id, ws_id, 1, "v1", now),
                )
        return self.get_state(ws_id)

    def list_workspaces(self) -> list[dict]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT w.id, w.name, w.created_at, w.migration_phase, "
                "       e.number AS epoch_number, e.version AS epoch_version "
                "FROM workspaces w LEFT JOIN epochs e ON e.id = w.current_epoch_id "
                "ORDER BY w.created_at"
            ).fetchall()
            return [dict(r) for r in rows]

    # ---------------------------------------------------------------- 页面

    def open_page(self, ws_id: str) -> dict:
        """打开页面：在当前已发布纪元上建立围栏。触发一次惰性恢复。"""
        with self._lock:
            self._maintenance_locked(ws_id)
            with self._tx():
                ws = self._ws(ws_id)
                page_id = new_id("pg")
                now = utcnow()
                self.conn.execute(
                    "INSERT INTO pages(id,workspace_id,epoch_id,state,created_at,last_seen) "
                    "VALUES(?,?,?,'active',?,?)",
                    (page_id, ws_id, ws["current_epoch_id"], now, now),
                )
                epoch = self._epoch(ws["current_epoch_id"])
                return {
                    "page_id": page_id,
                    "workspace_id": ws_id,
                    "epoch_id": epoch["id"],
                    "epoch_number": epoch["number"],
                    "state": "active",
                }

    def heartbeat(self, ws_id: str, page_id: str) -> dict:
        with self._lock:
            self._maintenance_locked(ws_id)
            page = self._page(page_id)
            if page is None or page["workspace_id"] != ws_id:
                raise ApiError(404, "page_not_found", "页面不存在，请重新载入")
            if page["state"] != "active":
                raise ApiError(409, "page_not_active", "页面已失效或已关闭，请重新载入",
                               {"page_state": page["state"]})
            self.conn.execute("UPDATE pages SET last_seen=? WHERE id=?", (utcnow(), page_id))
            return {"page_id": page_id, "state": "active"}

    def close_page(self, ws_id: str, page_id: str) -> dict:
        with self._lock:
            page = self._page(page_id)
            if page is None or page["workspace_id"] != ws_id:
                raise ApiError(404, "page_not_found", "页面不存在")
            if page["state"] == "active":
                self.conn.execute(
                    "UPDATE pages SET state='closed', closed_at=? WHERE id=?",
                    (utcnow(), page_id),
                )
            # 页面关闭可能让迁移属主失联：立即按持久化阶段恢复
            self._maintenance_locked(ws_id)
            page = self._page(page_id)
            return {"page_id": page_id, "state": page["state"]}

    # ---------------------------------------------------------------- 记录

    def add_record(self, ws_id: str, page_id: str, content: str) -> dict:
        """写入记录。发布前后旧页面的迟到保存一律被拒绝并提示重新载入。"""
        content = (content or "").strip()
        if not content:
            raise ApiError(400, "bad_content", "记录内容不能为空")
        if len(content) > 2000:
            raise ApiError(400, "bad_content", "记录内容过长")
        with self._lock:
            self._maintenance_locked(ws_id)
            with self._tx():
                ws = self._ws(ws_id)
                page = self._page(page_id)
                if page is None or page["workspace_id"] != ws_id:
                    raise ApiError(404, "page_not_found", "页面不存在，请重新载入")
                if page["state"] != "active":
                    raise ApiError(409, "page_not_active",
                                   "本页面已失效，保存被拒绝，请重新载入",
                                   {"page_state": page["state"]})
                if ws["migration_phase"] in ACTIVE_PHASES:
                    raise ApiError(409, "migration_in_progress",
                                   f"迁移进行中（{ws['migration_phase']}），保存被拒绝，请重新载入或稍后重试")
                if page["epoch_id"] != ws["current_epoch_id"]:
                    raise ApiError(409, "stale_epoch",
                                   "工作区已切换到新纪元，本次保存被拒绝，请重新载入")
                seq = self.conn.execute(
                    "SELECT COALESCE(MAX(seq),0)+1 AS s FROM records WHERE epoch_id=?",
                    (ws["current_epoch_id"],),
                ).fetchone()["s"]
                rec_id = new_id("rec")
                self.conn.execute(
                    "INSERT INTO records(id,workspace_id,epoch_id,seq,content,created_at) "
                    "VALUES(?,?,?,?,?,?)",
                    (rec_id, ws_id, ws["current_epoch_id"], seq, content, utcnow()),
                )
                return {"id": rec_id, "seq": seq, "content": content,
                        "epoch_id": ws["current_epoch_id"]}

    # ---------------------------------------------------------------- 迁移

    def start_migration(self, ws_id: str, page_id: str, target_version: str) -> dict:
        """发起目标版本迁移：创建唯一候选纪元并进入复制阶段。"""
        target_version = (target_version or "").strip()
        if not target_version:
            raise ApiError(400, "bad_version", "目标版本不能为空")
        if len(target_version) > 40:
            raise ApiError(400, "bad_version", "目标版本过长")
        with self._lock:
            self._maintenance_locked(ws_id)
            with self._tx():
                ws = self._ws(ws_id)
                if ws["migration_phase"] in ACTIVE_PHASES:
                    raise ApiError(409, "migration_active",
                                   "已有进行中的迁移，不会创建第二个候选纪元")
                self._require_driver(ws, self._page(page_id))
                # 上一次 failed/aborted 遗留的候选先回收
                old_cand = ws["migration_candidate_epoch_id"]
                if old_cand:
                    self.conn.execute("DELETE FROM records WHERE epoch_id=?", (old_cand,))
                    self.conn.execute("DELETE FROM epochs WHERE id=?", (old_cand,))
                number = self.conn.execute(
                    "SELECT COALESCE(MAX(number),0)+1 AS n FROM epochs WHERE workspace_id=?",
                    (ws_id,),
                ).fetchone()["n"]
                cand_id = new_id("ep")
                now = utcnow()
                self.conn.execute(
                    "INSERT INTO epochs(id,workspace_id,number,version,kind,created_at) "
                    "VALUES(?,?,?,?,'candidate',?)",
                    (cand_id, ws_id, number, target_version, now),
                )
                # 每次迁移一个全局唯一 ID，并在「此刻」冻结本工作区当前纪元的完整记录。
                # 同目标版本的旧清单（上次失败/重试）先作废，绝不复用过时快照。
                migration_id = new_id("mig")
                self.conn.execute(
                    "DELETE FROM migration_manifests WHERE workspace_id=? AND target_version=?",
                    (ws_id, target_version),
                )
                manifest = self._create_manifest_locked(
                    ws_id, migration_id, ws["current_epoch_id"], target_version)
                total = manifest["record_count"]
                self.conn.execute(
                    "UPDATE workspaces SET migration_phase='copying', migration_id=?, "
                    "migration_target_version=?, migration_candidate_epoch_id=?, "
                    "migration_source_epoch_id=?, migration_owner_page_id=?, "
                    "migration_copied=0, migration_total=?, "
                    "migration_started_at=?, migration_updated_at=?, migration_error=NULL "
                    "WHERE id=?",
                    (migration_id, target_version, cand_id, ws["current_epoch_id"], page_id,
                     total, now, now, ws_id),
                )
        return self.get_state(ws_id)

    def copy_batch(self, ws_id: str, page_id: str, batch_size: int = 1) -> dict:
        """复制一批记录到候选纪元；全部复制完自动进入校验阶段。"""
        try:
            batch_size = int(batch_size)
        except (TypeError, ValueError):
            batch_size = 1
        batch_size = max(1, min(batch_size, 500))
        with self._lock:
            self._maintenance_locked(ws_id)
            with self._tx():
                ws = self._ws(ws_id)
                if ws["migration_phase"] != "copying":
                    raise ApiError(409, "bad_phase",
                                   f"当前迁移阶段为 {ws['migration_phase']}，不能复制")
                self._require_driver(ws, self._page(page_id))
                # 继续驱动的页面认领迁移属主（用于崩溃检测）
                self.conn.execute(
                    "UPDATE workspaces SET migration_owner_page_id=? WHERE id=?",
                    (page_id, ws_id))
                cand = ws["migration_candidate_epoch_id"]
                copied = ws["migration_copied"]
                manifest = self._manifest_for_locked(ws_id, ws["migration_id"])
                if manifest is None:
                    raise ApiError(409, "manifest_missing", "迁移复制清单不存在，无法继续复制")
                # 清单必须属于本工作区的本次迁移：杜绝跨工作区复用同版本清单
                if manifest["source_epoch_id"] != ws["migration_source_epoch_id"]:
                    raise ApiError(409, "manifest_mismatch",
                                   "迁移复制清单与源纪元不匹配，请重新发起迁移")
                records = self._manifest_records(manifest)
                rows = records[copied:copied + batch_size]
                for r in rows:
                    self.conn.execute(
                        "INSERT INTO records(id,workspace_id,epoch_id,seq,content,created_at) "
                        "VALUES(?,?,?,?,?,?)",
                        (new_id("rec"), ws_id, cand, r["seq"], r["content"], r["created_at"]),
                    )
                copied += len(rows)
                total = ws["migration_total"]
                new_phase = "validating" if copied >= total else "copying"
                self.conn.execute(
                    "UPDATE workspaces SET migration_copied=?, migration_phase=?, "
                    "migration_updated_at=? WHERE id=?",
                    (copied, new_phase, utcnow(), ws_id),
                )
        return self.get_state(ws_id)

    def validate_migration(self, ws_id: str, page_id: str) -> dict:
        """校验候选纪元与源纪元一致；通过则进入待发布阶段。"""
        with self._lock:
            self._maintenance_locked(ws_id)
            with self._tx():
                ws = self._ws(ws_id)
                if ws["migration_phase"] != "validating":
                    raise ApiError(409, "bad_phase",
                                   f"当前迁移阶段为 {ws['migration_phase']}，不能校验")
                self._require_driver(ws, self._page(page_id))
                # 认领迁移属主，避免被维护逻辑误判为属主失联
                self.conn.execute(
                    "UPDATE workspaces SET migration_owner_page_id=? WHERE id=?",
                    (page_id, ws_id))
                manifest = self._manifest_for_locked(ws_id, ws["migration_id"])
                if manifest is None:
                    raise ApiError(409, "manifest_missing", "迁移复制清单不存在，无法校验")
                if manifest["source_epoch_id"] != ws["migration_source_epoch_id"]:
                    raise ApiError(409, "manifest_mismatch",
                                   "迁移复制清单与源纪元不匹配，请重新发起迁移")
                src_sum = (manifest["record_count"], manifest["record_checksum"])
                cand_sum = self._checksum(ws["migration_candidate_epoch_id"])
                now = utcnow()
                if src_sum == cand_sum:
                    self.conn.execute(
                        "UPDATE workspaces SET migration_phase='publishing', "
                        "migration_error=NULL, migration_updated_at=? WHERE id=?",
                        (now, ws_id),
                    )
                else:
                    self.conn.execute(
                        "UPDATE workspaces SET migration_phase='failed', migration_error=?, "
                        "migration_updated_at=? WHERE id=?",
                        (f"校验失败：源 {src_sum[0]} 条 / 候选 {cand_sum[0]} 条或内容摘要不一致",
                         now, ws_id),
                    )
        return self.get_state(ws_id)

    def publish_migration(self, ws_id: str, page_id: str) -> dict:
        """原子发布：单事务完成指针切换、旧纪元作废、旧页面失效。"""
        with self._lock:
            self._maintenance_locked(ws_id)
            with self._tx():
                ws = self._ws(ws_id)
                if ws["migration_phase"] != "publishing":
                    raise ApiError(409, "bad_phase",
                                   f"当前迁移阶段为 {ws['migration_phase']}，不能发布")
                self._require_driver(ws, self._page(page_id))
                if not self._publish_locked(ws):
                    raise ApiError(409, "validation_mismatch",
                                   "发布前复核不一致，迁移已标记失败")
        return self.get_state(ws_id)

    def _publish_locked(self, ws: sqlite3.Row) -> bool:
        """发布事务体（恢复路径与 publish API 共用）。返回是否成功。"""
        old = ws["current_epoch_id"]
        cand = ws["migration_candidate_epoch_id"]
        if not cand:
            return False
        now = utcnow()
        # 发布前复核：候选必须与本工作区本次迁移的冻结清单一致，否则失败而非发布半成品
        manifest = self._manifest_for_locked(ws["id"], ws["migration_id"])
        if manifest is None or manifest["source_epoch_id"] != ws["migration_source_epoch_id"]:
            self.conn.execute(
                "UPDATE workspaces SET migration_phase='failed', migration_error=?, "
                "migration_updated_at=? WHERE id=?",
                ("发布前复核发现迁移清单与本工作区不匹配，已中止发布", now, ws["id"]),
            )
            return False
        expected = (manifest["record_count"], manifest["record_checksum"])
        if self._checksum(cand) != expected:
            self.conn.execute(
                "UPDATE workspaces SET migration_phase='failed', migration_error=?, "
                "migration_updated_at=? WHERE id=?",
                ("发布前复核不一致，已中止发布", now, ws["id"]),
            )
            return False
        self.conn.execute("UPDATE epochs SET kind='superseded' WHERE id=?", (old,))
        self.conn.execute("UPDATE epochs SET kind='published' WHERE id=?", (cand,))
        # 持有旧纪元围栏的页面全部失效：其后的迟到保存会被拒绝
        self.conn.execute(
            "UPDATE pages SET state='invalidated', invalidated_at=? "
            "WHERE workspace_id=? AND state='active' AND epoch_id=?",
            (now, ws["id"], old),
        )
        self.conn.execute(
            "UPDATE workspaces SET current_epoch_id=?, migration_phase='published', "
            "migration_candidate_epoch_id=NULL, migration_owner_page_id=NULL, "
            "migration_error=NULL, published_frozen_count=?, migration_updated_at=? WHERE id=?",
            (cand, manifest["record_count"], now, ws["id"]),
        )
        return True

    # ---------------------------------------------------------------- 状态

    def get_state(self, ws_id: str, touch_page_id: str | None = None) -> dict:
        """页面展示所需的全部状态：当前纪元、迁移阶段、围栏页面及失效状态、当前纪元记录。"""
        with self._lock:
            self._maintenance_locked(ws_id)
            if touch_page_id:
                self.conn.execute(
                    "UPDATE pages SET last_seen=? WHERE id=? AND workspace_id=? AND state='active'",
                    (utcnow(), touch_page_id, ws_id),
                )
            ws = self._ws(ws_id)
            cur = self._epoch(ws["current_epoch_id"])
            records = self.conn.execute(
                "SELECT id, seq, content, created_at FROM records "
                "WHERE epoch_id=? ORDER BY seq",
                (ws["current_epoch_id"],),
            ).fetchall()
            pages = self.conn.execute(
                "SELECT p.id, p.epoch_id, p.state, p.created_at, p.last_seen, "
                "       p.closed_at, p.invalidated_at, e.number AS epoch_number "
                "FROM pages p LEFT JOIN epochs e ON e.id = p.epoch_id "
                "WHERE p.workspace_id=? ORDER BY p.created_at DESC, p.id DESC LIMIT 50",
                (ws_id,),
            ).fetchall()
            migration = None
            if ws["migration_started_at"] is not None:
                cand = self._epoch(ws["migration_candidate_epoch_id"])
                migration = {
                    "phase": ws["migration_phase"],
                    "target_version": ws["migration_target_version"],
                    "candidate_epoch": (
                        {"id": cand["id"], "number": cand["number"]} if cand else None
                    ),
                    "source_epoch_id": ws["migration_source_epoch_id"],
                    "owner_page_id": ws["migration_owner_page_id"],
                    "copied": ws["migration_copied"],
                    "total": ws["migration_total"],
                    "started_at": ws["migration_started_at"],
                    "updated_at": ws["migration_updated_at"],
                    "error": ws["migration_error"],
                }
            return {
                "id": ws["id"],
                "name": ws["name"],
                "created_at": ws["created_at"],
                "current_epoch": {
                    "id": cur["id"],
                    "number": cur["number"],
                    "version": cur["version"],
                    "record_count": len(records),
                },
                "migration": migration,
                "pages": [dict(p) for p in pages],
                "records": [dict(r) for r in records],
            }
