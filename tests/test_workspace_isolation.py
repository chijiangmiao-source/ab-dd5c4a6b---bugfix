"""多工作区同目标版本迁移的隔离性，以及旧版本受影响工作区的安全收敛。"""

import hashlib
import json
import os
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db import ApiError, Store  # noqa: E402


def drive(store, ws_id, page_id, version="v2", batch=10):
    """发起并完成一次迁移。"""
    store.start_migration(ws_id, page_id, version)
    while True:
        s = store.copy_batch(ws_id, page_id, batch)
        if s["migration"]["phase"] == "validating":
            break
    s = store.validate_migration(ws_id, page_id)
    assert s["migration"]["phase"] == "publishing"
    return store.publish_migration(ws_id, page_id)


class IsolationTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "test.db")
        self.store = Store(self.db_path, page_ttl_seconds=45)

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def make_ws(self, name, contents):
        ws = self.store.create_workspace(name)
        page = self.store.open_page(ws["id"])["page_id"]
        for c in contents:
            self.store.add_record(ws["id"], page, c)
        return ws, page

    def snapshot(self, ws_id):
        s = self.store.get_state(ws_id)
        return s, [(r["seq"], r["content"], r["created_at"]) for r in s["records"]]

    # ------------------------------------------------ 顺序迁移

    def test_sequential_same_target_version_isolated(self):
        """两个工作区先后迁移到同一目标版本，各自只携带本工作区冻结的记录。"""
        a, pa = self.make_ws("样地A", ["A记录-1", "A记录-2", "A记录-3"])
        b, pb = self.make_ws("样地B", ["B记录-甲", "B记录-乙", "B记录-丙", "B记录-丁"])
        _, a_before = self.snapshot(a["id"])
        _, b_before = self.snapshot(b["id"])

        drive(self.store, a["id"], pa, "v2")
        drive(self.store, b["id"], pb, "v2")

        sa, a_after = self.snapshot(a["id"])
        sb, b_after = self.snapshot(b["id"])
        self.assertEqual(sa["current_epoch"]["number"], 2)
        self.assertEqual(sb["current_epoch"]["number"], 2)
        self.assertEqual(a_after, a_before)   # 顺序、内容、时间信息完全保留
        self.assertEqual(b_after, b_before)
        self.assertEqual(sb["migration"]["integrity"], "ok")
        self.assertTrue({c for _, c, _ in a_after}.isdisjoint(
            {c for _, c, _ in b_after}))

    def test_first_workspace_unchanged_after_second_migrates(self):
        """第二个工作区迁移后，第一个工作区及其已发布纪元不得变化。"""
        a, pa = self.make_ws("样地A", ["A-1"])
        b, pb = self.make_ws("样地B", ["B-1", "B-2"])
        sa = drive(self.store, a["id"], pa, "v2")
        epoch_a = sa["current_epoch"]
        records_a = [(r["seq"], r["content"], r["created_at"]) for r in sa["records"]]
        drive(self.store, b["id"], pb, "v2")
        sa2 = self.store.get_state(a["id"])
        self.assertEqual(sa2["current_epoch"], epoch_a)
        self.assertEqual(
            [(r["seq"], r["content"], r["created_at"]) for r in sa2["records"]],
            records_a)

    # ------------------------------------------------ 交叠迁移

    def test_overlapping_same_target_version_isolated(self):
        """两个工作区的迁移交叠进行（复制/校验/发布互相穿插），记录完全隔离。"""
        a, pa = self.make_ws("样地A", ["A-1", "A-2", "A-3"])
        b, pb = self.make_ws("样地B", ["B-1", "B-2", "B-3", "B-4", "B-5"])
        _, a_before = self.snapshot(a["id"])
        _, b_before = self.snapshot(b["id"])

        # A 复制一半后 B 发起；B 先复制完并发布，A 再复制完并发布
        self.store.start_migration(a["id"], pa, "v2")
        self.store.copy_batch(a["id"], pa, 2)
        self.store.start_migration(b["id"], pb, "v2")
        self.store.copy_batch(b["id"], pb, 5)
        self.store.copy_batch(a["id"], pa, 5)
        self.store.validate_migration(b["id"], pb)
        self.store.publish_migration(b["id"], pb)
        self.store.validate_migration(a["id"], pa)
        self.store.publish_migration(a["id"], pa)

        _, a_after = self.snapshot(a["id"])
        _, b_after = self.snapshot(b["id"])
        self.assertEqual(a_after, a_before)
        self.assertEqual(b_after, b_before)

    def test_concurrent_migration_uniqueness_is_per_workspace(self):
        """候选唯一性是工作区内约束：不同工作区可同时各有一个候选，互不干扰。"""
        a, pa = self.make_ws("样地A", ["A-1"])
        b, pb = self.make_ws("样地B", ["B-1"])
        self.store.start_migration(a["id"], pa, "v2")
        self.store.start_migration(b["id"], pb, "v2")  # 不冲突
        cands = self.store.conn.execute(
            "SELECT workspace_id, COUNT(*) AS c FROM epochs WHERE kind='candidate' "
            "GROUP BY workspace_id ORDER BY workspace_id").fetchall()
        self.assertEqual(sorted((r["workspace_id"], r["c"]) for r in cands),
                         sorted([(a["id"], 1), (b["id"], 1)]))
        # 同一工作区内仍不得建立第二个候选
        pa2 = self.store.open_page(a["id"])["page_id"]
        with self.assertRaises(ApiError) as ctx:
            self.store.start_migration(a["id"], pa2, "v2b")
        self.assertEqual(ctx.exception.status, 409)
        self.assertEqual(ctx.exception.code, "migration_active")

    # ------------------------------------------------ 重新冻结

    def test_retry_refreezes_including_new_records(self):
        """同一工作区失败后以同一目标版本重发：冻结的是重发时的完整记录。"""
        w, p = self.make_ws("样地W", ["旧-1", "旧-2"])
        self.store.start_migration(w["id"], p, "v2")
        while self.store.copy_batch(w["id"], p, 10)["migration"]["phase"] != "validating":
            pass
        cand = self.store.get_state(w["id"])["migration"]["candidate_epoch"]["id"]
        self.store.conn.execute(
            "UPDATE records SET content='被篡改' WHERE epoch_id=? AND seq=1", (cand,))
        s = self.store.validate_migration(w["id"], p)
        self.assertEqual(s["migration"]["phase"], "failed")

        self.store.add_record(w["id"], p, "失败后新增")
        drive(self.store, w["id"], p, "v2")
        self.assertEqual(
            [r["content"] for r in self.store.get_state(w["id"])["records"]],
            ["旧-1", "旧-2", "失败后新增"])


# -------------------------------------------------------- 旧库升级与安全收敛

LEGACY_DDL = """
CREATE TABLE workspaces (
  id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE, created_at TEXT NOT NULL,
  current_epoch_id TEXT, migration_phase TEXT NOT NULL DEFAULT 'idle',
  migration_target_version TEXT, migration_candidate_epoch_id TEXT,
  migration_source_epoch_id TEXT, migration_owner_page_id TEXT,
  migration_copied INTEGER NOT NULL DEFAULT 0, migration_total INTEGER NOT NULL DEFAULT 0,
  migration_started_at TEXT, migration_updated_at TEXT, migration_error TEXT
);
CREATE TABLE epochs (id TEXT PRIMARY KEY, workspace_id TEXT NOT NULL, number INTEGER NOT NULL,
  version TEXT NOT NULL, kind TEXT NOT NULL, created_at TEXT NOT NULL,
  UNIQUE(workspace_id, number));
CREATE TABLE records (id TEXT PRIMARY KEY, workspace_id TEXT NOT NULL, epoch_id TEXT NOT NULL,
  seq INTEGER NOT NULL, content TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE TABLE migration_manifests (target_version TEXT PRIMARY KEY, source_epoch_id TEXT NOT NULL,
  record_count INTEGER NOT NULL, record_checksum TEXT NOT NULL, records_json TEXT NOT NULL,
  created_at TEXT NOT NULL);
CREATE TABLE pages (id TEXT PRIMARY KEY, workspace_id TEXT NOT NULL, epoch_id TEXT NOT NULL,
  state TEXT NOT NULL, created_at TEXT NOT NULL, last_seen TEXT NOT NULL,
  closed_at TEXT, invalidated_at TEXT);
CREATE INDEX idx_records_epoch ON records(epoch_id, seq);
CREATE INDEX idx_pages_ws ON pages(workspace_id);
"""


def checksum(rows):
    h = hashlib.sha256()
    for r in rows:
        h.update(f"{r['seq']}|".encode())
        h.update(r["content"].encode())
        h.update(b"\n")
    return h.hexdigest()


def build_legacy_db(path, b_phase="published"):
    """构造旧版库：A 正常发布 v2；B 的 v2 迁移命中 A 的全局清单。

    b_phase:
      published  —— B 已发布但新纪元装的是 A 的记录，且迁移后新增过一条
      copying/validating/publishing —— B 的迁移停在该阶段，候选装的是 A 的记录
    """
    a = [{"seq": i, "content": f"A-{i}", "created_at": f"tA{i}"} for i in range(1, 4)]
    b = [{"seq": i, "content": f"B-{i}", "created_at": f"tB{i}"} for i in range(1, 4)]
    conn = sqlite3.connect(path)
    conn.executescript(LEGACY_DDL)
    conn.execute("INSERT INTO epochs VALUES('epA1','wsA',1,'v1','superseded','t0')")
    conn.execute("INSERT INTO epochs VALUES('epA2','wsA',2,'v2','published','t0')")
    for i, r in enumerate(a, 1):
        conn.execute("INSERT INTO records VALUES(?,?,?,?,?,?)",
                     (f"ra1{i}", "wsA", "epA1", i, r["content"], r["created_at"]))
        conn.execute("INSERT INTO records VALUES(?,?,?,?,?,?)",
                     (f"ra2{i}", "wsA", "epA2", i, r["content"], r["created_at"]))
    conn.execute("INSERT INTO records VALUES(?,?,?,?,?,?)",
                 ("rb11", "wsB", "epB1", 1, "B-1", "tB1"))
    conn.execute("INSERT INTO records VALUES(?,?,?,?,?,?)",
                 ("rb12", "wsB", "epB1", 2, "B-2", "tB2"))
    conn.execute("INSERT INTO records VALUES(?,?,?,?,?,?)",
                 ("rb13", "wsB", "epB1", 3, "B-3", "tB3"))
    conn.execute("INSERT INTO migration_manifests VALUES(?,?,?,?,?,?)",
                 ("v2", "epA1", 3, checksum(a),
                  json.dumps(a, ensure_ascii=False, separators=(",", ":")), "t0"))
    conn.execute(
        "INSERT INTO workspaces(id,name,created_at,current_epoch_id,migration_phase,"
        "migration_target_version,migration_candidate_epoch_id,migration_source_epoch_id,"
        "migration_owner_page_id,migration_copied,migration_total,migration_started_at,"
        "migration_updated_at,migration_error) "
        "VALUES('wsA','样地A','t0','epA2','published','v2',NULL,'epA1',NULL,3,3,'t0','t0',NULL)")
    conn.execute("INSERT INTO pages VALUES('pgAold','wsA','epA1','invalidated','t0','t0',NULL,'t0')")

    if b_phase == "published":
        conn.execute("INSERT INTO epochs VALUES('epB2','wsB',2,'v2','published','t0')")
        for i, r in enumerate(a, 1):  # B 的新纪元错误地装了 A 的记录
            conn.execute("INSERT INTO records VALUES(?,?,?,?,?,?)",
                         (f"rbx{i}", "wsB", "epB2", i, r["content"], r["created_at"]))
        conn.execute("INSERT INTO records VALUES(?,?,?,?,?,?)",
                     ("rbnew", "wsB", "epB2", 4, "B迁移后新增", "tNEW"))
        conn.execute(
            "INSERT INTO workspaces(id,name,created_at,current_epoch_id,migration_phase,"
            "migration_target_version,migration_candidate_epoch_id,migration_source_epoch_id,"
            "migration_owner_page_id,migration_copied,migration_total,migration_started_at,"
            "migration_updated_at,migration_error) "
            "VALUES('wsB','样地B','t0','epB2','published','v2',NULL,'epB1',NULL,3,3,'t0','t0',NULL)")
    else:
        conn.execute("INSERT INTO epochs VALUES('epB1','wsB',1,'v1','published','t0')")
        conn.execute("INSERT INTO epochs VALUES('epBc','wsB',2,'v2','candidate','t0')")
        n = 2 if b_phase == "copying" else 3
        for i in range(1, n + 1):
            conn.execute("INSERT INTO records VALUES(?,?,?,?,?,?)",
                         (f"rbc{i}", "wsB", "epBc", i, f"A-{i}", f"tA{i}"))
        conn.execute(
            "INSERT INTO workspaces(id,name,created_at,current_epoch_id,migration_phase,"
            "migration_target_version,migration_candidate_epoch_id,migration_source_epoch_id,"
            "migration_owner_page_id,migration_copied,migration_total,migration_started_at,"
            "migration_updated_at,migration_error) "
            "VALUES('wsB','样地B','t0','epB1',?,'v2','epBc','epB1',NULL,?,3,'t0','t0',NULL)",
            (b_phase, n))
    conn.execute("INSERT INTO pages VALUES('pgBold','wsB','epB1','invalidated','t0','t0',NULL,'t0')")
    conn.commit()
    conn.close()


class LegacyConvergenceTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "legacy.db")

    def tearDown(self):
        self.tmp.cleanup()

    def test_published_affected_workspace_converges_safely(self):
        build_legacy_db(self.db_path, "published")
        store = Store(self.db_path, page_ttl_seconds=45)
        sb = store.get_state("wsB")
        # 标记为受影响，但不倒退纪元、不改写记录（含迁移后新增）
        self.assertEqual(sb["migration"]["integrity"], "affected_foreign_records")
        self.assertEqual(sb["current_epoch"]["id"], "epB2")
        self.assertEqual(sb["current_epoch"]["number"], 2)
        self.assertEqual([r["content"] for r in sb["records"]],
                         ["A-1", "A-2", "A-3", "B迁移后新增"])
        # 旧页面不重新取得写入资格
        with self.assertRaises(ApiError) as ctx:
            store.add_record("wsB", "pgBold", "旧页迟到保存")
        self.assertEqual(ctx.exception.code, "page_not_active")
        # 第一个工作区完全不变
        sa = store.get_state("wsA")
        self.assertEqual(sa["migration"]["integrity"], "ok")
        self.assertEqual(sa["current_epoch"]["id"], "epA2")
        self.assertEqual([r["content"] for r in sa["records"]],
                         ["A-1", "A-2", "A-3"])
        # 新页面可继续追加，审计/恢复不覆盖迁移后新增
        page = store.open_page("wsB")["page_id"]
        store.add_record("wsB", page, "恢复后追加")
        store.close()

        store = Store(self.db_path, page_ttl_seconds=45)  # 再重开：结论稳定
        sb = store.get_state("wsB")
        self.assertEqual(sb["migration"]["integrity"], "affected_foreign_records")
        self.assertEqual([r["content"] for r in sb["records"]],
                         ["A-1", "A-2", "A-3", "B迁移后新增", "恢复后追加"])
        store.close()

    def test_inflight_validating_foreign_manifest_fails_then_remigrates(self):
        build_legacy_db(self.db_path, "validating")
        store = Store(self.db_path, page_ttl_seconds=45)
        sb = store.get_state("wsB")
        self.assertEqual(sb["migration"]["phase"], "validating")  # 候选保留待续
        page = store.open_page("wsB")["page_id"]
        with self.assertRaises(ApiError) as ctx:
            store.validate_migration("wsB", page)
        self.assertEqual(ctx.exception.code, "manifest_missing")
        self.assertEqual(store.get_state("wsB")["migration"]["phase"], "failed")
        drive(store, "wsB", page, "v2")
        self.assertEqual(
            [r["content"] for r in store.get_state("wsB")["records"]],
            ["B-1", "B-2", "B-3"])
        store.close()

    def test_inflight_publishing_foreign_manifest_not_published(self):
        build_legacy_db(self.db_path, "publishing")
        store = Store(self.db_path, page_ttl_seconds=45)
        sb = store.get_state("wsB")
        self.assertEqual(sb["migration"]["phase"], "failed")  # 恢复补齐发布时复核拒绝
        self.assertEqual(sb["current_epoch"]["id"], "epB1")   # 纪元不倒退、不发外来候选
        self.assertEqual([r["content"] for r in sb["records"]],
                         ["B-1", "B-2", "B-3"])
        page = store.open_page("wsB")["page_id"]
        drive(store, "wsB", page, "v2")
        self.assertEqual(
            [r["content"] for r in store.get_state("wsB")["records"]],
            ["B-1", "B-2", "B-3"])
        store.close()

    def test_inflight_copying_foreign_candidate_recycled(self):
        build_legacy_db(self.db_path, "copying")
        store = Store(self.db_path, page_ttl_seconds=45)
        sb = store.get_state("wsB")
        self.assertEqual(sb["migration"]["phase"], "aborted")
        self.assertIsNone(sb["migration"]["candidate_epoch"])
        self.assertEqual([r["content"] for r in sb["records"]],
                         ["B-1", "B-2", "B-3"])
        page = store.open_page("wsB")["page_id"]
        drive(store, "wsB", page, "v2")
        self.assertEqual(
            [r["content"] for r in store.get_state("wsB")["records"]],
            ["B-1", "B-2", "B-3"])
        store.close()


if __name__ == "__main__":
    unittest.main()
