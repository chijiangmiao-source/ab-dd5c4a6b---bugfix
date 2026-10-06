"""纪元迁移状态机的单元测试。"""

import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db import ApiError, Store  # noqa: E402


class StoreTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "test.db")
        self.store = Store(self.db_path, page_ttl_seconds=45)

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    # ------------------------------------------------------------ 辅助

    def make_ws(self, name="样地A", record_count=0):
        ws = self.store.create_workspace(name)
        page = self.store.open_page(ws["id"])
        for i in range(record_count):
            self.store.add_record(ws["id"], page["page_id"], f"观测记录-{i + 1}")
        return ws, page["page_id"]

    def drive_to(self, ws_id, page_id, target_phase, version="v2", batch=1, start=True):
        """把迁移推进到指定阶段。start=False 表示迁移已发起，直接续推。"""
        if start:
            self.store.start_migration(ws_id, page_id, version)
        if target_phase == "copying":
            return
        while True:
            s = self.store.copy_batch(ws_id, page_id, batch)
            if s["migration"]["phase"] == "validating":
                break
        if target_phase == "validating":
            return
        self.store.validate_migration(ws_id, page_id)
        if target_phase == "publishing":
            return
        self.store.publish_migration(ws_id, page_id)

    def assert_api_error(self, status, code, fn, *args, **kwargs):
        with self.assertRaises(ApiError) as ctx:
            fn(*args, **kwargs)
        self.assertEqual(ctx.exception.status, status)
        self.assertEqual(ctx.exception.code, code)
        return ctx.exception

    # ------------------------------------------------------------ 基础

    def test_workspace_starts_at_epoch_one(self):
        ws, _ = self.make_ws()
        self.assertEqual(ws["current_epoch"]["number"], 1)
        self.assertEqual(ws["current_epoch"]["version"], "v1")
        self.assertEqual(ws["records"], [])

    def test_add_and_read_records(self):
        ws, page = self.make_ws(record_count=3)
        s = self.store.get_state(ws["id"])
        self.assertEqual([r["content"] for r in s["records"]],
                         ["观测记录-1", "观测记录-2", "观测记录-3"])
        self.assertEqual([r["seq"] for r in s["records"]], [1, 2, 3])

    def test_write_requires_active_page(self):
        ws, page = self.make_ws()
        self.store.close_page(ws["id"], page)
        self.assert_api_error(409, "page_not_active",
                              self.store.add_record, ws["id"], page, "迟到记录")

    # ------------------------------------------------------------ 迁移主流程

    def test_full_migration_and_stale_write_rejected(self):
        """两个页面打开同一工作区：迁移完成后，另一页的迟到保存被拒绝。"""
        ws, page_a = self.make_ws(record_count=4)
        page_b = self.store.open_page(ws["id"])["page_id"]

        self.store.start_migration(ws["id"], page_a, "v2")
        # 发布前：迁移进行中，另一页的保存被拒绝
        self.assert_api_error(409, "migration_in_progress",
                              self.store.add_record, ws["id"], page_b, "迟到记录")
        # 复制 -> 校验 -> 发布
        self.drive_to(ws["id"], page_a, "publishing", start=False)
        self.store.publish_migration(ws["id"], page_a)

        s = self.store.get_state(ws["id"])
        self.assertEqual(s["current_epoch"]["number"], 2)
        self.assertEqual(s["current_epoch"]["version"], "v2")
        self.assertEqual([r["content"] for r in s["records"]],
                         [f"观测记录-{i}" for i in range(1, 5)])
        # 两个旧页面均已失效
        states = {p["id"]: p["state"] for p in s["pages"]}
        self.assertEqual(states[page_a], "invalidated")
        self.assertEqual(states[page_b], "invalidated")
        # 发布后：旧页面的迟到保存仍被拒绝并提示重新载入
        err = self.assert_api_error(409, "page_not_active",
                                    self.store.add_record, ws["id"], page_b, "迟到记录")
        self.assertIn("重新载入", err.message)
        # 重开页面后只能读到新纪元，且可继续写入
        page_c = self.store.open_page(ws["id"])["page_id"]
        self.store.add_record(ws["id"], page_c, "新纪元记录")
        s = self.store.get_state(ws["id"])
        self.assertEqual(len(s["records"]), 5)
        self.assertEqual(s["records"][-1]["content"], "新纪元记录")

    def test_concurrent_migration_creates_no_second_candidate(self):
        ws, page_a = self.make_ws(record_count=2)
        page_b = self.store.open_page(ws["id"])["page_id"]
        self.store.start_migration(ws["id"], page_a, "v2")
        self.assert_api_error(409, "migration_active",
                              self.store.start_migration, ws["id"], page_b, "v2b")
        cand_count = self.store.conn.execute(
            "SELECT COUNT(*) AS c FROM epochs WHERE kind='candidate'").fetchone()["c"]
        self.assertEqual(cand_count, 1)

    def test_reads_never_come_from_candidate(self):
        """复制进行中读取到的仍是完整旧纪元，候选的部分数据不可见。"""
        ws, page_a = self.make_ws(record_count=5)
        self.store.start_migration(ws["id"], page_a, "v2")
        self.store.copy_batch(ws["id"], page_a, 2)  # 只复制 2/5
        s = self.store.get_state(ws["id"])
        self.assertEqual(s["current_epoch"]["number"], 1)
        self.assertEqual(len(s["records"]), 5)
        self.assertEqual(s["migration"]["copied"], 2)

    # ------------------------------------------------------------ 中断恢复

    def test_copy_interruption_recycles_candidate(self):
        """复制阶段页面关闭：候选被安全回收，绝不展示部分复制数据。"""
        ws, page_a = self.make_ws(record_count=5)
        self.store.start_migration(ws["id"], page_a, "v2")
        self.store.copy_batch(ws["id"], page_a, 2)
        self.store.close_page(ws["id"], page_a)  # 模拟页面在复制之间关闭

        s = self.store.get_state(ws["id"])
        self.assertEqual(s["migration"]["phase"], "aborted")
        self.assertIsNone(s["migration"]["candidate_epoch"])
        self.assertEqual(s["current_epoch"]["number"], 1)
        self.assertEqual(len(s["records"]), 5)  # 完整旧纪元
        cand_count = self.store.conn.execute(
            "SELECT COUNT(*) AS c FROM epochs WHERE kind='candidate'").fetchone()["c"]
        self.assertEqual(cand_count, 0)

    def test_validating_resumes_same_candidate(self):
        """校验阶段页面关闭：后来页面续用同一候选并完成迁移。"""
        ws, page_a = self.make_ws(record_count=3)
        self.drive_to(ws["id"], page_a, "validating")
        cand_before = self.store.get_state(ws["id"])["migration"]["candidate_epoch"]
        self.store.close_page(ws["id"], page_a)

        s = self.store.get_state(ws["id"])
        self.assertEqual(s["migration"]["phase"], "validating")
        self.assertEqual(s["migration"]["candidate_epoch"], cand_before)  # 同一候选

        page_b = self.store.open_page(ws["id"])["page_id"]
        self.store.validate_migration(ws["id"], page_b)
        self.store.publish_migration(ws["id"], page_b)
        s = self.store.get_state(ws["id"])
        self.assertEqual(s["current_epoch"]["number"], 2)
        self.assertEqual(len(s["records"]), 3)

    def test_publishing_completes_after_owner_close(self):
        """发布阶段页面关闭：恢复时把原子发布补齐。"""
        ws, page_a = self.make_ws(record_count=3)
        self.drive_to(ws["id"], page_a, "publishing")
        self.store.close_page(ws["id"], page_a)

        s = self.store.get_state(ws["id"])
        self.assertEqual(s["migration"]["phase"], "published")
        self.assertEqual(s["current_epoch"]["number"], 2)
        self.assertEqual(len(s["records"]), 3)

    def test_crash_recovery_on_reopen_store(self):
        """模拟进程在复制中途崩溃：重开存储后候选被回收，数据不残缺。"""
        ws, page_a = self.make_ws(record_count=4)
        self.store.start_migration(ws["id"], page_a, "v2")
        self.store.copy_batch(ws["id"], page_a, 1)
        self.store.close()  # 模拟进程崩溃（事务已提交到 copying 阶段）

        self.store = Store(self.db_path, page_ttl_seconds=45)
        s = self.store.get_state(ws["id"])
        self.assertEqual(s["migration"]["phase"], "aborted")
        self.assertEqual(s["current_epoch"]["number"], 1)
        self.assertEqual(len(s["records"]), 4)

    # ------------------------------------------------------------ 校验与持久化

    def test_validate_mismatch_marks_failed_and_allows_retry(self):
        ws, page_a = self.make_ws(record_count=3)
        self.drive_to(ws["id"], page_a, "validating")
        cand = self.store.get_state(ws["id"])["migration"]["candidate_epoch"]["id"]
        # 人为破坏候选内容
        self.store.conn.execute(
            "UPDATE records SET content='被篡改' WHERE epoch_id=? AND seq=1", (cand,))
        s = self.store.validate_migration(ws["id"], page_a)
        self.assertEqual(s["migration"]["phase"], "failed")
        # 失败后可重新发起：旧候选被回收，新候选唯一
        self.store.start_migration(ws["id"], page_a, "v2")
        cand_count = self.store.conn.execute(
            "SELECT COUNT(*) AS c FROM epochs WHERE kind='candidate'").fetchone()["c"]
        self.assertEqual(cand_count, 1)
        self.drive_to(ws["id"], page_a, "published", start=False)
        s = self.store.get_state(ws["id"])
        self.assertEqual(s["current_epoch"]["number"], 2)
        self.assertEqual(len(s["records"]), 3)

    def test_persistence_after_publish(self):
        """发布后重开存储：纪元、记录、页面失效状态一致。"""
        ws, page_a = self.make_ws(record_count=4)
        page_b = self.store.open_page(ws["id"])["page_id"]
        self.drive_to(ws["id"], page_a, "published")
        before = self.store.get_state(ws["id"])
        self.store.close()

        self.store = Store(self.db_path, page_ttl_seconds=45)
        after = self.store.get_state(ws["id"])
        self.assertEqual(after["current_epoch"], before["current_epoch"])
        self.assertEqual([r["content"] for r in after["records"]],
                         [r["content"] for r in before["records"]])
        states = {p["id"]: p["state"] for p in after["pages"]}
        self.assertEqual(states[page_a], "invalidated")
        self.assertEqual(states[page_b], "invalidated")
        self.assertEqual(after["migration"]["phase"], "published")

    # ------------------------------------------------- 多工作区同版本隔离

    def make_ws_named(self, name, contents):
        ws = self.store.create_workspace(name)
        page = self.store.open_page(ws["id"])["page_id"]
        for c in contents:
            self.store.add_record(ws["id"], page, c)
        return ws, page

    def drive_version(self, ws_id, page, version, batch=10):
        self.store.start_migration(ws_id, page, version)
        while True:
            s = self.store.copy_batch(ws_id, page, batch)
            if s["migration"]["phase"] == "validating":
                break
        self.store.validate_migration(ws_id, page)
        return self.store.publish_migration(ws_id, page)

    def test_two_workspaces_same_target_version_stay_isolated(self):
        """两个含明显不同记录的工作区先后迁移到同一目标版本：各自只携带自身冻结记录。"""
        ws_a, page_a = self.make_ws_named("样地甲", [f"甲-{i}" for i in range(1, 4)])
        ws_b, page_b = self.make_ws_named("样地乙", [f"乙-{i}" for i in range(1, 3)])
        src_a = {r["content"]: r["created_at"]
                 for r in self.store.get_state(ws_a["id"])["records"]}

        self.drive_version(ws_a["id"], page_a, "vX")
        self.drive_version(ws_b["id"], page_b, "vX")  # 同一目标版本

        sa = self.store.get_state(ws_a["id"])
        sb = self.store.get_state(ws_b["id"])
        self.assertEqual([r["content"] for r in sa["records"]], ["甲-1", "甲-2", "甲-3"])
        self.assertEqual([r["content"] for r in sb["records"]], ["乙-1", "乙-2"])
        self.assertEqual(sa["current_epoch"]["version"], "vX")
        self.assertEqual(sb["current_epoch"]["version"], "vX")
        # 时间信息保留：新纪元冻结段沿用各工作区源记录时间戳
        for r in sa["records"]:
            self.assertEqual(r["created_at"], src_a[r["content"]])

    def test_second_workspace_preserves_order_content_and_timestamps(self):
        """第二个工作区新纪元的顺序、内容、时间信息与自身源纪元逐条一致。"""
        ws_a, page_a = self.make_ws_named("甲站", ["a1", "a2"])
        ws_b, page_b = self.make_ws_named("乙站", ["b1", "b2", "b3"])
        src_b = [(r["seq"], r["content"], r["created_at"])
                 for r in self.store.get_state(ws_b["id"])["records"]]

        self.drive_version(ws_a["id"], page_a, "v2")
        self.drive_version(ws_b["id"], page_b, "v2")
        got_b = [(r["seq"], r["content"], r["created_at"])
                 for r in self.store.get_state(ws_b["id"])["records"]]
        self.assertEqual(got_b, src_b)
        # 第一个工作区及其已发布纪元不变
        self.assertEqual(
            [(r["seq"], r["content"]) for r in self.store.get_state(ws_a["id"])["records"]],
            [(1, "a1"), (2, "a2")])

    def test_overlapping_same_version_migrations_do_not_share_freeze(self):
        """两个工作区迁移交叠（甲在复制中，乙已发布），完成后记录仍完全隔离。"""
        ws_a, page_a = self.make_ws_named("甲站", ["甲-x", "甲-y"])
        ws_b, page_b = self.make_ws_named("乙站", ["乙-only"])

        self.store.start_migration(ws_a["id"], page_a, "v9")
        self.store.copy_batch(ws_a["id"], page_a, 1)  # 甲停在复制中
        # 乙完整迁移并发布同一版本
        self.drive_version(ws_b["id"], page_b, "v9")
        self.assertEqual(
            [r["content"] for r in self.store.get_state(ws_b["id"])["records"]], ["乙-only"])
        # 甲继续完成：仍是自己的冻结记录
        self.store.copy_batch(ws_a["id"], page_a, 10)
        self.store.validate_migration(ws_a["id"], page_a)
        self.store.publish_migration(ws_a["id"], page_a)
        self.assertEqual(
            [r["content"] for r in self.store.get_state(ws_a["id"])["records"]],
            ["甲-x", "甲-y"])

    def test_post_publish_appends_survive_other_workspace_migration(self):
        """甲发布并新增记录后，乙迁移同版本；甲的新增不丢、乙不带入甲记录。"""
        ws_a, page_a = self.make_ws_named("甲站", ["甲-1"])
        ws_b, page_b = self.make_ws_named("乙站", ["乙-1", "乙-2"])
        self.drive_version(ws_a["id"], page_a, "v3")
        page_a2 = self.store.open_page(ws_a["id"])["page_id"]
        self.store.add_record(ws_a["id"], page_a2, "甲迁移后新增")
        self.drive_version(ws_b["id"], page_b, "v3")
        self.assertEqual([r["content"] for r in self.store.get_state(ws_a["id"])["records"]],
                         ["甲-1", "甲迁移后新增"])
        self.assertEqual([r["content"] for r in self.store.get_state(ws_b["id"])["records"]],
                         ["乙-1", "乙-2"])

    # ------------------------------------------------- 已污染发布的收敛

    def _legacy_schema(self):
        """按旧版本结构重建库（旧清单仅按 target_version 唯一，跨区共享）。"""
        import json as _json  # noqa: F401
        import hashlib  # noqa: F401
        from datetime import datetime, timezone

        def ts():
            return datetime.now(timezone.utc).isoformat(timespec="milliseconds")

        self.store.close()
        conn = __import__("sqlite3").connect(self.db_path)
        conn.executescript(
            "DROP TABLE IF EXISTS workspaces; DROP TABLE IF EXISTS epochs; "
            "DROP TABLE IF EXISTS records; DROP TABLE IF EXISTS migration_manifests; "
            "DROP TABLE IF EXISTS pages;")
        conn.executescript("""
CREATE TABLE workspaces (id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE, created_at TEXT NOT NULL,
  current_epoch_id TEXT, migration_phase TEXT NOT NULL DEFAULT 'idle',
  migration_target_version TEXT, migration_candidate_epoch_id TEXT,
  migration_source_epoch_id TEXT, migration_owner_page_id TEXT,
  migration_copied INTEGER NOT NULL DEFAULT 0, migration_total INTEGER NOT NULL DEFAULT 0,
  migration_started_at TEXT, migration_updated_at TEXT, migration_error TEXT);
CREATE TABLE epochs (id TEXT PRIMARY KEY, workspace_id TEXT NOT NULL, number INTEGER NOT NULL,
  version TEXT NOT NULL, kind TEXT NOT NULL, created_at TEXT NOT NULL, UNIQUE (workspace_id, number));
CREATE TABLE records (id TEXT PRIMARY KEY, workspace_id TEXT NOT NULL, epoch_id TEXT NOT NULL,
  seq INTEGER NOT NULL, content TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE TABLE migration_manifests (target_version TEXT PRIMARY KEY, source_epoch_id TEXT NOT NULL,
  record_count INTEGER NOT NULL, record_checksum TEXT NOT NULL, records_json TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE TABLE pages (id TEXT PRIMARY KEY, workspace_id TEXT NOT NULL, epoch_id TEXT NOT NULL,
  state TEXT NOT NULL, created_at TEXT NOT NULL, last_seen TEXT NOT NULL, closed_at TEXT, invalidated_at TEXT);
""")
        return conn, ts

    def test_published_cross_workspace_contamination_converges_on_reopen(self):
        """旧版本已发布的跨区污染：重开后按本工作区源纪元收敛，新增保留、纪元不回退。"""
        import time
        conn, ts = self._legacy_schema()
        t0 = ts()
        conn.execute("INSERT INTO workspaces VALUES ('wA','甲',?,'eA2','published','v2',NULL,'eA1',NULL,2,2,?,?,NULL)", (t0, t0, t0))
        conn.execute("INSERT INTO workspaces VALUES ('wB','乙',?,'eB2','published','v2',NULL,'eB1',NULL,2,2,?,?,NULL)", (t0, t0, t0))
        for args in (('eA1', 'wA', 1, 'v1', 'superseded'), ('eA2', 'wA', 2, 'v2', 'published'),
                     ('eB1', 'wB', 1, 'v1', 'superseded'), ('eB2', 'wB', 2, 'v2', 'published')):
            conn.execute("INSERT INTO epochs VALUES (?,?,?,?,?,?)", (*args, t0))
        for i, content in enumerate(("甲-1", "甲-2"), 1):
            conn.execute("INSERT INTO records VALUES (?,?,?,?,?,?)", (f"a{i}", 'wA', 'eA1', i, content, t0))
            conn.execute("INSERT INTO records VALUES (?,?,?,?,?,?)", (f"aa{i}", 'wA', 'eA2', i, content, t0))
        for i, content in enumerate(("乙-1", "乙-2"), 1):
            conn.execute("INSERT INTO records VALUES (?,?,?,?,?,?)", (f"b{i}", 'wB', 'eB1', i, content, t0))
        # 乙的新纪元错误地装了甲的冻结记录（seq 1-2），其后乙又新增了一条
        conn.execute("INSERT INTO records VALUES (?,?,?,?,?,?)", ("bb1", 'wB', 'eB2', 1, "甲-1", t0))
        conn.execute("INSERT INTO records VALUES (?,?,?,?,?,?)", ("bb2", 'wB', 'eB2', 2, "甲-2", t0))
        time.sleep(0.01)
        t_new = ts()
        conn.execute("INSERT INTO records VALUES (?,?,?,?,?,?)", ("bb3", 'wB', 'eB2', 3, "乙新增", t_new))
        import json as _json, hashlib
        snap = _json.dumps([{"seq": 1, "content": "甲-1", "created_at": t0},
                            {"seq": 2, "content": "甲-2", "created_at": t0}],
                           ensure_ascii=False, separators=(",", ":"))
        h = hashlib.sha256()
        for i, content in enumerate(("甲-1", "甲-2"), 1):
            h.update(f"{i}|".encode()); h.update(content.encode()); h.update(b"\n")
        conn.execute("INSERT INTO migration_manifests VALUES (?,?,?,?,?,?)",
                     ("v2", "eA1", 2, h.hexdigest(), snap, t0))
        conn.execute("INSERT INTO pages VALUES (?,?,?, 'invalidated', ?, ?, NULL, ?)",
                     ("pBold", 'wB', 'eB1', t0, t0, t0))
        conn.commit(); conn.close()

        self.store = Store(self.db_path, page_ttl_seconds=45)
        sb = self.store.get_state("wB")
        # 冻结段还原为乙自身记录（顺序/内容/时间同乙源纪元），新增顺延保留
        self.assertEqual([(r["seq"], r["content"]) for r in sb["records"]],
                         [(1, "乙-1"), (2, "乙-2"), (3, "乙新增")])
        src_times = dict(self.store.conn.execute(
            "SELECT content, created_at FROM records WHERE epoch_id='eB1'").fetchall())
        for r in sb["records"][:2]:
            self.assertEqual(r["created_at"], src_times[r["content"]])
        # 纪元不回退、旧页不复活
        self.assertEqual(sb["current_epoch"]["number"], 2)
        self.assertEqual({p["id"]: p["state"] for p in sb["pages"]}["pBold"], "invalidated")
        # 甲完全不变
        sa = self.store.get_state("wA")
        self.assertEqual([r["content"] for r in sa["records"]], ["甲-1", "甲-2"])
        # 再次重开：幂等，不重复收敛
        self.store.close()
        self.store = Store(self.db_path, page_ttl_seconds=45)
        self.assertEqual([(r["seq"], r["content"]) for r in self.store.get_state("wB")["records"]],
                         [(1, "乙-1"), (2, "乙-2"), (3, "乙新增")])

    def test_foreign_inflight_validating_is_neutralized_on_upgrade(self):
        """旧库中乙在 validating/publishing 持有他区清单：升级后候选回收、可安全重发。"""
        conn, ts = self._legacy_schema()
        t0 = ts()
        conn.execute("INSERT INTO workspaces VALUES ('wA','甲',?,'eA2','published','v2',NULL,'eA1',NULL,1,1,?,?,NULL)", (t0, t0, t0))
        conn.execute("INSERT INTO epochs VALUES ('eA1','wA',1,'v1','superseded',?)", (t0,))
        conn.execute("INSERT INTO epochs VALUES ('eA2','wA',2,'v2','published',?)", (t0,))
        conn.execute("INSERT INTO records VALUES (?,?,?,?,?,?)", ("a1", 'wA', 'eA1', 1, "甲-1", t0))
        conn.execute("INSERT INTO records VALUES (?,?,?,?,?,?)", ("aa1", 'wA', 'eA2', 1, "甲-1", t0))
        import json as _json, hashlib
        snap = _json.dumps([{"seq": 1, "content": "甲-1", "created_at": t0}])
        h = hashlib.sha256(); h.update(b"1|"); h.update("甲-1".encode()); h.update(b"\n")
        conn.execute("INSERT INTO migration_manifests VALUES (?,?,?,?,?,?)",
                     ("v2", "eA1", 1, h.hexdigest(), snap, t0))
        # 乙在 validating，候选装的是甲的记录
        conn.execute("INSERT INTO workspaces VALUES ('wB','乙',?,'eB1','validating','v2','eB2','eB1',NULL,1,1,?,?,NULL)", (t0, t0, t0))
        conn.execute("INSERT INTO epochs VALUES ('eB1','wB',1,'v1','published',?)", (t0,))
        conn.execute("INSERT INTO epochs VALUES ('eB2','wB',2,'v2','candidate',?)", (t0,))
        conn.execute("INSERT INTO records VALUES (?,?,?,?,?,?)", ("b1", 'wB', 'eB1', 1, "乙-1", t0))
        conn.execute("INSERT INTO records VALUES (?,?,?,?,?,?)", ("bc1", 'wB', 'eB2', 1, "甲-1", t0))
        conn.commit(); conn.close()

        self.store = Store(self.db_path, page_ttl_seconds=45)
        sb = self.store.get_state("wB")
        self.assertEqual(sb["migration"]["phase"], "aborted")
        self.assertIsNone(sb["migration"]["candidate_epoch"])
        self.assertEqual([r["content"] for r in sb["records"]], ["乙-1"])
        # 可重新发起并完成，这次携带乙自己的冻结记录
        page = self.store.open_page("wB")["page_id"]
        self.drive_version("wB", page, "v2")
        self.assertEqual([r["content"] for r in self.store.get_state("wB")["records"]], ["乙-1"])


if __name__ == "__main__":
    unittest.main()
