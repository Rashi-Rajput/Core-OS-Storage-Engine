from support import EngineTest


class DeadlockTest(EngineTest):
    def test_youngest_victim(self):
        a, b = self.connect(), self.connect()
        for table in ("leftside", "rightside"):
            self.ok(a, f"CREATE TABLE {table} (id INT);")
        old = self.ok(a, "BEGIN;").split()[-1]
        young = self.ok(b, "BEGIN;").split()[-1]
        self.ok(a, "INSERT INTO leftside VALUES (1);")
        self.ok(b, "INSERT INTO rightside VALUES (2);")
        b.send("SELECT * FROM leftside;")
        self.assertTrue(b.blocked())
        a.send("INSERT INTO rightside VALUES (2);")
        self.assertEqual(b.reply(), f"ERROR deadlock detected: transaction {young} aborted, locks released")
        self.assertEqual(a.reply(), "OK 1 rows affected")
        self.assertEqual(b.ask("COMMIT;"), "ERROR no active transaction")
        self.assertNotEqual(self.ok(b, "BEGIN;").split()[-1], young)
        self.ok(b, "ABORT;")
        self.ok(a, "COMMIT;")
        self.assertIn("(1 rows)", self.ok(b, "SELECT * FROM rightside WHERE id = 2;"))
        graph = self.log.read_text().split("[DEADLOCK]", 1)[1]
        print("[DEADLOCK]" + graph, end="")
        self.assertIn(f"  T{old} -> T{young}\n", graph)
        self.assertIn(f"  T{young} -> T{old}\n", graph)
        self.assertIn(f"cycle found: T{old} -> T{young} -> T{old}", graph)
        self.assertIn(f"victim: T{young} (youngest), aborting and releasing locks", graph)
