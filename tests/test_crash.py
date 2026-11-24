from support import EngineTest


class CrashTest(EngineTest):
    def test_dirty_eviction_and_repeated_recovery(self):
        a = self.connect()
        self.ok(a, "CREATE TABLE items (id INT, tag TEXT);")
        self.ok(a, "INSERT INTO items VALUES (-1, 'saved');")
        loser = self.ok(a, "BEGIN;").split()[-1]
        self.ok(a, "UPDATE items SET tag = 'uncommitted' WHERE id = -1;")
        self.ok(a, "INSERT INTO items VALUES (63, 'collision');")
        self.ok(a, "SELECT * FROM items;")  # Evict the dirty page from eight frames.
        self.assertIn(b"uncommitted", (self.folder / "core.db").read_bytes())
        self.ok(a, "CREATE TABLE transient (id INT);")
        self.stop()  # SIGKILL while the transaction still holds its locks.
        self.start()
        startup = self.log.read_text()
        print(startup, end="")
        self.assertIn(f"undo 3 records (incomplete txns: {loser})", startup)
        self.assertIn("hash index rebuilt: 1 entries", startup)
        a = self.connect()
        self.assertIn("| saved |", self.ok(a, "SELECT tag FROM items WHERE id = -1;"))
        self.assertIn("(0 rows)", self.ok(a, "SELECT * FROM items WHERE id = 63;"))
        self.assertIn("unknown table", a.ask("SELECT * FROM transient;"))
        self.ok(a, "UPDATE items SET id = 63 WHERE id = -1;")
        self.stop()
        self.start()
        a = self.connect()
        self.assertIn("undo 0 records", self.log.read_text())
        self.assertIn("| saved |", self.ok(a, "SELECT tag FROM items WHERE id = 63;"))
        self.assertEqual(a.ask("INSERT INTO items VALUES (63, 'again');"), "ERROR duplicate key")
