from support import EngineTest


class BlockingTest(EngineTest):
    def test_readers_share_and_writer_waits_for_commit(self):
        a, b = self.connect(), self.connect()
        self.ok(a, "CREATE TABLE items (id INT);")
        self.ok(a, "BEGIN;")
        self.ok(a, "SELECT * FROM items;")
        self.ok(b, "BEGIN;")
        self.ok(b, "SELECT * FROM items;")
        b.send("INSERT INTO items VALUES (9);")
        self.assertTrue(b.blocked(), "writer passed a shared lock")
        self.ok(a, "COMMIT;")
        self.assertEqual(b.reply(), "OK 1 rows affected")
        a.send("SELECT * FROM items WHERE id = 9;")
        self.assertTrue(a.blocked(), "reader saw an uncommitted write")
        self.ok(b, "ABORT;")
        self.assertIn("(0 rows)", a.reply())
