CC = gcc
CFLAGS = -std=c11 -g -Wall -pthread
SRC = disk_cache.c page.c hash.c lock.c wal.c txn.c sql.c exec.c server.c

coreos: $(SRC) common.h
	$(CC) $(CFLAGS) $(SRC) -o $@

test: coreos
	python3 -m unittest discover -s tests -v

clean:
	rm -f coreos

.PHONY: test clean
