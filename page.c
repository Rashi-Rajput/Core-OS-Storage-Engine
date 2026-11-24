#include "common.h"

typedef struct { uint16_t count, free_end; } Header;
typedef struct { uint16_t offset, length; } Slot;
_Static_assert(sizeof(Table) <= IMAGE, "catalog image too large");

// Read the number of allocated directory entries.
int page_slots(int page) {
    Header h;
    memcpy(&h, pin(page), sizeof h);
    unpin(page, 0);
    return h.count;
}

// Find a reusable slot or room for another directory entry.
int free_slot(int page) {
    unsigned char *buf = pin(page);
    Header h;
    memcpy(&h, buf, sizeof h);
    int slot;
    for (slot = 0; slot < h.count; slot++) {
        Slot s;
        memcpy(&s, buf + sizeof h + slot * sizeof s, sizeof s);
        if (!s.length) break;
    }
    int end = h.count ? h.free_end : PAGE;
    if (slot == h.count && (slot == SLOTS ||
        end - ROW_BYTES < (int)(sizeof h + (slot + 1) * sizeof(Slot)))) slot = -1;
    unpin(page, 0);
    return slot;
}

// Copy a row or catalog entry into a WAL-sized image.
Image read_image(int page, int slot) {
    Image img = {0};
    unsigned char *buf = pin(page);
    if (page == 0) {
        img.len = sizeof(Table);
        memcpy(img.data, buf + slot * sizeof(Table), img.len);
    } else {
        Header h;
        memcpy(&h, buf, sizeof h);
        if (slot < h.count) {
            Slot s;
            memcpy(&s, buf + sizeof h + slot * sizeof s, sizeof s);
            img.len = s.length;
            if (img.len > IMAGE || img.len > ROW_BYTES || s.offset > PAGE - img.len) {
                fprintf(stderr, "invalid row on page %d slot %d\n", page, slot);
                exit(1);
            }
            memcpy(img.data, buf + s.offset, img.len);
        }
    }
    unpin(page, 0);
    return img;
}

// Install an image in its original slot, retaining space on deletion.
void put_image(int page, int slot, Image img) {
    unsigned char *buf = pin(page);
    if (page == 0) {
        memcpy(buf + slot * sizeof(Table), img.data, sizeof(Table));
        memcpy(&tables[slot], img.data, sizeof(Table));
    } else {
        Header h;
        Slot s;
        memcpy(&h, buf, sizeof h);
        unsigned char *entry = buf + sizeof h + slot * sizeof s;
        if (slot == h.count) {
            if (!h.count) h.free_end = PAGE;
            h.count++;
            h.free_end -= ROW_BYTES;
            s.offset = h.free_end;
            memcpy(buf, &h, sizeof h);
        } else memcpy(&s, entry, sizeof s);
        s.length = img.len;
        memcpy(entry, &s, sizeof s);
        memcpy(buf + s.offset, img.data, img.len);
    }
    unpin(page, 1);
}
