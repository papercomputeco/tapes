-- The trace reads stay correct without the index; each page goes back to
-- sorting the whole trace.
DROP INDEX IF EXISTS spans_20260615_page_order_idx;
