-- Transactional outbox for order events (synthetic fixture, never executed).
-- A comment that says INSERT INTO order_outbox (topic) VALUES ('comment.only') is not a producer.
CREATE TABLE IF NOT EXISTS public.order_outbox (
    id         bigserial   PRIMARY KEY,
    topic      text        NOT NULL,
    payload    jsonb       NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION public.enqueue_order_created() RETURNS trigger AS $$
BEGIN
    INSERT INTO public."order_outbox" (topic, payload)
    VALUES ('order.created', to_jsonb(NEW));
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER orders_after_insert
    AFTER INSERT ON public.orders
    FOR EACH ROW EXECUTE FUNCTION public.enqueue_order_created();

-- Dynamic topic: the key is built at run time, so it is not statically resolved.
CREATE OR REPLACE FUNCTION public.enqueue_row_change() RETURNS trigger AS $$
BEGIN
    insert into order_outbox (payload, topic)
    values (to_jsonb(NEW), TG_TABLE_NAME || '.changed');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION public.announce_stock_changed() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('stock_changed', NEW.sku);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER stock_after_update
    AFTER UPDATE ON public.stock
    FOR EACH ROW EXECUTE FUNCTION public.announce_stock_changed();

-- Not an outbox table, and the topic only appears inside a string literal.
INSERT INTO audit_log (message) VALUES ('INSERT INTO order_outbox (topic) VALUES (''string.only'')');
