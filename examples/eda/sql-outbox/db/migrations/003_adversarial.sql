-- Adversarial cases (synthetic fixture, never executed).
-- A bind value inside a multi-row VALUES is not a site; the literal rows are.
INSERT INTO outbox (topic, payload) VALUES ('multi.first', '{}'), ($1, '{}'), ('multi.third', '{}');

-- pg_notify with a channel that is a variable: not statically resolved.
SELECT pg_notify(TG_TABLE_NAME, 'changed');

-- Documentation text in a dollar-quoted string is not a statement.
COMMENT ON FUNCTION public.enqueue_order_created() IS $$NOTIFY doc_only$$;
