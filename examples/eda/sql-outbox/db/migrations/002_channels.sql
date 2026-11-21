-- Notification channels used by the workers (psql script, synthetic).
NOTIFY invoice_ready;
NOTIFY "Refund.Requested", 'batch-1';
Select Pg_Notify('invoice_ready', '{}');
listen   Invoice_Ready;
