package shop;

import org.springframework.jdbc.core.JdbcTemplate;

public class InvoiceOutbox {
    private final JdbcTemplate jdbc;

    public InvoiceOutbox(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void issue(String invoiceId) {
        jdbc.update("INSERT INTO event_queue (event_type, body) VALUES ('invoice.issued', ?)", invoiceId);
    }
}
