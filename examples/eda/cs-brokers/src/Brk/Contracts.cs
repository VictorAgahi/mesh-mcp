namespace Brk;

public record OrderCreated(Guid OrderId);
public record OrderShipped(Guid OrderId);
public record PaymentSettled(Guid PaymentId);
public record InvoiceIssued(Guid InvoiceId);
public record PlaceOrder(Guid OrderId);
public record OrderPlaced(Guid OrderId);
public record BillOrder(Guid OrderId);
