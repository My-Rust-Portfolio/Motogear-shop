-- Add migration script here
-- Creates shipment and notification tables.
-- Shipments store simulated delivery details for accepted orders.
-- Notifications store customer-facing messages produced by workers.

CREATE TABLE shipments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id UUID NOT NULL REFERENCES orders (id) ON DELETE RESTRICT,
    tracking_reference TEXT NOT NULL UNIQUE,
    shipped_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_shipments_order_id
        UNIQUE (order_id)
);

CREATE TABLE notifications (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id UUID NOT NULL REFERENCES customers (id) ON DELETE RESTRICT,
    order_id UUID NOT NULL REFERENCES orders (id) ON DELETE RESTRICT,
    source_event_id UUID NOT NULL,
    kind TEXT NOT NULL,
    message TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_notifications_source_event_kind
        UNIQUE (source_event_id, kind)
);

CREATE INDEX idx_notifications_customer_id
    ON notifications (customer_id);

CREATE INDEX idx_notifications_order_id
    ON notifications (order_id);