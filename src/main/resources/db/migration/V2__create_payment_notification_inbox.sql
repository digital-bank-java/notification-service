create table payment_event_inbox (
    event_id varchar(200) primary key,
    fingerprint varchar(64) not null,
    event_type varchar(100) not null,
    schema_version varchar(32) not null,
    producer varchar(100) not null,
    occurred_at timestamp with time zone not null,
    aggregate_id varchar(200) not null,
    correlation_id varchar(200) not null,
    causation_id varchar(200) not null,
    instruction_id varchar(200) not null,
    idempotency_key varchar(200) not null,
    amount numeric(19, 4) not null,
    currency varchar(3) not null,
    event_status varchar(32) not null,
    received_at timestamp with time zone not null,
    constraint ck_payment_event_inbox_type check (event_type = 'PaymentInstructionStateChanged.v1'),
    constraint ck_payment_event_inbox_schema check (schema_version = '1.0.0'),
    constraint ck_payment_event_inbox_producer check (producer = 'payment-service'),
    constraint ck_payment_event_inbox_amount check (amount > 0),
    constraint ck_payment_event_inbox_currency check (currency ~ '^[A-Z]{3}$'),
    constraint ck_payment_event_inbox_status check (event_status in ('PENDING', 'COMPLETED', 'FAILED'))
);

create table payment_notification_work (
    event_id varchar(200) primary key,
    instruction_id varchar(200) not null,
    correlation_id varchar(200) not null,
    causation_id varchar(200) not null,
    amount numeric(19, 4) not null,
    currency varchar(3) not null,
    work_type varchar(64) not null,
    status varchar(32) not null,
    created_at timestamp with time zone not null,
    constraint fk_payment_notification_work_inbox
        foreign key (event_id) references payment_event_inbox (event_id),
    constraint uq_payment_notification_work_instruction_status unique (instruction_id, status),
    constraint ck_payment_notification_work_type check (work_type = 'PAYMENT_INSTRUCTION_STATE_CHANGED'),
    constraint ck_payment_notification_work_status check (status in ('PENDING', 'COMPLETED', 'FAILED')),
    constraint ck_payment_notification_work_amount check (amount > 0),
    constraint ck_payment_notification_work_currency check (currency ~ '^[A-Z]{3}$')
);

create table payment_event_quarantine (
    id uuid primary key,
    quarantine_key varchar(64) not null unique,
    event_id varchar(200),
    fingerprint varchar(64),
    event_type varchar(100),
    schema_version varchar(32),
    producer varchar(100),
    aggregate_id varchar(200),
    instruction_id varchar(200),
    correlation_id varchar(200),
    causation_id varchar(200),
    amount numeric(19, 4),
    currency varchar(3),
    event_status varchar(32),
    topic varchar(250),
    kafka_partition integer,
    kafka_offset bigint,
    reason varchar(500) not null,
    quarantined_at timestamp with time zone not null
);

create index ix_payment_event_inbox_received_at on payment_event_inbox (received_at);
create index ix_payment_event_quarantine_event_id on payment_event_quarantine (event_id);
