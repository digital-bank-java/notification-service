create table transfer_event_inbox (
    event_id varchar(200) primary key,
    fingerprint varchar(64) not null,
    correlation_id varchar(200) not null,
    causation_id varchar(200) not null,
    producer varchar(100) not null,
    schema_version varchar(32) not null,
    occurred_at timestamp with time zone not null,
    status varchar(32) not null,
    received_at timestamp with time zone not null,
    constraint ck_transfer_event_inbox_status check (status in ('ACCEPTED'))
);

create table notification_work (
    event_id varchar(200) primary key,
    correlation_id varchar(200) not null,
    causation_id varchar(200) not null,
    work_type varchar(64) not null,
    status varchar(32) not null,
    created_at timestamp with time zone not null,
    constraint fk_notification_work_inbox
        foreign key (event_id) references transfer_event_inbox (event_id),
    constraint ck_notification_work_status check (status in ('PENDING'))
);

create table transfer_event_quarantine (
    id uuid primary key,
    quarantine_key varchar(64) not null unique,
    event_id varchar(200),
    fingerprint varchar(64),
    correlation_id varchar(200),
    causation_id varchar(200),
    producer varchar(100),
    schema_version varchar(32),
    topic varchar(250),
    kafka_partition integer,
    kafka_offset bigint,
    reason varchar(500) not null,
    quarantined_at timestamp with time zone not null
);

create index ix_transfer_event_inbox_received_at on transfer_event_inbox (received_at);
create index ix_transfer_event_quarantine_event_id on transfer_event_quarantine (event_id);
