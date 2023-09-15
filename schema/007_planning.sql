create table planning_item(
  uuid uuid primary key not null,
  version bigint not null,
  title text not null,
  description text not null,
  public bool not null,
  tentative bool not null,
  start_date date not null,
  end_date date not null,
  priority smallint,
  event uuid,
  foreign key(uuid) references document(uuid) on delete cascade
);

create index planning_item_event_idx on planning_item(event);

create table planning_assignment(
  uuid uuid primary key,
  version bigint not null,
  planning_item uuid not null,
  status text,
  publish timestamptz,
  publish_slot smallint,
  starts timestamptz not null,
  ends timestamptz,
  start_date date not null,
  end_date date not null,
  full_day boolean not null,
  public boolean not null,
  kind text[] not null,
  description text not null,
  foreign key(planning_item) references planning_item(uuid) on delete cascade
);

create index planning_assignment_kind_idx on planning_assignment using GIN(kind);
create index planning_assignment_publish_idx on planning_assignment(publish);
create index planning_assignment_publish_slot_idx on planning_assignment(publish_slot);

create table planning_deliverable(
  assignment uuid not null,
  document uuid not null,
  version bigint not null,
  primary key(assignment, document),
  foreign key(assignment) references planning_assignment(uuid) on delete cascade
);

create index planning_deliverable_idx on planning_deliverable(document);

create table planning_assignee(
  assignment uuid not null,
  assignee uuid not null,
  version bigint not null,
  role text not null,
  primary key(assignment, assignee),
  foreign key(assignment) references planning_assignment(uuid) on delete cascade
);

create index planning_assignee_idx on planning_assignee(assignee);

---- create above / drop below ----

drop table if exists planning_assignee;
drop table if exists planning_deliverable;
drop table if exists user_reference;
drop table if exists planning_assignment;
drop table if exists planning_item;
