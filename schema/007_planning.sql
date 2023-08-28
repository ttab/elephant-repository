create table planning_item(
  uuid uuid primary key not null,
  version bigint not null,
  title text not null,
  description text not null,
  public bool not null,
  tentative bool not null,
  date date not null,
  urgency smallint,
  event uuid,
  foreign key(uuid) references document(uuid) on delete cascade
);

create index planning_item_event_idx on planning_item(event);

create table planning_assignment(
  uuid uuid primary key,
  version bigint not null,
  planning_item uuid not null,
  status text not null,
  publish timestamptz,
  publish_slot smallint,
  starts timestamptz not null,
  ends timestamptz,
  full_day boolean not null,
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
  foreign key(assignment) references planning_assignment(uuid) on delete cascade,
  foreign key(document) references document(uuid) on delete cascade
);

create table user_reference(
  uuid uuid primary key not null,
  external_id text not null unique,
  name text not null,
  avatar_url text not null
);

create table planning_assignee(
  assignment uuid not null,
  assignee uuid not null,
  version bigint not null,
  role text not null,
  primary key(assignment, assignee),
  foreign key(assignee) references user_reference(uuid) on delete cascade,
  foreign key(assignment) references planning_assignment(uuid) on delete cascade
);

---- create above / drop below ----

drop table if exists planning_assignee;
drop table if exists planning_deliverable;
drop table if exists user_reference;
drop table if exists planning_assignment;
drop table if exists planning_item;
