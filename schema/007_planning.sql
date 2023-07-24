create table planning_coverage(
  uuid uuid primary key not null,
  created timestamptz not null,
  modified timestamptz not null,
  title text not null,
  description text not null,
  status text not null,
  public bool not null,
  starts date not null,
  ends date,
  data jsonb not null,
  units text[] not null
);

create index planning_coverage_date_range_idx on planning_coverage(starts, ends);

create table planning_item(
  uuid uuid primary key not null,
  created timestamptz not null,
  modified timestamptz not null,
  title text not null,
  description text not null,
  status text not null,
  public bool not null,
  publish timestamptz,
  publish_slot smallint,
  data jsonb not null,
  units text[] not null,
  coverage uuid,
  foreign key(coverage) references planning_coverage(uuid) on delete restrict
);

create index planning_item_publish_idx on planning_item(publish);
create index planning_item_publish_slot_idx on planning_item(publish_slot);

create table planning_item_deliverable(
  planning_item uuid not null,
  document uuid not null,
  primary key(planning_item, document),
  foreign key(planning_item) references planning_item(uuid) on delete cascade,
  foreign key(document) references document(uuid) on delete cascade
);

create table user_reference(
  uuid uuid primary key not null,
  external_id text not null unique,
  name text not null,
  avatar_url text not null
);

create table planning_assignment(
  uuid uuid primary key,
  created timestamptz not null,
  modified timestamptz not null,
  planning_item uuid not null,
  status text not null,
  starts timestamptz not null,
  ends timestamptz,
  full_day boolean not null,
  kind text[] not null,
  description text not null,
  foreign key(planning_item) references planning_item(uuid) on delete cascade
);

create index planning_assignment_kind_idx on planning_assignment using GIN(kind);

create table planning_assignee(
  assignee uuid not null,
  assignment uuid not null,
  role text not null,
  primary key(assignee, assignment),
  foreign key(assignee) references user_reference(uuid) on delete cascade,
  foreign key(assignment) references planning_assignment(uuid) on delete cascade
);

---- create above / drop below ----

drop table planning_assignee;
drop table planning_assignment;
drop table planning_item_deliverable;
drop table user_reference;
drop table planning_item;
drop table planning_coverage;
