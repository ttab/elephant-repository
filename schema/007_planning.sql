create table planning_coverage(
  uuid uuid primary key not null,
  title text not null,
  description text not null,
  status text not null,
  public bool not null,
  starts date not null,
  ends date
);

create index planning_coverage_date_range_idx on planning_coverage(starts, ends);

create table planning_item(
  uuid uuid primary key not null,
  version bigint not null,
  title text not null,
  description text not null,
  public bool not null,
  tentative bool not null,
  date date not null,
  publish timestamptz,
  publish_slot smallint,
  urgency smallint,
  coverage uuid,
  foreign key(uuid) references document(uuid) on delete cascade
);

create index planning_item_coverage_idx on planning_item(coverage);
create index planning_item_publish_idx on planning_item(publish);
create index planning_item_publish_slot_idx on planning_item(publish_slot);

create table planning_assignment(
  uuid uuid primary key,
  version bigint not null,
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

drop index planning_assignment_kind_idx;

drop table planning_assignee;
drop table planning_assignment;
drop table planning_item_deliverable;
drop table user_reference;
drop table planning_item;
drop table planning_coverage;
