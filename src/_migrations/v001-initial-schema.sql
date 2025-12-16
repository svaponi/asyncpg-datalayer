create extension if not exists "uuid-ossp";

create table org (
  org_id uuid primary key not null default uuid_generate_v4(),
  org_name varchar unique not null,
  org_display_name varchar
);

create table user_account (
  user_id uuid primary key not null default uuid_generate_v4(),
  email varchar unique not null,
  email_verified bool not null default false,
  hashed_password varchar,
  full_name varchar
);

create table user_auth (
  user_auth_id uuid primary key not null default uuid_generate_v4(),
  user_id uuid not null references user_account(user_id),
  org_id uuid references org(org_id),
  user_auth_type varchar not null,
  permissions varchar[] not null default array[]::varchar[]
);