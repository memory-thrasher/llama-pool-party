-- Bootstrap admin user (D-J4: recovery path for system admins).
-- Idempotent: re-running never duplicates or changes existing rows.
-- Token is stored as sha256 hex of the literal string 'admin' (D-J2: hash only).
--
--   echo 'admin' | sha256sum
--   8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918

insert into users (id, email, display_name, role, status, activated_at)
values (gen_random_uuid(), 'admin', 'admin', 'admin', 'active', now())
on conflict (email) do nothing;

-- The bootstrap token row: name 'default', token_hash = sha256('admin').
insert into user_tokens (id, user_id, name, token_hash)
select gen_random_uuid(), id, 'default', '8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918'
from users
where email = 'admin'
on conflict do nothing;
