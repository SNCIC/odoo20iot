-- Persist whether a device type is a gateway so PG-backed authentication
-- can build the same ACL as the file-backed directory.
ALTER TABLE t_device_type
    ADD COLUMN IF NOT EXISTS is_gateway BOOLEAN NOT NULL DEFAULT false;
