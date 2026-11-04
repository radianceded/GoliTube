CREATE TABLE IF NOT EXISTS danmaku (
    id VARBINARY(128) NOT NULL PRIMARY KEY,
    room_id VARBINARY(128) NOT NULL,
    message_type TINYINT UNSIGNED NOT NULL,
    data BLOB NOT NULL,
    stored_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    INDEX room_history (room_id, stored_at, id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS users (
    id VARBINARY(128) NOT NULL PRIMARY KEY,
    username VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL UNIQUE,
    password_hash VARBINARY(60) NOT NULL,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS danmaku_history (
    id VARBINARY(128) NOT NULL PRIMARY KEY,
    room_id VARBINARY(128) NOT NULL,
    content VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    position_ms BIGINT NOT NULL,
    color CHAR(7) CHARACTER SET ascii NOT NULL,
    created_at_ms BIGINT NOT NULL,
    INDEX room_position (room_id, position_ms, id)
) ENGINE=InnoDB;
