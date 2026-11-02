CREATE TABLE IF NOT EXISTS danmaku (
    id VARBINARY(128) NOT NULL PRIMARY KEY,
    room_id VARBINARY(128) NOT NULL,
    message_type TINYINT UNSIGNED NOT NULL,
    data BLOB NOT NULL,
    stored_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    INDEX room_history (room_id, stored_at, id)
) ENGINE=InnoDB;
