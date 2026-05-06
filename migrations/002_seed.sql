USE training_msg_queue;

INSERT INTO message_campaigns (name, segment_file_key, status)
VALUES ('Training campaign 001', 'segments/active-users.json', 'pending');
