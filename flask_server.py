import hashlib
import os
import time
import random

import pymysql.cursors

from flask import Flask, request, abort, send_from_directory, jsonify, send_file
from models import *

API_URL = '/api/1.0'

app = Flask(__name__)
app.config['TRAP_HTTP_EXCEPTIONS'] = True
app.config['UPLOAD_FOLDER'] = 'S:\\securechat\\files'
MAX_FILE_SIZE = 5242880  # 5 Mb

events = []


def get_db():
    return pymysql.connect(host='127.0.0.1',
                           user='root',
                           password='root',
                           db='securechat',
                           autocommit=True)


@app.errorhandler(Exception)
def error_handler(error):
    return "", error.code


def require_token(db, scopes=None):
    if scopes is None:
        scopes = []

    tokenString = request.headers.get('Authorization')
    if tokenString is None:
        abort(401)

    token = AccessToken(value=tokenString)
    if not token.get_one(db):
        abort(401)

    token_scopes = set(token.scopes.split(','))
    if len(set(scopes) - token_scopes) > 0:
        abort(403)

    return token


def is_friends(db, user_id1, user_id2):
    friend0 = Friend(user_id=user_id1,
                     friend_id=user_id2)
    friend1 = Friend(user_id=user_id2,
                     friend_id=user_id1)
    return friend0.get_one(db) or friend1.get_one(db)


@app.route(f'{API_URL}/signup', methods=('POST',))
def signup():
    db = get_db()

    if request.json is None:
        abort(400)

    if len({'username', 'password'} - set(request.json.keys())) > 0:
        abort(400)

    username = request.json['username']
    password = request.json['password']

    if len(username) < 5 or len(password) < 6:  # todo
        abort(400)

    user = User(username=username)
    if user.get_one(db):
        abort(409)

    user = User(username=username,
                password_hash=hashlib.sha256(password.encode('utf-8')).hexdigest(),
                time=round(time.time()))
    user.add(db)

    user.get_one(db)

    return user.serialize(include_secret_fields=False)


@app.route(f'{API_URL}/token', methods=('POST',))
def token_handler():
    db = get_db()

    print(request.json)

    if request.json is None:
        abort(400)

    grant_type = request.args.get('grant_type', type=str)
    if grant_type not in ('password',):
        abort(400)

    if 'username' not in request.json or 'password' not in request.json:
        abort(400)

    username = request.json['username']
    password = request.json['password']

    if not (password and username):
        abort(400)

    password = hashlib.sha256(password.encode('utf-8')).hexdigest()

    user = User(username=username,
                password_hash=password)

    if not user.get_one(db):
        abort(401)

    value = ''.join(random.choice('qwertyuiopasdfghjklzxcvbnm1234567890') for _ in range(ACCESS_TOKEN_LENGTH))

    token = AccessToken(user_id=user.id,
                        value=value,
                        scopes=','.join(SCOPES),
                        expired=0,
                        time=round(time.time()))
    token.add(db)

    return token.serialize()


@app.route(f'{API_URL}/users/isBusy/<string:username>', methods=('GET',))
def users_is_busy(username):
    return {'is_busy': User(username=username).get_one(get_db())}


@app.route(f'{API_URL}/users', methods=('GET',))
def users_handler():
    db = get_db()

    require_token(db)

    count = request.args.get('count', type=int)
    if count is None:
        count = 20
    offset = request.args.get('offset', type=int)
    if offset is None:
        offset = 0

    users = []
    with db.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute("SELECT * FROM `users` LIMIT %s, %s;", (offset, count))
        for user in cur.fetchall():
            users.append(User(**user).serialize(include_secret_fields=False))
    return jsonify(users)


@app.route(f'{API_URL}/users/<int:user_id>', methods=('GET',))
def users_by_id_handler(user_id):
    db = get_db()

    require_token(db)

    user = User(id=user_id)
    if not user.get_one(db):
        abort(404)

    return user.serialize(include_secret_fields=False)


@app.route(f'{API_URL}/users', methods=('POST',))
def users_post():
    db = get_db()

    if request.json is None:
        abort(400)

    data = {k: v for k, v in request.json.items() if k in User.NEED_FIELDS}
    if len(data) < len(User.NEED_FIELDS):
        abort(400)

    sameUsername = User(username=request.json['username'])
    if sameUsername.get_one(db):
        abort(409)

    user = User(time=round(time.time()), **data)
    user.add(db)

    return user.serialize(include_secret_fields=False)


@app.route(f'{API_URL}/users/profile', methods=('PUT',))
def users_put():
    db = get_db()

    token = require_token(db, ['profile'])

    if request.json is None:
        abort(400)

    data = {k: v for k, v in request.json.items() if k in User.PROFILE_FIELDS}
    if len(data) < len(User.PROFILE_FIELDS):
        abort(400)

    user = User(id=token.user_id, **data)
    user.update(db)

    return user.serialize(include_secret_fields=False)


@app.route(f'{API_URL}/users/search/<string:username>', methods=('GET',))
def users_search(username):
    db = get_db()

    token = require_token(db, ['friends'])

    if len(username) < 3:
        abort(400)

    with db.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(f"SELECT * FROM `{User.TABLE_NAME}` WHERE `username` LIKE %s;", f'{username}%')
        users = [User(**kwargs).serialize(include_secret_fields=False) for kwargs in cur.fetchall()]

    return jsonify(users)


@app.route(f'{API_URL}/messages/chats/search/<string:title>', methods=('GET',))
def chats_search(title):
    db = get_db()

    token = require_token(db, ['messages'])

    if len(title) < 2:
        abort(400)

    with db.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(f"SELECT `{Chat.TABLE_NAME}`.* FROM `{Chat.TABLE_NAME}`, `{ChatMember.TABLE_NAME}` "
                    f"WHERE `{Chat.TABLE_NAME}`.`title` LIKE %s AND "
                    f"`{ChatMember.TABLE_NAME}`.`chat_id` = `{Chat.TABLE_NAME}`.`id` AND "
                    f"`{ChatMember.TABLE_NAME}`.`user_id` = %s;", (f'{title}%', token.user_id))
        chats = [Chat(**kwargs).serialize(include_secret_fields=False) for kwargs in cur.fetchall()]

    return jsonify(chats)


@app.route(f'{API_URL}/messages/send', methods=['POST'])
def messages_send():
    db = get_db()

    token = require_token(db, ['messages'])

    if request.json is None:
        abort(400)

    print(request.json)

    data_keys = request.json.keys()

    if 'chat_id' not in data_keys or ('content' not in data_keys and 'attachment' not in data_keys):
        abort(400)

    if not isinstance(request.json['chat_id'], int):
        abort(400)

    message = Message(user_id=token.user_id,
                      chat_id=request.json['chat_id'],
                      time=round(time.time()))

    if 'attachment' in data_keys:
        for attachment_cleartext in request.json['attachment'].strip().split(','):
            attachment_splitted = attachment_cleartext.split('_')
            if len(attachment_splitted) != 3:
                abort(400)

            a_type = attachment_splitted[0].lower()
            if a_type not in ('photo', 'audio', 'video', 'doc'):
                abort(400)

            a_user_id = attachment_splitted[1]
            a_id = attachment_splitted[2]
            if not a_user_id.isnumeric() or not a_id.isnumeric():
                abort(400)

            attachment = Attachment(id=a_id,
                                    user_id=a_user_id,
                                    type=a_type)
            if not attachment.get_one(db):
                abort(404)

        message.attachment = request.json['attachment']

    if 'content' in data_keys:
        message.content = request.json['content'].strip()

    message.add(db)

    message.get_one(db)

    events.append(Event(Event.MESSAGE_NEW, message.serialize(), None, db=db))
    print(message.serialize())
    return message.serialize()


@app.route(f'{API_URL}/messages/chat/markAsRead/<int:chat_id>', methods=['POST'])
def messages_chat_read(chat_id):
    db = get_db()

    token = require_token(db, ['messages'])
    chat_member = ChatMember(user_id=token.user_id,
                             chat_id=chat_id,
                             status='DEFAULT')
    if not chat_member.get_one(db):
        abort(403)

    with db.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute("UPDATE `messages` SET `status`='READ' WHERE `chat_id`=%s AND `status`='UNREAD';",
                    chat_id)
        cur.execute("SELECT * FROM `messages` WHERE `status`='READ' AND `chat_id`=%s ORDER BY `time` DESC LIMIT 1;")

        last_message = Message(**cur.fetchone())

    events.append(Event(Event.MESSAGE_CHANGE_STATUS,
                        last_message.serialize(), None, db=db))

    return 200


@app.route(f'{API_URL}/messages/markAsRead/<int:message_id>', methods=['POST'])
def messages_read(message_id):
    db = get_db()

    token = require_token(db, ['messages'])

    message = Message(id=message_id)
    if not message.get_one(db):
        abort(404)

    chat_member = ChatMember(user_id=token.user_id,
                             chat_id=message.chat_id,
                             status='DEFAULT')
    if not chat_member.get_one(db):
        abort(403)

    with db.cursor(pymysql.cursors.DictCursor) as cur:
        print(
            cur.execute("UPDATE `messages` SET `status`='READ' WHERE `chat_id`=%s AND `id` < %s AND `status`='UNREAD';",
                        (message.chat_id, message.id)))

    events.append(Event(Event.MESSAGE_CHANGE_STATUS,
                        message.serialize(), None, db=db))

    return message.serialize(include_secret_fields=False)


@app.route(f'{API_URL}/messages/getByChatId/<int:chat_id>', methods=['GET', 'POST'])
def messages_get_by_chat_id(chat_id):
    db = get_db()

    token = require_token(db, ['messages'])

    count = request.args.get('count', type=int)
    if count is None:
        count = 100
    offset = request.args.get('offset', type=int)
    if offset is None:
        offset = 0

    chat = Chat(id=chat_id)
    if not chat.get_one(db):
        abort(404)

    chat_member = ChatMember(user_id=token.user_id,
                             chat_id=chat.id,
                             status='DEFAULT')
    if not chat_member.get_one(db):
        abort(403)

    with db.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute("SELECT * FROM `messages` WHERE `chat_id`=%s AND `status` != 'DELETED' ORDER BY `time` "
                    "DESC LIMIT %s, %s;",
                    (chat.id, offset, count))
        return json.dumps([Message(**kwargs).serialize(include_secret_fields=False)
                           for kwargs in reversed(cur.fetchall())])


@app.route(f'{API_URL}/messages/getById/<string:message_ids>', methods=['GET'])
def messages_get_by_ids(message_ids):
    db = get_db()

    token = require_token(db, ['messages'])

    message_ids = message_ids.split(',')
    if len(message_ids) > 100:
        abort(400)

    requested_chat_members = {}
    messages = []
    for message_id in message_ids:
        message = Message(id=message_id)
        if not message.get_one(db):
            continue

        if message.chat_id in requested_chat_members.keys():
            chat_member = requested_chat_members[message.chat_id]
            if chat_member is None:
                continue
        else:
            chat_member = ChatMember(user_id=token.user_id,
                                     chat_id=message.chat_id,
                                     status='DEFAULT')
            if not chat_member.get_one(db):
                requested_chat_members[message.chat_id] = None
                continue
            requested_chat_members[message.chat_id] = chat_member

        messages.append(message.serialize(include_secret_fields=False))
    return jsonify(messages)


@app.route(f'{API_URL}/messages/getChatById/<int:chat_id>', methods=['GET'])
def messages_get_chat_by_id(chat_id):
    db = get_db()

    token = require_token(db, ['messages'])

    extended = request.args.get('extended', type=int)
    if extended is None or extended != 1:
        extended = 0

    chat = Chat(id=chat_id)
    if not chat.get_one(db):
        abort(404)

    if not ChatMember(user_id=token.user_id,
                      chat_id=chat.id).get_one(db):  # todo (may need to set status=DEFAULT) !
        abort(403)

    chat_serialized = chat.serialize(include_secret_fields=False)
    if extended == 1:
        with db.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute("SELECT `users`.* FROM `users`, `chat_members` WHERE `users`.`id` = `chat_members`.`user_id` "
                        "AND `chat_members`.`chat_id` = %s AND `chat_members`.`status` = 'DEFAULT';", chat.id)
            chat_serialized['members'] = [User(**kwargs).serialize(include_secret_fields=False)
                                          for kwargs in cur.fetchall()]

    return chat_serialized


@app.route(f'{API_URL}/messages/getChats', methods=['GET'])
def messages_get_chats():
    db = get_db()

    token = require_token(db, ['messages'])

    extended = request.args.get('extended', type=int)
    if extended is None or extended != 1:
        extended = 0

    with db.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute("SELECT `chats`.* FROM `chats`, `chat_members` WHERE `chat_members`.`chat_id`=`chats`.`id` AND "
                    "`chat_members`.`user_id`=%s AND `chat_members`.`status`='DEFAULT';", token.user_id)
        chats = [Chat(**kwargs).serialize(include_secret_fields=False) for kwargs in cur.fetchall()]

        if extended == 1:
            for chat in chats:
                cur.execute("SELECT `users`.* FROM `users`, `chat_members` WHERE `chat_members`.`chat_id` = %s "
                            "AND `users`.`id` = `chat_members`.`user_id`;", chat['id'])
                chat['members'] = [User(**kwargs).serialize(include_secret_fields=False) for kwargs in cur.fetchall()]
                if chat['title'] is None and len(chat['members']) == 2:
                    for member in chat['members']:
                        if member['id'] != token.user_id:
                            chat['title'] = member['username']
                            break

                if cur.execute("SELECT * FROM `messages` WHERE `chat_id` = %s ORDER BY `time` DESC LIMIT 1;",
                               chat['id']) > 0:
                    chat['last_message'] = Message(**cur.fetchone()).serialize(include_secret_fields=False)
                else:
                    chat['last_message'] = None

        chats.sort(key=lambda c: 0 if c['last_message'] is None else c['last_message']['time'], reverse=True)

        return jsonify(chats)


@app.route(f'{API_URL}/messages/createChat', methods=['POST'])
def messages_create_chat():
    db = get_db()

    token = require_token(db, ['messages'])

    if request.json is None:
        abort(400)

    if len({'user_ids', 'title'} - set(request.json.keys())) > 0:
        abort(400)

    user_ids = request.json['user_ids']
    if isinstance(user_ids, str):
        user_ids = [user_id for user_id in user_ids.split(',') if user_id.isnumeric()]

    title = request.json['title']

    if len(user_ids) < 2 or len(title) == 0:
        abort(400)

    chat = Chat(title=title,
                status='DEFAULT',
                time=round(time.time()))
    chat.add(db)

    if token.user_id not in user_ids:
        user_ids.append(token.user_id)

    for user_id in user_ids:
        user = User(id=user_id)
        if not user.get_one(db):
            continue

        if not is_friends(db, token.user_id, user_id):
            continue
        chat_member = ChatMember(chat_id=chat.id,
                                 user_id=user_id,
                                 status='DEFAULT',
                                 time=round(time.time()))
        chat_member.add(db)
    real_chat_member_ids = [cm.user_id for cm in ChatMember(chat_id=chat.id, status='DEFAULT').get_many(db)]

    events.append(Event(Event.MESSAGE_CHAT_NEW_MEMBER, {'members': real_chat_member_ids,
                                                        'chat_id': chat.id},
                        real_chat_member_ids))

    return chat.serialize(include_secret_fields=False), 201


@app.route(f'{API_URL}/messages/setActivity', methods=['POST'])
def messages_set_activity():
    db = get_db()

    token = require_token(db, ['messages'])

    if request.json is None:
        abort(400)

    if len({'type', 'chat_id'} - set(request.json.keys())) > 0:
        abort(400)

    events.append(Event(Event.MESSAGE_TYPING_STATE, {'from_id': token.user_id,
                                                     'chat_id': request.json['chat_id'],
                                                     'state': 'typing'}, None, db=db))

    return "", 200


@app.route(f'{API_URL}/event', methods=['GET'])
def event():
    db = get_db()

    wait = request.args.get('wait', type=int)
    if wait is None:
        wait = 25
    elif not (0 < wait <= 90):
        abort(400)

    token = require_token(db)

    start_time = time.time()

    user_events = []
    while time.time() - start_time < wait:
        events_copy = list(events)
        for _event in events_copy:
            if token.user_id in _event.affected_users:
                _event.add_viewed_user(token.user_id)
                user_events.append(_event.serialize())
                if len(_event.affected_users) == 0:
                    events.remove(_event)
        if len(user_events) > 0:
            break
        time.sleep(0.5)
    print(user_events)
    return jsonify(user_events)


@app.route(f'{API_URL}/attachments/upload', methods=['POST'])
def attachments_upload():
    db = get_db()

    token = require_token(db)

    file = request.files.get('file')
    if file is None:
        abort(400)

    ext = file.filename.split('.')[-1]

    attachment_type = 'doc'
    if ext in ('mp3', 'ogg'):
        attachment_type = 'audio'
    elif ext in ('jpg', 'png', 'gif'):
        attachment_type = 'photo'
    elif ext in ('mp4', 'avi'):
        attachment_type = 'video'

    file_data = file.read(MAX_FILE_SIZE + 1)
    if len(file_data) == MAX_FILE_SIZE + 1:
        abort(400)

    attachment_hash = hashlib.md5(file_data).hexdigest()

    same_attachment = Attachment(hash=attachment_hash)
    if same_attachment.get_one(db):
        same_attachment.url = f"{API_URL}/files/{same_attachment.hash}"
        return same_attachment.serialize(include_secret_fields=False), 200

    attachment_path = os.path.join(app.config['UPLOAD_FOLDER'],
                                   f'{attachment_hash}.{file.filename.split(".")[-1]}')

    with open(attachment_path, 'wb') as f:
        f.write(file_data)

    attachment = Attachment(user_id=token.user_id,
                            orig_name=file.filename,
                            type=attachment_type,
                            path=attachment_path,
                            hash=attachment_hash,
                            time=round(time.time()))
    attachment.add(db)

    attachment.url = f"{API_URL}/files/{attachment.hash}"

    return attachment.serialize(include_secret_fields=False), 201


@app.route(f'{API_URL}/attachments/get/<int:attachment_id>', methods=['GET'])
def attachments_get(attachment_id):
    db = get_db()

    require_token(db)

    attachment = Attachment(id=attachment_id)
    if not attachment.get_one(db):
        abort(404)

    data = attachment.serialize(include_secret_fields=False)
    data['url'] = f"files/{attachment.hash}"

    return data, 200


@app.route(f'{API_URL}/files/<string:attachment_hash>', methods=['GET'])
def files_get(attachment_hash):
    db = get_db()

    attachment = Attachment(hash=attachment_hash)
    if not attachment.get_one(db):
        abort(404)

    if not os.path.exists(attachment.path):
        abort(404)

    try:
        return send_file(attachment.path)
    except FileNotFoundError:
        abort(404)


if __name__ == '__main__':
    app.run(host='51.210.128.122', port=8083, debug=True, threaded=True)
