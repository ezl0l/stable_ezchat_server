import json

import pymysql

from typing import List

ACCESS_TOKEN_LIFETIME = 86400
REFRESH_TOKEN_LIFETIME = 86400 * 7

ACCESS_TOKEN_LENGTH = 64
REFRESH_TOKEN_LENGTH = 128

SCOPES = ['settings', 'profile', 'messages', 'friends']


class AbstractModel:
    TABLE_NAME = None
    SECRET_FIELDS = []

    def __init__(self, **kwargs):
        object.__setattr__(self, 'fields', kwargs)

    def __getattr__(self, item):
        return self.fields[item] if item in self.fields.keys() else None

    def __setattr__(self, key, value):
        self.fields[key] = value

    def update_fields(self, fields: dict):
        object.__getattribute__(self, 'fields').update(fields)

    def _get_keys(self):
        return f"({', '.join(self.fields.keys())})"

    def add(self, db: pymysql.Connection):
        db.ping()
        with db.cursor() as cur:
            sql = f"INSERT INTO `{self.TABLE_NAME}` {self._get_keys()} VALUES ({', '.join(['%s'] * len(self.fields))});"
            cur.execute(
                sql,
                tuple(self.fields.values()))
            self.fields['id'] = cur.lastrowid

    def get_one(self, db: pymysql.Connection):
        db.ping()
        with db.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(f"SELECT * FROM `{self.TABLE_NAME}` WHERE "
                        f"{' AND '.join(f'`{k}`=%s' for k in self.fields.keys())}",
                        tuple(self.fields.values()))
            f = cur.fetchone()
            if f is not None:
                self.fields.update(**dict(f))
                return True
        return False

    def get_many(self, db: pymysql.Connection):
        models = []
        db.ping()
        with db.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(f"SELECT * FROM `{self.TABLE_NAME}` WHERE "
                        f"{' AND '.join(f'`{k}`=%s' for k in self.fields.keys())}",
                        tuple(self.fields.values()))
            for fields in cur.fetchall():
                models.append(self.__class__(**dict(fields)))
        return models

    def delete(self, db: pymysql.Connection):
        db.ping()
        with db.cursor(pymysql.cursors.DictCursor) as cur:
            return cur.execute(
                f"DELETE FROM `{self.TABLE_NAME}` WHERE {' AND '.join(f'`{k}`=%s' for k in self.fields.keys())};",
                tuple(self.fields.values())) > 0

    def update(self, db: pymysql.Connection):
        db.ping()
        with db.cursor(pymysql.cursors.DictCursor) as cur:
            return cur.execute(f"UPDATE `{self.TABLE_NAME}` SET {', '.join(f'`{k}`=%s' for k in self.fields.keys())} "
                               f"WHERE `id`={self.id};",
                               tuple(self.fields.values()))

    def __str__(self):
        return self.serialize()

    def serialize(self, include_secret_fields=False):
        if include_secret_fields:
            return self.fields
        return {k: v for k, v in self.fields.items() if k not in self.SECRET_FIELDS}

    def to_json(self, **kwargs):
        return json.dumps(self.serialize(**kwargs))


class User(AbstractModel):
    TABLE_NAME = "users"

    SECRET_FIELDS = ('password_hash',)
    PROFILE_FIELDS = ('username',)
    SETTINGS_FIELDS = ('password_hash',)
    NEED_FIELDS = ('username', 'password_hash')


class AccessToken(AbstractModel):
    TABLE_NAME = "access_tokens"


class Message(AbstractModel):
    TABLE_NAME = "messages"


class Chat(AbstractModel):
    TABLE_NAME = "chats"


class ChatMember(AbstractModel):
    TABLE_NAME = "chat_members"


class Friend(AbstractModel):
    TABLE_NAME = "friends"


class Attachment(AbstractModel):
    TABLE_NAME = "attachments"

    SECRET_FIELDS = ('path',)


class Event(object):
    class AffectedUsersRecognizerError(Exception):
        pass

    MESSAGE_NEW = 'message_new'
    MESSAGE_CHANGE_STATUS = 'message_change_status'
    MESSAGE_CHAT_NEW_MEMBER = 'message_chat_new_member'
    MESSAGE_TYPING_STATE = 'message_typing_state'

    def __init__(self, _type: str, _object: dict, affected_users: [None, List[int]], db: pymysql.Connection = None):
        self.affected_users = affected_users
        self.object = _object
        self.type = _type

        if self.affected_users is None:
            if self.type in (self.MESSAGE_TYPING_STATE, self.MESSAGE_NEW, self.MESSAGE_CHANGE_STATUS):
                chat = Chat(id=self.object['chat_id'])
                if not chat.get_one(db):
                    raise self.AffectedUsersRecognizerError("Can't find given object")
                chat_members = ChatMember(chat_id=chat.id,
                                          status='DEFAULT').get_many(db)

                self.affected_users = [chat_member.user_id for chat_member in chat_members]

                if self.type == self.MESSAGE_TYPING_STATE:
                    self.affected_users.remove(self.object['from_id'])
            elif self.type == self.MESSAGE_CHAT_NEW_MEMBER:
                self.affected_users = self.object['members']
            else:
                raise self.AffectedUsersRecognizerError()
        print(self.type, self.affected_users)

    def add_viewed_user(self, user_id: int):
        if user_id in self.affected_users:
            self.affected_users.remove(user_id)

    def serialize(self):
        return {'type': self.type,
                'object': self.object}

    def to_json(self):
        return json.dumps(self.serialize())
