# -*- coding: utf-8 -*-

# Enable SHA-256 sum
import hashlib

#----------------- Common Functions -----------------#


def escape(html):
    """
    Replaces ampresands, quotes and carets in the given HTML with their safe versions.

    :return: Escaped HTML
    """
    return html.replace('&', '&amp;').replace('<', '&lt;').replace(' > ', '&gt;').replace('"', '&quot;').replace("'", '&#39;')


#----------------- User Functions -------------------#


def check_username(db, username):
    """
    Checks if given username exists in the database.

    :param username: username to check
    :return: True if username is in the database, False otherwise
    """

    if db(db.users.username == username).select():
        return True
    return False


def check_password(db, username, password):
    """
    Check if given password correspond to given user.

    :param username: Username to check password.
    :param password: Password to check
    :return: True if password correspond to the user, False otherwise.
    """

    # Check if user exists
    if not check_username(db, username):
        return False

    # Get user id
    user_id = db(db.users.username == username).select(db.users.id)[0].id
    # Get salt and corresponding hash
    salt = db(db.users_auth.user_id == user_id).select(db.users_auth.salt)[0].salt
    hash = hashlib.sha256(salt + password).hexdigest()

    # Verify generated hash
    if db(db.users_auth.user_id == user_id and db.users_auth.password == hash).select():
        return True
    return False