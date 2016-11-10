# -*- coding: utf-8 -*-

# -------------------------------------------------------------------------
# if SSL/HTTPS is properly configured and you want all HTTP requests to
# be redirected to HTTPS, uncomment the line below:
# -------------------------------------------------------------------------
# request.requires_https()

# -------------------------------------------------------------------------
# app configuration made easy. Look inside private/appconfig.ini
# -------------------------------------------------------------------------
from gluon.contrib.appconfig import AppConfig

# -------------------------------------------------------------------------
# once in production, remove reload=True to gain full speed
# -------------------------------------------------------------------------
myconf = AppConfig(reload=True)

# ---------------------------------------------------------------------
# if NOT running on Google App Engine use SQLite or other DB
# ---------------------------------------------------------------------
db = DAL(myconf.get('db.uri'),
         pool_size=myconf.get('db.pool_size'),
         migrate_enabled=myconf.get('db.migrate'),
         check_reserved=['all'],
         auto_import=True)

# -------------------------------------------------------------------------
# by default give a view/generic.extension to all actions from localhost
# none otherwise. a pattern can be 'controller/function.extension'
# -------------------------------------------------------------------------
response.generic_patterns = ['*'] if request.is_local else []
# -------------------------------------------------------------------------
# choose a style for forms
# -------------------------------------------------------------------------
response.formstyle = myconf.get('forms.formstyle')  # or 'bootstrap3_stacked' or 'bootstrap2' or other
response.form_label_separator = myconf.get('forms.separator') or ''

# -------------------------------------------------------------------------
# (optional) optimize handling of static files
# -------------------------------------------------------------------------
# response.optimize_css = 'concat,minify,inline'
# response.optimize_js = 'concat,minify,inline'

# -------------------------------------------------------------------------
# (optional) static assets folder versioning
# -------------------------------------------------------------------------
# response.static_version = '0.0.0'

# -------------------------------------------------------------------------
# Define your tables below (or better in another model file) for example
#
# >>> db.define_table('mytable', Field('myfield', 'string'))
#
# Fields can be 'string','text','password','integer','double','boolean'
#       'date','time','datetime','blob','upload', 'reference TABLENAME'
# There is an implicit 'id integer autoincrement' field
# Consult manual for more options, validators, etc.
#
# More API examples for controllers:
#
# >>> db.mytable.insert(myfield='value')
# >>> rows = db(db.mytable.myfield == 'value').select(db.mytable.ALL)
# >>> for row in rows: print row.id, row.myfield
# -------------------------------------------------------------------------

db.define_table('users',
                Field('username', 'string', required=True, notnull=True, unique=True),
                Field('name', 'string', required=True, notnull=True),
                Field('organization', 'string', required=True, notnull=True),
                Field('email', 'string', required=True, notnull=True),
                Field('role', 'string', required=True, notnull=True))

db.define_table('users_auth',
                Field('user_id', 'integer', required=True, notnull=True, unique=True),
                Field('salt', 'string', length=20, required=True, notnull=True),
                Field('password', 'string', length=64, required=True, notnull=True))

db.define_table('users_logins',
                Field('user_id', 'integer', required=True, notnull=True, unique=True),
                Field('last_login', 'datetime', required=True, notnull=True))

# -------------------------------------------------------------------------
# after defining tables, uncomment below to enable auditing
# -------------------------------------------------------------------------
# auth.enable_record_versioning(db)
