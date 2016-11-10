# -*- coding: utf-8 -*-

# Enable to get current timestamp
import time
# Enable to get uuid of the web2py user
from gluon.utils import web2py_uuid


#----------------- Session Management ---------------#


# Store session in the clint cookie
session.secure()
cookie_key = cache.ram('cookie_key',lambda: web2py_uuid(),None)
session.connect(request, response, cookie_key=cookie_key, check_client=True, compression_level=None)

# Make expired sessions log out
SESSION_TIMEOUT = 30*60 # 30 x 60 seconds
if session.logged and session.lastrequest and (session.lastrequest < time.time()-SESSION_TIMEOUT) and (request.function != "logout"):
    redirect("/logout?automatic=true")
# Set last request time
session.lastrequest=time.time()
