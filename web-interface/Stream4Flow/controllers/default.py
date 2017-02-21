# -*- coding: utf-8 -*-

# Enable to get current datetime
from datetime import datetime
# Import Elasticsearch library
import elasticsearch
from elasticsearch_dsl import Search, Q, A
# Import global functions
from global_functions import escape
from global_functions import check_password


#----------------- Authentication -------------------#


def login():
    """
    Verify given username and corresponding password, and logs user if credentials are correct. Set session variables to the logged user.

    :return: Index page with alert message if some error occured, otherwise redirect to the index
    """

    # Default alert
    alert_type = ""
    alert_message = ""
    error = False

    # Check mandatory inputs
    if not (request.post_vars.username and request.post_vars.password and request.post_vars.current_page):
        alert_type = "danger"
        alert_message = "No credentials given!"
        error = True

    # Parse inputs
    username = escape(request.post_vars.username) if not error else ""
    password = escape(request.post_vars.password) if not error else ""
    current_page = escape(request.post_vars.current_page) if not error else ""

    # Check if username exists and corresponds to the password
    if not error and not check_password(db, username, password):
        alert_type = "danger"
        alert_message = "Username or password is incorrect!"
        error = True

    # Set session variables
    if not error:
        # Get user info
        user_info = db(db.users.username == username).select(db.users.ALL)[0]
        # Set session
        session.logged = True
        session.user_id = user_info.id
        session.username = user_info.username
        session.name = user_info.name
        session.organization = user_info.organization
        session.role = user_info.role
        session.email = user_info.email

    # Set login time.
    if not error:
        db(db.users_logins.user_id == session.user_id).update(last_login=datetime.now())

    # Redirect to the index if everything was ok
    if not error:
        if ("login" in current_page) or ("logout" in current_page):
            redirect("/index")
        else:
            redirect(current_page)

    # Use standard error view
    response.view = 'error.html'
    return dict(
        alert_type=alert_type,
        alert_message=alert_message,
    )


def logout():
    """
    Log outs current user (erase session variables).

    :return: Redirects to the index page
    """

    # Clear the session
    session.clear()

    # Check if logout was automatic
    if request.get_vars.automatic:
        # Use standard error view
        response.view = 'error.html'
        return dict(
                  alert_type="info",
                  alert_message="You have been automatically logged out!",
        )

    # Redirect to the index page if logout was manually requested
    redirect("/index")


#----------------- Index ----------------------------#


def index():
    """
    Show the main page of Stream4Flow

    :return: Empty dictionary
    """

    # Do not save the session
    session.forget(response)
    return dict()


#----------------- Elasticsearch --------------------#


def get_summary_statistics():
    """
    Obtains statistics about current sum of flows, packets, bytes.

    :return: JSON with status "ok" or "error" and requested data.
    """

    try:
        # Elastic query
        client = elasticsearch.Elasticsearch([{'host': myconf.get('consumer.hostname'), 'port': myconf.get('consumer.port')}])
        elastic_bool = []
        elastic_bool.append({'range': {'@timestamp': {'gte': "now-5m", 'lte': "now"}}})
        elastic_bool.append({'term': {'@type': 'protocols_statistics'}})

        qx = Q({'bool': {'must': elastic_bool}})
        s = Search(using=client, index='_all').query(qx)
        s.aggs.bucket('sum_of_flows', 'sum', field='flows')
        s.aggs.bucket('sum_of_packets', 'sum', field='packets')
        s.aggs.bucket('sum_of_bytes', 'sum', field='bytes')
        s.sort('@timestamp')
        result = s.execute()

        # Result Parsing into CSV in format: timestamp, tcp protocol value, udp protocol value
        data = "Timestamp, Flows, Packets, Bytes;"
        timestamp = "Last 5 Minutes"
        data += timestamp + ', ' +\
                str(int(result.aggregations.sum_of_flows['value'])) + ', ' +\
                str(int(result.aggregations.sum_of_packets['value'])) + ', ' +\
                str(int(result.aggregations.sum_of_bytes['value']))

        json_response = '{"status": "Ok", "data": "' + data + '"}'
        return json_response

    except Exception as e:
        json_response = '{"status": "Error", "data": "Elasticsearch query exception: ' + escape(str(e)) + '"}'
        return json_response
