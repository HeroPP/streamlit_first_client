import streamlit as st
from requests_oauthlib import OAuth2Session
from teamleader import Client

REDIRECT_URI = "https://localhost:3000/oauth.php"
TOKEN_SITE = "https://app.teamleader.eu/oauth2/access_token"
AUTHORIZE_URI = "https://app.teamleader.eu/oauth2/authorize"
REFRESH_URL = "https://app.teamleader.eu/oauth2/access_token"

st.title("Teamleader OAuth2 flow")

"## Configuration"

client_id = st.secrets["teamleader"]["CLIENT_ID"]
client_secret = st.secrets["teamleader"]["CLIENT_SECRET"]
redirect_uri = st.text_input("Redirect URI", "https://localhost:3000/oauth.php")

def get_oauth():
    return OAuth2Session(client_id, redirect_uri=REDIRECT_URI)

def request_original_token():
    global token
    with st.form(key='my_form'):
        oauth = get_oauth()
        authorization_url, _ = oauth.authorization_url(AUTHORIZE_URI, state=oauth.state)
        st.markdown("check out this [link](%s)" % authorization_url)

        authorization_response = st.text_input("Paste reponse")
        submit_button = st.form_submit_button(label='Submit')
    if submit_button:
        token = oauth.fetch_token(
            TOKEN_SITE,
            authorization_response=authorization_response,
            client_id=client_id,
            client_secret=client_secret,
        )
        safe_token_state(token)

def safe_token_state(token):
    # Initialization
    if 'auth_token' not in st.session_state:
        st.session_state['auth_token'] = token

client = None
if 'auth_token' not in st.session_state:
    request_original_token()
else:
    client = OAuth2Session(
        client_id,
        token=st.session_state['auth_token'],
        auto_refresh_url=REFRESH_URL,
        auto_refresh_kwargs={
            "client_id": client_id,
            "client_secret": client_secret,
        },
        token_updater=safe_token_state,
    )


def go_with_the_flow(client):
    tl = Client(client= client)
    st.write(list(tl.tags.list()))


if client:
    "we go with the flow"
    go_with_the_flow(client)
else:
    "Waiting for client ."

