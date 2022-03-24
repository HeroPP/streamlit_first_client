import streamlit as st
from requests_oauthlib import OAuth2Session
from teamleader import Client
from teamleaderApiV1 import Client as TL_v1_client
from statistics import run_statistics

REDIRECT_URI = "https://localhost:3000/oauth.php"
TOKEN_SITE = "https://app.teamleader.eu/oauth2/access_token"
AUTHORIZE_URI = "https://app.teamleader.eu/oauth2/authorize"
REFRESH_URL = "https://app.teamleader.eu/oauth2/access_token"

st.sidebar.title("Teamleader OAuth2 flow")

client_id = st.secrets["teamleader"]["CLIENT_ID"]
client_secret = st.secrets["teamleader"]["CLIENT_SECRET"]
redirect_uri = st.secrets["teamleader"]["REDIRECT_URL"]


def get_oauth():
    return OAuth2Session(client_id, redirect_uri=REDIRECT_URI)


def request_original_token():
    with st.form(key="my_form"):
        oauth = get_oauth()
        authorization_url, _ = oauth.authorization_url(AUTHORIZE_URI, state=oauth.state)
        st.markdown("check out this [link](%s)" % authorization_url)

        authorization_response = st.text_input("Paste reponse")
        submit_button = st.form_submit_button(label="Submit")
    if submit_button:
        token = oauth.fetch_token(
            TOKEN_SITE,
            authorization_response=authorization_response,
            client_id=client_id,
            client_secret=client_secret,
        )
        safe_token_state(token)
        return


def safe_token_state(local_token):
    # Initialization
    if "auth_token" not in st.session_state:
        st.session_state["auth_token"] = local_token


client = None
if "auth_token" not in st.session_state:
    request_original_token()
else:
    client = OAuth2Session(
        client_id,
        token=st.session_state["auth_token"],
        auto_refresh_url=REFRESH_URL,
        auto_refresh_kwargs={
            "client_id": client_id,
            "client_secret": client_secret,
        },
        token_updater=safe_token_state,
    )




reset_button = st.sidebar.button("reset")
if reset_button:
    del st.session_state["auth_token"]
    st.sidebar.write("Press the refresh button")

st.sidebar.button("refresh")





def go_with_the_flow(local_client):
    tl = Client(client=local_client)
    tl_v1_client =TL_v1_client(api_group= st.secrets['teamleader_v1']['api_group'], api_secret=st.secrets['teamleader_v1']['api_secret'])
    run_statistics(tl, tl_v1_client)


if client:
    st.sidebar.write("Client is fine 🟢")
    go_with_the_flow(client)
else:
    st.sidebar.write("Waiting for client 🔴")
