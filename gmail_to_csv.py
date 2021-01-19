import base64
import csv
import email
#import html
import os.path
import pickle
from apiclient import errors

from tqdm import tqdm

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request


# If modifying these scopes, delete TOKEN_FILE
SCOPES = [
    'https://www.googleapis.com/auth/gmail.modify',
]
SECRETS_FILE = 'credentials.json'
TOKEN_FILE = 'token.pickle'


def build_service():
    """Instantiates a valid gmail service object

    Create OAuth credentials file from the google console:

        https://console.developers.google.com/apis/credentials

    Store the credentials json SECRETS_FILE in the current path
    """
    creds = None

    # TOKEN_FILE stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow first completes.
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, 'rb') as token:
            creds = pickle.load(token)

    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(SECRETS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)

        # Save the credentials for the next run
        with open(TOKEN_FILE, 'wb') as token:
            pickle.dump(creds, token)

    service = build('gmail', 'v1', credentials=creds)

    return service


def list_messages(service, query=None):
    """Gets a list of messages

    Parameters
    ----------
    service:  authorized Gmail API service instance
    query:  string used to filter messages returned
        Eg.- 'label:UNREAD' for unread Messages only
        Search operators:  https://support.google.com/mail/answer/7190

    Returns
    -------
    messages : list
        List of messages that match the criteria of the query.
        Note that the returned list contains Message IDs--you must use
        get with the appropriate id to get the details of a Message

    """
    try:
        response = service.users().messages().list(
            userId='me',
            q=query,
        ).execute()
        messages = response['messages']

        while 'nextPageToken' in response:
            page_token = response['nextPageToken']
            response = service.users().messages().list(
                userId='me',
                q=query,
                pageToken=page_token,
            ).execute()
            messages.extend(response['messages'])

        return messages
    except errors.HttpError as error:
        print(f'An error occurred: {error}')

        if error.resp.status == 401:
            # Credentials have been revoked.
            # TODO: Redirect the user to the authorization URL.
            raise NotImplementedError()


def read_message(service, msg_id):
    """Read a message and output a dictionary summary

    Parameters
    ----------
    service:  authorized Gmail API service instance
    msg_id:  string uniquely identifying message

    Returns
    -------
    summary :  dict
        'subject', 'date', 'message'

    """
    summary = {}

    try:
        message = service.users().messages().get(
            userId='me',
            id=msg_id,
            format='full'
        ).execute()
        headers = message['payload']['headers']

        for header in headers:
            if header['name'] == 'Subject':
                summary['subject'] = header['value']
            elif header['name'] == 'Date':
                summary['date'] = header['value']

        data = message['payload']['parts'][0]['body']['data']
        data = base64.urlsafe_b64decode(data).decode('utf-8')
        data = email.message_from_string(data)
        #data = html.unescape(data)
        summary['message'] = data
    except Exception as e:
        print(e)
        summary = {}
    finally:
        return summary


def write_messages(service, messages, csvfile):
    """Write a list of messages to CSV file

    Parameters
    ----------
    service:  authorized Gmail API service instance.
    messages:  list
        Message IDs -- use `get` with the appropriate id
        to get the details of a Message.
    csvfile:  string indicating path/name to CSV file.

    """
    with open(csvfile, 'w', encoding='utf-8') as f:
        fieldnames = ['subject', 'date', 'message']
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=',')
        writer.writeheader()

        for m in tqdm(messages):
            msg_id = m['id']  # get id of individual message
            email_dict = read_message(service, msg_id)

            if email_dict:
                writer.writerow(email_dict)


if __name__ == '__main__':

    service = build_service()

    # garagiste wine
    query = '''
    from:nicki@garagistewine.com
    -subject:"re:"
    '''
    messages = list_messages(service, query=query)
    write_messages(service, messages, 'garagiste_wine.csv')

    # the source imports
    query = '''
    from:ted@thesourceimports.com
    '''
    messages = list_messages(service, query=query)
    write_messages(service, messages, 'the_source_imports.csv')
