#!/usr/bin/env python
import os
import time
import yaml
import psycopg2
from slackclient import SlackClient

# starterbot's ID as an environment variable
BOT_ID = "U4H5KC04E"

# constants
AT_BOT = "<@" + BOT_ID + ">"
EXAMPLE_COMMAND = "do"

# instantiate Slack & Twilio clients
slack_client = SlackClient(os.environ.get('SLACK_BOT_TOKEN'))


def handle_command(command, channel):
    """
        Receives commands directed at the bot and determines if they
        are valid commands. If so, then acts on the commands. If not,
        returns back what it needs for clarification.
    """
    conn = psycopg2.connect(
                            database=credentials['postgres_bot'].get('dbase'),
                            user=credentials['postgres_bot'].get('user'),
                            host=credentials['postgres_bot'].get('host'),
                            port=credentials["postgres_bot"].get("port"),
                            password=credentials['postgres_bot'].get('pass'))
    cur = conn.cursor()

    def interact(call, conn):
        url_head = "https://www.youtube.com/watch?v="

        if call is 'hello':
            user = "Hi, there! What can I call you?"
            return user

        elif call is 'Jeremy':
            name = "So, Jeremy, which trailers do you want to watch: in theaters or coming soon?"
            return name

        elif call is 'in theaters':
            cur.execute("""SELECT url FROM in_theaters WHERE title = 'Logan'""")
            for i in cur.fetchall():
                url_suffix = i[0]
            return url_head + url_suffix

        elif call is 'coming_soon':
            cur.execute("""SELECT url FROM coming_soon WHERE title = 'Beauty and the Beast'""")
            for i in cur.fetchall():
                url_suffix = i[0]
            return url_head + url_suffix

        elif call is 'bye':
            seeya = "See you soon!"
            return seeya

    response = interact(command, conn)
    slack_client.api_call("chat.postMessage", channel=channel,
                          text=response, as_user=True)


def parse_slack_output(slack_rtm_output):
    """
        The Slack Real Time Messaging API is an events firehose.
        this parsing function returns None unless a message is
        directed at the Bot, based on its ID.
    """
    output_list = slack_rtm_output
    if output_list and len(output_list) > 0:
        for output in output_list:
            if output and 'text' in output and AT_BOT in output['text']:
                # return text after the @ mention, whitespace removed
                return output['text'].split(AT_BOT)[1].strip().lower(), \
                       output['channel']
    return None, None


if __name__ == "__main__":
    credentials = yaml.load(open(os.path.expanduser('~/admin/bot-creds.yaml')))
    READ_WEBSOCKET_DELAY = 1  # 1 second delay between reading from firehose
    if slack_client.rtm_connect():
        print("StarterBot connected and running!")
        while True:
            command, channel = parse_slack_output(slack_client.rtm_read())
            if command and channel:
                handle_command(command, channel)
            time.sleep(READ_WEBSOCKET_DELAY)
    else:
        print("Connection failed. Invalid Slack token or bot ID?")
