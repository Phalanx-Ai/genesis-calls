import csv
import logging
import datetime
import math
import sys

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

import PureCloudPlatformClientV2 as pc2

KEY_CLIENT_ID = 'client_id'
KEY_PASSWORD = '#password'
KEY_CLOUD_URL = 'cloud_url'
KEY_DAYS = 'last_days_interval'

REQUIRED_PARAMETERS = [KEY_CLIENT_ID, KEY_PASSWORD, KEY_CLOUD_URL]
REQUIRED_IMAGE_PARS = []


class Component(ComponentBase):
    """
        Extends base class for general Python components. Initializes the CommonInterface
        and performs configuration validation.

        For easier debugging the data folder is picked up by default from `../data` path,
        relative to working directory.

        If `debug` parameter is present in the `config.json`, the default logger is set to verbose DEBUG mode.
    """

    def __init__(self):
        super().__init__()

    def run(self):
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.validate_image_parameters(REQUIRED_IMAGE_PARS)
        params = self.configuration.parameters

        if params.get(KEY_DAYS):
            DAYS_COUNT = int(params.get(KEY_DAYS))
        else:
            DAYS_COUNT = 1

        # get data from previous calendar day only
        start_date = datetime.datetime.combine(
                datetime.datetime.utcnow(),
                datetime.time(00, 00, 00)
            ) - datetime.timedelta(days=DAYS_COUNT)
        end_date = start_date + datetime.timedelta(days=DAYS_COUNT)
        filter = {
            "interval": "%s/%s" % (
                start_date.isoformat(timespec="seconds"),
                end_date.isoformat(timespec="seconds")
            )
        }

        # obtain data
        api_client = pc2.api_client.ApiClient(
            host=params.get(KEY_CLOUD_URL)
        ).get_client_credentials_token(params.get(KEY_CLIENT_ID), params.get(KEY_PASSWORD))

        conversation_api = pc2.ConversationsApi(api_client=api_client)
        users_api = pc2.UsersApi(api_client=api_client)
        routing_api = pc2.RoutingApi(api_client=api_client)

        output = {
            "conversations": []
        }

        body = pc2.ConversationQuery()
        body.interval = filter['interval']
        body.paging = pc2.PagingSpec()
        body.paging.page_size = 100
        body.paging.page_number = 1

        # @note: do this twice because this is the way how to get paging info
        responses_paging = conversation_api.post_analytics_conversations_details_query(body)

        if responses_paging.conversations is not None:
            page_max = math.ceil(responses_paging.total_hits / body.paging.page_size)

            for page_number in range(page_max):
                body.paging.page_number = page_number + 1
                responses = conversation_api.post_analytics_conversations_details_query(body)

                for conversation in responses.conversations:
                    c = {}

                    c['wrap_up_code'] = []
                    c['agents'] = []

                    c['conversation_id'] = conversation.conversation_id

                    if conversation.conversation_start is not None:
                        c['conversation_start'] = conversation.conversation_start.isoformat(timespec="seconds")
                    else:
                        logging.info("Conversation start is None for ID %s" % (str(conversation.conversation_id)))
                        c['conversation_start'] = None

                    if conversation.conversation_end is not None:
                        c['conversation_end'] = conversation.conversation_end.isoformat(timespec="seconds")
                    else:
                        logging.info("Conversation end is None for ID %s" % (str(conversation.conversation_id)))
                        c['conversation_end'] = None

                    # Get wrap_up_code and decode it to text value
                    for p in conversation.participants:
                        for session in p.sessions:
                            for segment in session.segments:
                                if segment.wrap_up_code is not None:
                                    code_id = segment.wrap_up_code

                                    try:
                                        x = routing_api.get_routing_wrapupcode(code_id)
                                        c['wrap_up_code'].append(x.name)
                                    except Exception:
                                        c['wrap_up_code'].append(code_id)

                        # Get agents and their emails
                        if p.purpose == "agent" and p.user_id is not None:
                            c['agents'].append(users_api.get_user(p.user_id).username)

                    output['conversations'].append(c)

        # Create output table - conversations
        conversation_table = self.create_out_table_definition(
             'conversations.csv', incremental=True, primary_key=['conversation_id'])
        with open(conversation_table.full_path, mode='wt', encoding='utf-8', newline='') as out_file:
            writer = csv.DictWriter(
                out_file,
                fieldnames=['conversation_id', 'conversation_start', 'conversation_end']
            )
            writer.writeheader()
            for line in output['conversations']:
                writer.writerow(
                    {key: value for key, value in line.items() if key not in ['agents', 'wrap_up_code']}
                )
        self.write_manifest(conversation_table)

        # Create output table - agents
        agents_table = self.create_out_table_definition(
            'agents.csv', incremental=True, primary_key=['conversation_id', 'agent_email'])
        with open(agents_table.full_path, mode='wt', encoding='utf-8', newline='') as out_file:
            writer = csv.DictWriter(
                out_file,
                fieldnames=['conversation_id', 'agent_email']
            )
            writer.writeheader()
            for line in output['conversations']:
                for agent in line['agents']:
                    writer.writerow({
                        'conversation_id': line['conversation_id'],
                        'agent_email': agent
                    })
        self.write_manifest(agents_table)

        # Create output table - wrap up code
        wrap_table = self.create_out_table_definition(
            'wrap_up_code.csv',
            incremental=True,
            primary_key=['conversation_id', 'wrap_up_code']
        )
        with open(wrap_table.full_path, mode='wt', encoding='utf-8', newline='') as out_file:
            writer = csv.DictWriter(out_file, fieldnames=['conversation_id', 'wrap_up_code'])
            writer.writeheader()
            for line in output['conversations']:
                for code in line['wrap_up_code']:
                    writer.writerow({
                        'conversation_id': line['conversation_id'],
                        'wrap_up_code': code
                    })
        self.write_manifest(wrap_table)


if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        sys.exit(1)
    except Exception as exc:
        logging.exception(exc)
        sys.exit(2)
