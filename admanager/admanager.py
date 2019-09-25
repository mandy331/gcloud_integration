import os
import sys
from os import environ as env

from dotenv import load_dotenv
from googleads import ad_manager, oauth2

load_dotenv()


class AdManager():

    def __init__(self):
        self.googleads_yaml = env.get("GOOGLEADS_YAML", "googleads.yaml")
        pass

    def cert(self):

        return ad_manager.AdManagerClient.LoadFromStorage(self.googleads_yaml)

    def run(self):

        self.print_all_orders(self.cert())

    def print_all_orders(self, ad_manager_client):

        # Initialize appropriate service.
        order_service = ad_manager_client.GetService('OrderService', version='v201908')

        # Create a statement to select orders.
        statement = ad_manager.StatementBuilder(version='v201908')

        # Retrieve a small amount of orders at a time, paging
        # through until all orders have been retrieved.
        while True:
            response = order_service.getOrdersByStatement(statement.ToStatement())
            if 'results' in response and len(response['results']):
                for order in response['results']:
                    # Print out some information for each order.
                    print('Order with ID "%d" and name "%s" was found.\n' % (order['id'],
                                                                            order['name']))
                statement.offset += statement.limit
            else:
                break

        print('\nNumber of results found: %s' % response['totalResultSetSize'])
