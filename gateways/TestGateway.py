from gateways.Gateway import Gateway

class TestGateway(Gateway):
    def __init__(self, server):
        pass

    def send_sms(self, phone_number, content):
        print(f'Sending "{content}" to {phone_number}"')

    def returns(self):
        return {
            "send_sms": {
                "returns": "bool"
            }
        }
