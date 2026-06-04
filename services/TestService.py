from services.Service import Service
import random

class TestService(Service):
    def __init__(self):
        pass

    def get_random_number(self, minimum, maximum):
        minimum = int(minimum)
        maximum = int(maximum)
        return random.randint(minimum, maximum)

    def get_two_random_numbers(self, minimum, maximum):
        return (self.get_random_number(minimum, maximum), self.get_random_number(minimum, maximum))

    def output_message_content(self, content):
        print(content)
        return None

    def returns(self):
        return {
            "get_random_number": {
                "returns" : "int"
            },
            "get_two_random_numbers": {
                "returns": "tuple"
            },
            "output_message_content": {
                "returns": "None"
            },
        }
