import json

class JsonRoutine:
    def __init__(self, ctx, instructions):
        self.instructions = instructions
        self.ctx = ctx


    def make_kwargs(self, inputs, results):
        kwargs = {}
        for i in inputs:
            print(f"i : {i}")
            input_type = i.get("type")
            value = i.get("value")
            as_json = i.get("input_as_json")
            target = i.get("for")

            print(f"input_type: {input_type}, value: {value}, target: {target}, as_json: {as_json}")

            if input_type == "result":
                value = results.get(value)

            if not as_json:
                try:
                    value = json.loads(value)
                except Exception as e:
                    pass

                print(f"BBBBB {type(value)}")
                if isinstance(value, dict):
                    if value.get(target, None) is not None:
                        kwargs[target] = value[target]
                    else:
                        kwargs[target] = value
                else:
                    kwargs[target] = value
                    print(f"AAAAAAAA {kwargs}")
            else:
                key = i.get("as")
                kwargs[key] = value
        return kwargs

    @staticmethod
    def get_conditions(conditions, results) -> list:
        results_list = []
        # [results.get(cond, {}).get("condition", False) for cond in]

        # cond is the str of the result id
        for cond in conditions:
            if cond.startswith("!"):
                results_list.append(not results.get(cond, {}).get("condition", False))
            else:
                results_list.append(results.get(cond, {}).get("condition", False))
        return results_list



    def run(self, payload):
        results = {"1": payload.get_json()}

        try:
            for instruction in self.instructions:
                conditions: list = self.get_conditions(instruction.get("conditions", []), results)
                # ^^ get the Boolean result of all the conditions
                if False in conditions:
                    continue
                t = instruction.get("type")
                if t == "service":
                    cls = getattr(self.ctx.services, instruction.get("service"), None)()
                elif t == "gateway":
                    cls = getattr(self.ctx.gateways, f'{instruction.get("gateway")}', None)
                else:
                    cls = None

                if cls is None:
                    continue

                func = instruction.get("function")
                output_id = instruction.get("output")
                inputs = instruction.get("inputs", [])
                kwargs = self.make_kwargs(inputs, results)
                print()
                print(f"Args for function {func} : {kwargs}")

                service = cls
                function = getattr(service, f'{func}_json')
                result = function(**kwargs)
                results[output_id] = result

            return results

        except Exception as e:
            print(f"Could not finish execution of routine: {e}")
            return None

    def __str__(self):
        return f"{self.instructions}"