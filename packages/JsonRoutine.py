class JsonRoutine:
    def __init__(self, ctx, instructions):
        self.instructions = instructions
        self.ctx = ctx

    def make_kwargs(self, inputs, results):
        kwargs = {}
        for i in inputs:
            input_type = i.get("type")
            as_json = i.get("input_as_json")
            value = i.get("value")

            if input_type == "result":
                value = results.get(value)


            if not as_json:
                for key, val in value.items():
                    kwargs[key] = val
            else:
                key = i.get("as")
                kwargs[key] = value
        return kwargs



    def run(self, payload):
        results = {"1": payload.get_json()}

        try:
            for instruction in self.instructions:
                t = instruction.get("type")
                if t == "service":
                    cls = getattr(self.ctx.services, instruction.get("service"), None)
                elif t == "gateway":
                    cls = getattr(self.ctx.gateway, instruction.get("gateway"), None)
                else:
                    cls = None

                if cls is None:
                    continue

                func = instruction.get("function")
                output_id = instruction.get("output")
                inputs = instruction.get("inputs", [])
                kwargs = self.make_kwargs(inputs, results)

                service = cls()
                function = getattr(service, f'{func}_json')
                result = function(**kwargs)
                results[output_id] = result

            print(results)
            return results

        except Exception as e:
            print(f"Could not finish execution of routine: {e}")
            return None

    def __str__(self):
        return f"{self.instructions}"