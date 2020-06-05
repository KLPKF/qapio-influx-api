class FluxQueryBuilder:
    def __init__(self):
        self.build_bucket = ""
        self.build_time = ""
        self.flatten_param = ""
        self.build_filters = ""
        self.build_flatten = ""
        self.build_flux_query = ""

    def bucket(self, bucket):
        self.build_bucket = f'from(bucket: "{bucket}")'
        return self

    def range(self, start, stop):
        self.build_time = f'|> range(start:{start}, stop:{stop})'
        return self

    @staticmethod
    def build_equals(key, value, equality="==", prefix="r", ):
        if type(value) == str and equality == "=~":
            return f'{prefix}.{key} {equality} /{value}/'
        if type(value) == str:
            return f'{prefix}.{key} == "{value}"'
        if type(value) == int:
            return f'{prefix}.{key} {equality} {value}'

    def filters(self, filters, equality="=="):
        filters_queries = []

        for filter in filters:
            or_query = ' or '.join(
                [self.build_equals(filter[0], value, equality) for value in
                 filter[1]])
            filters_queries.append(f'|> filter(fn: (r) => {or_query})')
            self.build_filters = "\n".join(filters_queries)
        return self

    def flatten(self, flat=True):
        if flat:
            self.build_flatten = f'|> group()'
            return self
        else:
            pass

    def do(self):
        self.build_flux_query = str(self.build_bucket) + str(
            self.build_time) + str(self.build_filters) + str(self.build_flatten)
        return self.build_flux_query
