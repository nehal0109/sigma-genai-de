END = "__end__"


class StateGraph:
    def __init__(self, state_type):
        self.nodes = {}
        self.edges = {}
        self.conditional = {}
        self.entry = None

    def add_node(self, name, fn):
        self.nodes[name] = fn

    def set_entry_point(self, name):
        self.entry = name

    def add_edge(self, src, dest):
        self.edges[src] = dest

    def add_conditional_edges(self, src, router, mapping):
        self.conditional[src] = (router, mapping)

    def compile(self):
        graph = self

        class App:
            def invoke(self, state):
                current = graph.entry
                data = dict(state)
                while current and current != END:
                    update = graph.nodes[current](data)
                    data.update(update or {})
                    if current in graph.conditional:
                        router, mapping = graph.conditional[current]
                        current = mapping[router(data)]
                    else:
                        current = graph.edges.get(current, END)
                return data

        return App()
