from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass
class Node:
    name: str
    task: str
    dependencies: list[str] = field(default_factory=list)
    input_data: Optional[Any] = None
    output: Optional[Any] = None
    completed: bool = False
    failed: Optional[str] = None

    @property
    def ready(self) -> bool:
        return not self.completed and not self.failed


@dataclass
class Edge:
    source: str
    target: str
    is_real_dependency: bool = True


class Graph:
    def __init__(self):
        self.nodes: dict[str, Node] = {}
        self.edges: list[Edge] = []

    def add_node(self, node: Node) -> None:
        self.nodes[node.name] = node

    def add_edge(self, source: str, target: str) -> None:
        self.edges.append(Edge(source=source, target=target))
        if target in self.nodes and source not in self.nodes[target].dependencies:
            self.nodes[target].dependencies.append(source)

    def remove_fake_edges(self) -> list[Edge]:
        fake = [e for e in self.edges if not e.is_real_dependency]
        for edge in fake:
            if edge.target in self.nodes and edge.source in self.nodes[edge.target].dependencies:
                self.nodes[edge.target].dependencies.remove(edge.source)
        self.edges = [e for e in self.edges if e.is_real_dependency]
        return fake

    def fake_edge_test(self) -> list[Edge]:
        questionable = []
        for edge in self.edges:
            source_node = self.nodes.get(edge.source)
            target_node = self.nodes.get(edge.target)
            if source_node and target_node:
                if not self._is_genuine_dependency(source_node, target_node):
                    questionable.append(edge)
        return questionable

    def _is_genuine_dependency(self, source: Node, target: Node) -> bool:
        if source.name in target.dependencies:
            return True
        return False

    def get_ready_nodes(self) -> list[Node]:
        ready = []
        for node in self.nodes.values():
            if not node.ready:
                continue
            deps_met = all(
                dep not in self.nodes or self.nodes[dep].completed
                for dep in node.dependencies
            )
            if deps_met:
                ready.append(node)
        return ready

    def get_nodes_without_dependencies(self) -> list[Node]:
        return [n for n in self.nodes.values() if not n.dependencies]

    def get_leaf_nodes(self) -> list[Node]:
        targets = {e.source for e in self.edges}
        return [n for n in self.nodes.values() if n.name not in targets]
