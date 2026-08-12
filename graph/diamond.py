import concurrent.futures
from typing import Any, Callable, Optional

from .node import Graph, Node


class Diamond:
    def __init__(self, graph: Graph, executor_fn: Callable[[Node], Any]):
        self.graph = graph
        self.executor_fn = executor_fn

    def run_parallel_workers(self, worker_nodes: list[Node]) -> dict[str, Any]:
        results = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(worker_nodes)) as executor:
            futures = {executor.submit(self.executor_fn, node): node.name for node in worker_nodes}
            for future in concurrent.futures.as_completed(futures):
                name = futures[future]
                try:
                    results[name] = future.result()
                except Exception as e:
                    results[name] = f"Error: {e}"
        return results

    def converge(self, worker_results: dict[str, Any], synthesis_node: Node) -> Any:
        synthesis_node.input_data = worker_results
        return self.executor_fn(synthesis_node)

    def execute(
        self,
        worker_names: list[str],
        synthesis_name: str,
        start_hook: Optional[Callable[[], Any]] = None,
    ) -> dict[str, Any]:
        if start_hook:
            start_hook()

        workers = []
        for name in worker_names:
            if name in self.graph.nodes:
                workers.append(self.graph.nodes[name])

        worker_results = self.run_parallel_workers(workers)

        for name, result in worker_results.items():
            if name in self.graph.nodes:
                self.graph.nodes[name].output = result
                self.graph.nodes[name].completed = True

        synthesis_node = self.graph.nodes.get(synthesis_name)
        if not synthesis_node:
            return {"error": f"Synthesis node '{synthesis_name}' not found", "workers": worker_results}

        synthesis_result = self.converge(worker_results, synthesis_node)
        synthesis_node.output = synthesis_result
        synthesis_node.completed = True

        return {
            "workers": worker_results,
            "synthesis": synthesis_result,
        }
