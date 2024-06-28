import ast
from typing import Iterator


class PydanticImportChecker(ast.NodeVisitor):
    name = "flake8-pydantic-import-checker"
    version = "0.1.0"

    def __init__(self, tree: ast.AST) -> None:
        self.tree = tree

    def run(self) -> Iterator[tuple[int, int, str, type]]:
        for node in ast.walk(self.tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == "pydantic":
                        yield self.error(node, "PYD001")
            elif isinstance(node, ast.ImportFrom):
                if node.module == "pydantic":
                    yield self.error(node, "PYD002")

    def error(self, node: ast.AST, error_code: str) -> tuple[int, int, str, type]:
        if error_code == "PYD001":
            msg = "PYD001 Direct import of 'pydantic' is not allowed. Use 'paradime.tools.pydantic' instead."
        elif error_code == "PYD002":
            msg = "PYD002 Importing from 'pydantic' is not allowed. Use 'paradime.tools.pydantic' instead."
        return (node.lineno, node.col_offset, msg, type(self))
