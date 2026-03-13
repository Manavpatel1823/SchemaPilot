from collections import defaultdict, deque


def build_graph(edges):
    deps = defaultdict(list)
    refs = defaultdict(list)
    tables = set()

    for e in edges:
        child = e["child_table"]
        parent = e["parent_table"]
        tables.add(child)
        tables.add(parent)

        # child -> parent
        deps[child].append(
            (parent, e["child_column"], e["parent_column"], e["constraint_name"])
        )

        # parent -> child
        refs[parent].append(
            (child, e["child_column"], e["parent_column"], e["constraint_name"])
        )

    return tables, deps, refs


def _neighbors(table, deps, refs):

    out = []
    for (parent, child_col, parent_col, cname) in deps.get(table, []):
        out.append((parent, table, child_col, parent, parent_col, cname))

    for (child, child_col, parent_col, cname) in refs.get(table, []):
        out.append((child, child, child_col, table, parent_col, cname))

    return out


def find_join_path(start, target, deps, refs, max_depth=6):
    """
    BFS (shortest path) from start table to target table using FK graph.

    Returns:
      path_tables: list[str] like ["enrollments", "sections", "courses"]
      joins: list[dict] where each dict contains an ON condition:
        {
          "left_table": "...",
          "left_col": "...",
          "right_table": "...",
          "right_col": "...",
          "constraint": "..."
        }

    If no path found within max_depth, returns (None, None).
    """
    if start == target:
        return [start], []

    visited = {start}
    q = deque([(start, 0)])
    parent = {start: None}
    parent_edge = {}

    while q:
        current, depth = q.popleft()
        if depth >= max_depth:
            continue

        for (nxt, lt, lc, rt, rc, cname) in _neighbors(current, deps, refs):
            if nxt in visited:
                continue

            visited.add(nxt)
            parent[nxt] = current
            parent_edge[nxt] = {
                "left_table": lt,
                "left_col": lc,
                "right_table": rt,
                "right_col": rc,
                "constraint": cname,
            }

            # Stop as soon as we reach the target: BFS guarantees shortest path
            if nxt == target:
                # Reconstruct path and join list by walking backwards from target
                path = [target]
                joins = []

                node = target
                while parent[node] is not None:
                    joins.append(parent_edge[node])
                    node = parent[node]
                    path.append(node)

                path.reverse()
                joins.reverse()
                return path, joins

            q.append((nxt, depth + 1))

    return None, None


def joins_to_sql(start_table, joins):
    """
    Convert the join steps returned by find_join_path() into a SQL JOIN skeleton.

    Example output:
      FROM "enrollments"
      JOIN "sections" ON "enrollments"."section_id" = "sections"."id"
      JOIN "courses"  ON "sections"."course_id"     = "courses"."id"
    """
    clauses = [f'FROM "{start_table}"']
    used = {start_table}

    for j in joins:
        lt, lc = j["left_table"], j["left_col"]
        rt, rc = j["right_table"], j["right_col"]
        if lt in used and rt not in used:
            new_table = rt
            on_left_table, on_left_col = lt, lc
            on_right_table, on_right_col = rt, rc
        elif rt in used and lt not in used:
            new_table = lt
            on_left_table, on_left_col = lt, lc
            on_right_table, on_right_col = rt, rc
        else:
            new_table = rt
            on_left_table, on_left_col = lt, lc
            on_right_table, on_right_col = rt, rc

        clauses.append(
            f'JOIN "{new_table}" ON "{on_left_table}"."{on_left_col}" = "{on_right_table}"."{on_right_col}"'
        )
        used.add(new_table)

    return "\n".join(clauses)