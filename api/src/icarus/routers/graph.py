from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query
from neo4j import AsyncSession

from icarus.constants import PEP_ROLES
from icarus.dependencies import get_session
from icarus.models.entity import SourceAttribution
from icarus.models.graph import GraphEdge, GraphNode, GraphResponse
from icarus.services.neo4j_service import execute_query

router = APIRouter(prefix="/api/v1/graph", tags=["graph"])


def _is_pep(properties: dict[str, Any]) -> bool:
    role = str(properties.get("role", "")).lower()
    return any(keyword in role for keyword in PEP_ROLES)


def _extract_label(node: Any, labels: list[str]) -> str:
    props = dict(node)
    entity_type = labels[0].lower() if labels else ""
    if entity_type == "company":
        return str(props.get("razao_social", props.get("name", props.get("nome_fantasia", ""))))
    return str(props.get("name", str(props.get("id", ""))))


@router.get("/{entity_id}", response_model=GraphResponse)
async def get_graph(
    entity_id: str,
    session: Annotated[AsyncSession, Depends(get_session)],
    depth: Annotated[int, Query(ge=1, le=4)] = 2,
    entity_types: Annotated[str | None, Query()] = None,
) -> GraphResponse:
    type_list = [t.strip().lower() for t in entity_types.split(",")] if entity_types else None

    node_records = await execute_query(
        session,
        "graph_expand",
        {
            "entity_id": entity_id,
            "entity_types": type_list,
            "depth": depth,
        },
    )

    if not node_records:
        raise HTTPException(status_code=404, detail="Entity not found")

    nodes: list[GraphNode] = []
    node_ids: list[str] = []
    seen: set[str] = set()

    for record in node_records:
        node_id = record["node_id"]
        if node_id in seen:
            continue
        seen.add(node_id)
        node_ids.append(node_id)

        node = record["node"]
        labels = record["node_labels"]
        props = dict(node)
        source_val = props.pop("source", None)
        sources: list[SourceAttribution] = []
        if isinstance(source_val, str):
            sources = [SourceAttribution(database=source_val)]
        elif isinstance(source_val, list):
            sources = [SourceAttribution(database=s) for s in source_val]

        nodes.append(GraphNode(
            id=node_id,
            label=_extract_label(node, labels),
            type=labels[0].lower() if labels else "unknown",
            properties=props,
            sources=sources,
            is_pep=_is_pep(props),
        ))

    edge_records = await execute_query(
        session,
        "graph_edges",
        {"node_ids": node_ids},
    )

    edges: list[GraphEdge] = []
    seen_edges: set[str] = set()

    for record in edge_records:
        rel_id = record["rel_id"]
        if rel_id in seen_edges:
            continue
        seen_edges.add(rel_id)

        rel_props = dict(record["rel_props"]) if record["rel_props"] else {}
        confidence = float(rel_props.pop("confidence", 1.0))
        rel_source_val = rel_props.pop("source", None)
        rel_sources: list[SourceAttribution] = []
        if isinstance(rel_source_val, str):
            rel_sources = [SourceAttribution(database=rel_source_val)]
        elif isinstance(rel_source_val, list):
            rel_sources = [SourceAttribution(database=s) for s in rel_source_val]

        edges.append(GraphEdge(
            id=rel_id,
            source=record["source_id"],
            target=record["target_id"],
            type=record["rel_type"],
            properties=rel_props,
            confidence=confidence,
            sources=rel_sources,
        ))

    center_id = node_records[0]["center_id"]

    return GraphResponse(
        nodes=nodes,
        edges=edges,
        center_id=center_id,
    )
