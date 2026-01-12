"""
Gradio web interface for GeBIZ Tender Intelligence Knowledge Graph.

Provides interactive exploration of the tender knowledge graph with:
- Tabular results for agency, supplier, category exploration
- Interactive graph visualizations for similar tenders and requirements overlap
- Ad-hoc graph explorer for entity neighborhoods
"""

import sys
from pathlib import Path
from typing import Any

# Optional dependencies
try:
    import gradio as gr
except ImportError:
    gr = None

try:
    from pyvis.network import Network
    import networkx as nx
except ImportError:
    Network = None
    nx = None

try:
    from neo4j import GraphDatabase
except ImportError:
    GraphDatabase = None  # type: ignore[misc,assignment]

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.config import Config
from queries.agency import get_tenders_by_agency, get_top_agencies
from queries.supplier import get_tenders_by_supplier, get_top_suppliers
from queries.category import explore_categories
from queries.similar import find_similar_tenders
from queries.requirements import find_requirements_overlap


# Color scheme for entity types
ENTITY_COLORS = {
    "Tender": "#FF6B6B",      # Red
    "Agency": "#4ECDC4",      # Teal
    "Supplier": "#95E1D3",    # Light teal
    "Category": "#FFD93D",    # Yellow
    "Requirement": "#A8E6CF", # Light green
    "Keyword": "#C7CEEA",     # Light blue
    "Date": "#FFEAA7",        # Light yellow
    "Chunk": "#DFE6E9",       # Light gray
}


def get_driver() -> Any | None:
    """Get a Neo4j driver instance if available."""
    if GraphDatabase is None:
        return None
    try:
        config = Config.load(require_neo4j=True)
        if config.neo4j is None:
            return None
        driver = GraphDatabase.driver(
            config.neo4j.uri,
            auth=(config.neo4j.username, config.neo4j.password)
        )
        return driver
    except Exception:
        return None


def check_connection() -> tuple[bool, str]:
    """Check if Neo4j connection is available."""
    try:
        driver = get_driver()
        if driver is None:
            return False, "Neo4j configuration not found. Please set NEO4J_URI, NEO4J_USERNAME, and NEO4J_PASSWORD environment variables."
        driver.verify_connectivity()
        driver.close()
        return True, "✓ Connected to Neo4j"
    except Exception as e:
        return False, f"✗ Connection failed: {str(e)}"


def query_agencies_tab(agency_name: str | None, show_top: bool, top_limit: int) -> str:
    """Handle agency exploration queries."""
    try:
        driver = get_driver()
        if driver is None:
            return "Error: Neo4j connection not available"

        if show_top:
            results = get_top_agencies(driver, limit=top_limit)
            if not results:
                driver.close()
                return "No agencies found in database."

            output = f"Top {len(results)} Agencies by Tender Count\n" + "=" * 50 + "\n\n"
            for r in results:
                output += f"Agency: {r['agency']}\n"
                output += f"Tender Count: {r['tender_count']}\n"
                output += "-" * 50 + "\n"
            driver.close()
            return output
        elif agency_name:
            results = get_tenders_by_agency(driver, agency_name)
            if not results:
                driver.close()
                return f"No tenders found for agency: {agency_name}"

            output = f"Tenders for Agency: {agency_name}\n" + "=" * 50 + "\n\n"
            for r in results:
                output += f"Tender: {r['tender_name']}\n"
                if r.get('tender_no'):
                    output += f"Tender No: {r['tender_no']}\n"
                if r.get('award_date'):
                    output += f"Award Date: {r['award_date']}\n"
                output += "-" * 50 + "\n"
            driver.close()
            return output
        else:
            driver.close()
            return "Please enter an agency name or check 'Show Top Agencies'"
    except Exception as e:
        return f"Error: {str(e)}"


def query_suppliers_tab(supplier_name: str | None, show_top: bool, top_limit: int) -> str:
    """Handle supplier exploration queries."""
    try:
        driver = get_driver()
        if driver is None:
            return "Error: Neo4j connection not available"

        if show_top:
            results = get_top_suppliers(driver, limit=top_limit)
            if not results:
                driver.close()
                return "No suppliers found in database."

            output = f"Top {len(results)} Suppliers by Total Amount\n" + "=" * 50 + "\n\n"
            for r in results:
                output += f"Supplier: {r['supplier']}\n"
                try:
                    amt = float(r['total_amount'])
                    output += f"Total Amount: ${amt:,.2f}\n"
                except (ValueError, TypeError):
                    output += f"Total Amount: {r['total_amount']}\n"
                output += f"Tender Count: {r['tender_count']}\n"
                output += "-" * 50 + "\n"
            driver.close()
            return output
        elif supplier_name:
            results = get_tenders_by_supplier(driver, supplier_name)
            if not results:
                driver.close()
                return f"No tenders found for supplier: {supplier_name}"

            output = f"Tenders for Supplier: {supplier_name}\n" + "=" * 50 + "\n\n"
            for r in results:
                output += f"Tender: {r['tender_name']}\n"
                if r.get('tender_no'):
                    output += f"Tender No: {r['tender_no']}\n"
                if r.get('award_date'):
                    output += f"Award Date: {r['award_date']}\n"
                if r.get('awarded_amt'):
                    try:
                        amt = float(r['awarded_amt'])
                        output += f"Amount: ${amt:,.2f}\n"
                    except (ValueError, TypeError):
                        output += f"Amount: {r['awarded_amt']}\n"
                output += "-" * 50 + "\n"
            driver.close()
            return output
        else:
            driver.close()
            return "Please enter a supplier name or check 'Show Top Suppliers'"
    except Exception as e:
        return f"Error: {str(e)}"


def query_categories_tab(category_name: str | None, show_all: bool) -> str:
    """Handle category exploration queries."""
    try:
        driver = get_driver()
        if driver is None:
            return "Error: Neo4j connection not available"

        if show_all:
            results = explore_categories(driver)
        elif category_name:
            results = explore_categories(driver, category_group=category_name)
        else:
            driver.close()
            return "Please enter a category group or check 'Show All Categories'"

        if not results:
            driver.close()
            return "No categories found in database."

        output = f"Categories ({len(results)} found)\n" + "=" * 50 + "\n\n"
        for r in results:
            output += f"Category: {r.get('category_name', 'N/A')}\n"
            category_group = r.get('category_group')
            if category_group:
                output += f"Category Group: {category_group}\n"
            output += f"Tender Count: {r['tender_count']}\n"

            keywords = r.get('keywords', [])
            keywords = [k for k in keywords if k is not None]
            if keywords:
                output += f"Keywords: {', '.join(keywords[:10])}"
                if len(keywords) > 10:
                    output += f" ... (+{len(keywords) - 10} more)"
                output += "\n"

            requirements = r.get('requirements', [])
            requirements = [req for req in requirements if req is not None]
            if requirements:
                output += f"Requirements: {', '.join(requirements[:10])}"
                if len(requirements) > 10:
                    output += f" ... (+{len(requirements) - 10} more)"
                output += "\n"

            output += "-" * 50 + "\n"
        driver.close()
        return output
    except Exception as e:
        return f"Error: {str(e)}"


def create_network_graph(nodes: list[dict[str, Any]], edges: list[dict[str, Any]],
                         title: str = "Knowledge Graph") -> str:
    """Create an interactive network graph using pyvis."""
    if Network is None:
        return "<p>Error: pyvis not installed. Please run: pip install pyvis</p>"

    # Use remote CDN resources and save to file for iframe display
    net = Network(height="600px", width="100%", bgcolor="#ffffff", font_color="black",
                  cdn_resources='remote')

    # Configure physics
    net.barnes_hut(
        gravity=-8000,
        central_gravity=0.3,
        spring_length=100,
        spring_strength=0.001,
        damping=0.09,
        overlap=0
    )

    # Add nodes
    for node in nodes:
        node_id = node['id']
        label = node.get('label', node_id)
        node_type = node.get('type', 'Unknown')
        color = ENTITY_COLORS.get(node_type, "#95A5A6")

        # Create title (tooltip) with node info
        title = f"{node_type}: {label}"
        if 'properties' in node:
            title += "\n" + "\n".join(f"{k}: {v}" for k, v in node['properties'].items())

        net.add_node(node_id, label=label, color=color, title=title, size=20)

    # Add edges
    for edge in edges:
        source = edge['source']
        target = edge['target']
        edge_type = edge.get('type', '')
        net.add_edge(source, target, title=edge_type, label=edge_type)

    # Generate HTML
    html_output: str = net.generate_html()
    return html_output


def query_similar_tenders_graph(tender_name: str, limit: int, include_category: bool) -> str:
    """Generate graph visualization for similar tenders."""
    try:
        if not tender_name:
            return "<p>Please enter a tender name</p>"

        driver = get_driver()
        if driver is None:
            return "<p>Error: Neo4j connection not available</p>"

        results = find_similar_tenders(driver, tender_name, limit=limit, include_category=include_category)
        driver.close()
        if not results:
            return f"<p>No similar tenders found for: {tender_name}</p>"

        # Build graph
        nodes = []
        edges = []

        # Add reference tender node
        ref_id = f"tender_{tender_name}"
        ref_label = tender_name[:30] + "..." if tender_name and len(tender_name) > 30 else (tender_name or "")
        nodes.append({
            'id': ref_id,
            'label': ref_label,
            'type': 'Tender',
            'properties': {'name': tender_name, 'role': 'Reference'}
        })

        # Add similar tender nodes and connections
        for i, result in enumerate(results):
            similar_name = result.get('tender_name', '')
            similar_id = f"tender_{similar_name}"
            similar_label = similar_name[:30] + "..." if similar_name and len(similar_name) > 30 else similar_name
            nodes.append({
                'id': similar_id,
                'label': similar_label,
                'type': 'Tender',
                'properties': {
                    'name': similar_name,
                    'score': result['similarity_score'],
                    'overlap': result['overlap_count']
                }
            })

            # Connect reference to similar tender
            edges.append({
                'source': ref_id,
                'target': similar_id,
                'type': f"Similar (score: {result['similarity_score']})"
            })

            # Add shared keyword nodes
            for keyword in result.get('shared_keywords', [])[:3]:  # Limit to top 3
                kw_id = f"kw_{keyword}"
                if not any(n['id'] == kw_id for n in nodes):
                    nodes.append({
                        'id': kw_id,
                        'label': keyword,
                        'type': 'Keyword',
                        'properties': {'name': keyword}
                    })
                edges.append({'source': ref_id, 'target': kw_id, 'type': 'HAS_KEYWORD'})
                edges.append({'source': similar_id, 'target': kw_id, 'type': 'HAS_KEYWORD'})

            # Add shared requirement nodes
            for req in result.get('shared_requirements', [])[:3]:  # Limit to top 3
                req_id = f"req_{req}"
                if not any(n['id'] == req_id for n in nodes):
                    nodes.append({
                        'id': req_id,
                        'label': req,
                        'type': 'Requirement',
                        'properties': {'name': req}
                    })
                edges.append({'source': ref_id, 'target': req_id, 'type': 'HAS_REQUIREMENT'})
                edges.append({'source': similar_id, 'target': req_id, 'type': 'HAS_REQUIREMENT'})

        return create_network_graph(nodes, edges, f"Similar Tenders: {tender_name}")
    except Exception as e:
        return f"<p>Error: {str(e)}</p>"


def query_requirements_overlap_graph(min_overlap: int, limit: int) -> str:
    """Generate graph visualization for requirements overlap."""
    try:
        driver = get_driver()
        if driver is None:
            return "<p>Error: Neo4j connection not available</p>"

        results = find_requirements_overlap(driver, min_overlap=min_overlap, limit=limit)
        driver.close()
        if not results:
            return f"<p>No tender pairs found with at least {min_overlap} shared requirements</p>"

        # Build graph
        nodes = []
        edges = []
        tender_ids = set()

        for result in results:
            t1_name = result.get('tender1_name', '')
            t2_name = result.get('tender2_name', '')
            t1_id = f"tender_{t1_name}"
            t2_id = f"tender_{t2_name}"

            # Add tender nodes
            if t1_id not in tender_ids:
                t1_label = t1_name[:30] + "..." if t1_name and len(t1_name) > 30 else t1_name
                nodes.append({
                    'id': t1_id,
                    'label': t1_label,
                    'type': 'Tender',
                    'properties': {'name': t1_name}
                })
                tender_ids.add(t1_id)

            if t2_id not in tender_ids:
                t2_label = t2_name[:30] + "..." if t2_name and len(t2_name) > 30 else t2_name
                nodes.append({
                    'id': t2_id,
                    'label': t2_label,
                    'type': 'Tender',
                    'properties': {'name': t2_name}
                })
                tender_ids.add(t2_id)

            # Add shared requirement nodes (limit to top 3)
            for req in result.get('shared_requirements', [])[:3]:
                req_id = f"req_{req}"
                if not any(n['id'] == req_id for n in nodes):
                    nodes.append({
                        'id': req_id,
                        'label': req,
                        'type': 'Requirement',
                        'properties': {'name': req}
                    })

                # Connect tenders to requirement
                edges.append({'source': t1_id, 'target': req_id, 'type': 'HAS_REQUIREMENT'})
                edges.append({'source': t2_id, 'target': req_id, 'type': 'HAS_REQUIREMENT'})

        return create_network_graph(nodes, edges, "Requirements Overlap")
    except Exception as e:
        return f"<p>Error: {str(e)}</p>"


def query_graph_explorer(entity_type: str, entity_name: str, depth: int) -> str:
    """Generate graph visualization for entity neighborhood exploration."""
    import logging
    logging.basicConfig(filename='/tmp/gradio_debug.log', level=logging.DEBUG,
                       format='%(asctime)s - %(message)s', force=True)

    try:
        logging.info(f"[DEBUG] Graph Explorer called with: type={entity_type}, name={entity_name}, depth={depth}")
        print(f"[DEBUG] Graph Explorer called with: type={entity_type}, name={entity_name}, depth={depth}", flush=True)

        if not entity_name:
            return "<p>Please enter an entity name</p>"

        driver = get_driver()
        if driver is None:
            return "<p>Error: Neo4j connection not available</p>"

        # Query for entity neighborhood
        query = f"""
        MATCH (center:__Entity__:`{entity_type}` {{name: $entity_name}})
        OPTIONAL MATCH path = (center)-[*1..{depth}]-(neighbor)
        WITH center, collect(DISTINCT neighbor) AS neighbors, collect(DISTINCT path) AS paths
        RETURN center, neighbors, paths
        """

        print(f"[DEBUG] Executing query...")

        with driver.session() as session:
            result = session.run(query, entity_name=entity_name)
            record = result.single()

            if not record:
                driver.close()
                print(f"[DEBUG] Entity not found")
                return f"<p>Entity not found: {entity_type} '{entity_name}'</p>"

            center = record['center']
            neighbors = record['neighbors']
            paths = record['paths']

            print(f"[DEBUG] Found center node, {len(neighbors)} neighbors, {len(paths)} paths")

        driver.close()

        # Build graph
        nodes = []
        edges = []
        node_ids = set()

        # Add center node
        center_name = center.get('name', '')
        center_id = f"{entity_type}_{center_name}"
        nodes.append({
            'id': center_id,
            'label': center_name or 'Unknown',
            'type': entity_type,
            'properties': dict(center)
        })
        node_ids.add(center_id)

        # Add neighbor nodes and edges from paths
        for path in paths:
            if path is None:
                continue

            for node in path.nodes:
                # Get node labels
                labels = list(node.labels)
                node_type = next((l for l in labels if l != '__Entity__'), 'Unknown')
                node_name = node.get('name', '')
                node_id = f"{node_type}_{node_name}"

                if node_id not in node_ids:
                    node_label = node_name[:30] + "..." if node_name and len(node_name) > 30 else (node_name or 'Unknown')
                    nodes.append({
                        'id': node_id,
                        'label': node_label,
                        'type': node_type,
                        'properties': dict(node)
                    })
                    node_ids.add(node_id)

            for rel in path.relationships:
                start_node = rel.start_node
                end_node = rel.end_node
                start_labels = list(start_node.labels)
                end_labels = list(end_node.labels)
                start_type = next((l for l in start_labels if l != '__Entity__'), 'Unknown')
                end_type = next((l for l in end_labels if l != '__Entity__'), 'Unknown')

                source_id = f"{start_type}_{start_node.get('name', '')}"
                target_id = f"{end_type}_{end_node.get('name', '')}"

                edges.append({
                    'source': source_id,
                    'target': target_id,
                    'type': rel.type
                })

        print(f"[DEBUG] Built graph with {len(nodes)} nodes and {len(edges)} edges")

        if len(nodes) == 1:
            return f"<p>No connections found for {entity_type} '{entity_name}'</p>"

        html = create_network_graph(nodes, edges, f"Neighborhood: {entity_type} '{entity_name}'")
        logging.info(f"[DEBUG] Generated HTML, length: {len(html)} bytes")
        print(f"[DEBUG] Generated HTML, length: {len(html)} bytes", flush=True)

        # Save to temp file for debugging
        with open('/tmp/last_graph.html', 'w') as f:
            f.write(html)

        logging.info("[DEBUG] Saved graph to /tmp/last_graph.html")

        # Extract the body content from pyvis HTML and wrap in a proper container
        import re
        # Find the body content
        body_match = re.search(r'<body>(.*?)</body>', html, re.DOTALL)
        if body_match:
            body_content = body_match.group(1)
        else:
            body_content = html

        # Find the script content
        script_match = re.search(r'<script type="text/javascript">(.*?)</script>', html, re.DOTALL)
        script_content = script_match.group(1) if script_match else ""

        # Create a self-contained div with inline scripts
        wrapper_html = f'''
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/vis-network/9.1.2/dist/dist/vis-network.min.css" integrity="sha512-WgxfT5LWjfszlPHXRmBWHkV2eceiWTOBvrKCNbdgDYTHrT2AeLCGbF4sZlZw3UMN3WtL0tGUoIAKsu8mllg/XA==" crossorigin="anonymous" referrerpolicy="no-referrer" />
        <script src="https://cdnjs.cloudflare.com/ajax/libs/vis-network/9.1.2/dist/vis-network.min.js" integrity="sha512-LnvoEWDFrqGHlHmDD2101OrLcbsfkrzoSpvtSQtxK3RMnRV0eOkhhBN2dXHKRrUU8p2DGRTk35n4O8nWSVe1mQ==" crossorigin="anonymous" referrerpolicy="no-referrer"></script>
        {body_content}
        <script type="text/javascript">
        {script_content}
        </script>
        '''

        return wrapper_html
    except Exception as e:
        print(f"[ERROR] Exception in query_graph_explorer: {str(e)}")
        import traceback
        traceback.print_exc()
        return f"<p>Error: {str(e)}</p>"


def create_gradio_app() -> Any:
    """Create and configure the Gradio interface."""
    if gr is None:
        raise ImportError("gradio is not installed. Please run: pip install gradio")

    # Check connection status
    is_connected, status_msg = check_connection()

    with gr.Blocks(title="GeBIZ Tender Intelligence") as app:
        gr.Markdown("# GeBIZ Tender Knowledge Graph")
        gr.Markdown("Explore tender awards, agencies, suppliers, and relationships through interactive queries and visualizations.")

        # Connection status
        gr.Markdown(f"**Connection Status:** {status_msg}")

        if not is_connected:
            gr.Markdown("⚠️ Please configure Neo4j connection to use this interface.")
            return app

        # Tabs
        with gr.Tabs():
            # Tab 1: Agency Explorer
            with gr.Tab("Agency Explorer"):
                gr.Markdown("### Search tenders by agency or view top agencies")
                with gr.Row():
                    agency_input = gr.Textbox(label="Agency Name", placeholder="Enter agency name...")
                    agency_show_top = gr.Checkbox(label="Show Top Agencies", value=False)
                    agency_limit = gr.Slider(minimum=5, maximum=50, value=10, step=5, label="Limit")

                # Example agency name
                gr.Examples(
                    examples=[["Accounting And Corporate Regulatory Authority", False, 10]],
                    inputs=[agency_input, agency_show_top, agency_limit],
                    label="Example"
                )

                agency_btn = gr.Button("Search")
                agency_output = gr.Textbox(label="Results", lines=20)
                agency_btn.click(
                    query_agencies_tab,
                    inputs=[agency_input, agency_show_top, agency_limit],
                    outputs=agency_output
                )

            # Tab 2: Supplier Explorer
            with gr.Tab("Supplier Explorer"):
                gr.Markdown("### Search tenders by supplier or view top suppliers")
                with gr.Row():
                    supplier_input = gr.Textbox(label="Supplier Name", placeholder="Enter supplier name...")
                    supplier_show_top = gr.Checkbox(label="Show Top Suppliers", value=False)
                    supplier_limit = gr.Slider(minimum=5, maximum=50, value=10, step=5, label="Limit")

                # Example supplier name
                gr.Examples(
                    examples=[["KPMG SERVICES PTE. LTD.", False, 10]],
                    inputs=[supplier_input, supplier_show_top, supplier_limit],
                    label="Example"
                )

                supplier_btn = gr.Button("Search")
                supplier_output = gr.Textbox(label="Results", lines=20)
                supplier_btn.click(
                    query_suppliers_tab,
                    inputs=[supplier_input, supplier_show_top, supplier_limit],
                    outputs=supplier_output
                )

            # Tab 3: Category Explorer
            with gr.Tab("Category Explorer"):
                gr.Markdown("### Browse categories, keywords, and requirements")
                with gr.Row():
                    category_input = gr.Textbox(label="Category Group", placeholder="Enter category group...")
                    category_show_all = gr.Checkbox(label="Show All Categories", value=False)

                # Example category group
                gr.Examples(
                    examples=[["IT Services & Software", False]],
                    inputs=[category_input, category_show_all],
                    label="Example"
                )

                category_btn = gr.Button("Search")
                category_output = gr.Textbox(label="Results", lines=20)
                category_btn.click(
                    query_categories_tab,
                    inputs=[category_input, category_show_all],
                    outputs=category_output
                )

            # Tab 4: Similar Tenders
            with gr.Tab("Similar Tenders"):
                gr.Markdown("### Find tenders similar to a reference tender (interactive graph)")
                with gr.Row():
                    similar_tender_input = gr.Textbox(label="Tender Name", placeholder="Enter tender name...")
                    similar_limit = gr.Slider(minimum=3, maximum=20, value=5, step=1, label="Max Results")
                    similar_category = gr.Checkbox(label="Include Category Boost", value=False)
                similar_btn = gr.Button("Find Similar")
                similar_output = gr.HTML(label="Graph Visualization")
                similar_btn.click(
                    query_similar_tenders_graph,
                    inputs=[similar_tender_input, similar_limit, similar_category],
                    outputs=similar_output
                )

            # Tab 5: Graph Explorer
            with gr.Tab("Graph Explorer"):
                gr.Markdown("### Explore neighborhoods around any entity")
                with gr.Row():
                    explorer_type = gr.Dropdown(
                        choices=["Tender", "Agency", "Supplier", "Category", "Requirement", "Keyword"],
                        label="Entity Type",
                        value="Tender"
                    )
                    explorer_name = gr.Textbox(label="Entity Name", placeholder="Enter entity name...")
                    explorer_depth = gr.Slider(minimum=1, maximum=3, value=1, step=1, label="Depth")

                # Example entity names
                gr.Examples(
                    examples=[["Tender", "ACR000ETT21000001", 1]],
                    inputs=[explorer_type, explorer_name, explorer_depth],
                    label="Example"
                )

                explorer_btn = gr.Button("Explore")

                with gr.Row():
                    with gr.Column(scale=4):
                        explorer_output = gr.HTML(label="Graph Visualization")
                    with gr.Column(scale=1):
                        explorer_file = gr.File(label="Download Graph HTML", visible=False)

                def explorer_wrapper(entity_type: str, entity_name: str, depth: int):
                    html = query_graph_explorer(entity_type, entity_name, depth)

                    # Save full HTML to downloadable file
                    with open('/tmp/last_graph.html', 'r') as f:
                        full_html = f.read()

                    temp_file = '/tmp/graph_download.html'
                    with open(temp_file, 'w') as f:
                        f.write(full_html)

                    # Create iframe pointing to the file
                    # Use Gradio's file serving by creating a data URL with the full HTML
                    import base64
                    html_b64 = base64.b64encode(full_html.encode()).decode()
                    iframe_html = f'''
                    <iframe
                        src="data:text/html;base64,{html_b64}"
                        width="100%"
                        height="650px"
                        frameborder="0"
                        style="border: 1px solid #ccc; border-radius: 4px;">
                    </iframe>
                    '''

                    return iframe_html, gr.File(value=temp_file, visible=True)

                explorer_btn.click(
                    explorer_wrapper,
                    inputs=[explorer_type, explorer_name, explorer_depth],
                    outputs=[explorer_output, explorer_file]
                )

    return app


def main() -> None:
    """Launch the Gradio app."""
    app = create_gradio_app()
    app.launch(server_name="0.0.0.0", server_port=7860, share=False)


if __name__ == "__main__":
    main()
