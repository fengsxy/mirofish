"""
Auto-Research API endpoints for Bibibo research module.
Provides endpoints for starting research, checking status, retrieving results,
and confirming/building text from selected evidence.
"""

from flask import request, jsonify

from . import research_bp
from ..services.research_service import ResearchService
from ..utils.logger import get_logger

logger = get_logger('mirofish.api.research')

# Module-level singleton for ResearchService
_research_service = None


def _get_service() -> ResearchService:
    """Get or create singleton ResearchService instance."""
    global _research_service
    if _research_service is None:
        _research_service = ResearchService()
    return _research_service


@research_bp.route('/start', methods=['POST'])
def start_research():
    """
    POST /api/research/start

    Start a new research task.

    Request body:
    {
        "question": "Your research question...",
        "project_id": "project-123"
    }

    Response on success:
    {
        "success": true,
        "data": {
            "task_id": "task-abc-123"
        }
    }
    """
    try:
        data = request.get_json()

        if not data:
            return jsonify({
                "success": False,
                "error": "Request body must be JSON"
            }), 400

        question = data.get('question', '').strip()
        project_id = data.get('project_id', '').strip()

        if not question:
            return jsonify({
                "success": False,
                "error": "question field cannot be empty"
            }), 400

        if not project_id:
            return jsonify({
                "success": False,
                "error": "project_id field cannot be empty"
            }), 400

        service = _get_service()
        task_id = service.start_research(question, project_id)

        logger.info(f"Started research task {task_id} for project {project_id}")

        return jsonify({
            "success": True,
            "data": {
                "task_id": task_id
            }
        }), 202

    except Exception as e:
        logger.error(f"Error in start_research: {e}", exc_info=True)
        return jsonify({
            "success": False,
            "error": "Internal server error"
        }), 500


@research_bp.route('/status/<task_id>', methods=['GET'])
def get_status(task_id: str):
    """
    GET /api/research/status/<task_id>

    Get the current status of a research task.

    Response on success:
    {
        "success": true,
        "data": {
            "status": "processing|completed|failed",
            "progress": 45,
            "message": "Searching (2/5): query text...",
            "phase": "Searching (2/5): query text..."
        }
    }

    Response on not found:
    {
        "success": false,
        "error": "Task not found"
    } (404)
    """
    try:
        service = _get_service()
        status = service.get_status(task_id)

        if status is None:
            return jsonify({
                "success": False,
                "error": "Task not found"
            }), 404

        return jsonify({
            "success": True,
            "data": status
        }), 200

    except Exception as e:
        logger.error(f"Error in get_status: {e}", exc_info=True)
        return jsonify({
            "success": False,
            "error": "Internal server error"
        }), 500


@research_bp.route('/results/<task_id>', methods=['GET'])
def get_results(task_id: str):
    """
    GET /api/research/results/<task_id>

    Get the research results (evidence) from a completed task.

    Response on success:
    {
        "success": true,
        "data": {
            "evidence": [
                {
                    "id": 0,
                    "title": "...",
                    "url": "...",
                    "snippet": "...",
                    "source": "duckduckgo|you.com",
                    "query": "..."
                },
                ...
            ],
            "total_count": 15,
            "queries_used": ["query 1", "query 2", ...]
        }
    }

    Response on not found or not completed:
    {
        "success": false,
        "error": "Results not available"
    } (404)
    """
    try:
        service = _get_service()
        results = service.get_results(task_id)

        if results is None:
            return jsonify({
                "success": False,
                "error": "Results not available"
            }), 404

        return jsonify({
            "success": True,
            "data": results
        }), 200

    except Exception as e:
        logger.error(f"Error in get_results: {e}", exc_info=True)
        return jsonify({
            "success": False,
            "error": "Internal server error"
        }), 500


@research_bp.route('/confirm/<task_id>', methods=['POST'])
def confirm_and_build_text(task_id: str):
    """
    POST /api/research/confirm/<task_id>

    Confirm selected evidence items and build final text.

    Request body:
    {
        "selected_ids": [0, 2, 5],
        "extra_text": "Additional information to append..."
    }

    Response on success:
    {
        "success": true,
        "data": {
            "text": "...",
            "metadata": {
                "source_count": 3,
                "has_extra_text": true,
                "sources": [
                    {"title": "...", "url": "..."},
                    ...
                ]
            }
        }
    }

    Response on not found:
    {
        "success": false,
        "error": "Task not found"
    } (404)
    """
    try:
        data = request.get_json()

        if not data:
            return jsonify({
                "success": False,
                "error": "Request body must be JSON"
            }), 400

        selected_ids = data.get('selected_ids', [])
        extra_text = data.get('extra_text', '')

        # Validate selected_ids
        if not isinstance(selected_ids, list):
            return jsonify({
                "success": False,
                "error": "selected_ids must be a list"
            }), 400

        if not selected_ids:
            return jsonify({
                "success": False,
                "error": "selected_ids cannot be empty"
            }), 400

        service = _get_service()
        result = service.confirm_and_build_text(task_id, selected_ids, extra_text)

        if result is None:
            return jsonify({
                "success": False,
                "error": "Task not found or evidence not available"
            }), 404

        logger.info(f"Built text from task {task_id} with {len(selected_ids)} sources")

        return jsonify({
            "success": True,
            "data": result
        }), 200

    except Exception as e:
        logger.error(f"Error in confirm_and_build_text: {e}", exc_info=True)
        return jsonify({
            "success": False,
            "error": "Internal server error"
        }), 500
