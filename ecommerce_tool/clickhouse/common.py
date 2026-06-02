from rest_framework.response import Response


def send_error_response(e: str = "failed", status_code: int = 500) -> Response:
    return {"status": "failed", "error": str(e), "is_error": True}


def send_response(data: dict = None, status_code: int = 200) -> Response:
    if data is None:
        data = {}

    return {"status": "success", "data": data}
